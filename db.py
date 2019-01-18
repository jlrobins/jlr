import psycopg2, psycopg2.extras, psycopg2.extensions
import time, threading
from functools import wraps

from flask import g

# Default exports
__all__ = ('configure_flask', 'configure_flask_socketio')

###
# Flask + psycopg helpers
###


def configure_flask(flask_app, params, idle_timeout_secs=30):
	###
	# Configure db access for a regular (non-socketio) flask app.
	# Set up DB-oriented before_first_request(), before_request(), and
	# teardown_request() lifecycle hooks in order to deal with
	# connection acquisition and placement as 'g.con', request boundary
	# is a db transaction boundary, and ensuring a rollback if the
	# flask request exceptions out (in production anyway, see notes
	# about bad interaction with flask debugger up in
	# ManagedConnection.begin_trasaction()).
	###

	global mc

	configure(params, timeout_secs=idle_timeout_secs)

	flask_app.before_first_request(mc.start_closing_thread)
	flask_app.before_first_request(register_composite_types)

	def begin_and_assign_tx():
		g.con = mc.begin_transaction()

	flask_app.before_request(begin_and_assign_tx)

	def finish_transaction(response):
		g.pop('con', None)
		mc.complete_transaction()

		return response

	flask_app.teardown_request(finish_transaction)

	@flask_app.errorhandler(500)
	def error_500(error):
		mc.set_rollback_only()
		raise error


def configure_flask_socketio(params, register_types=True,
											idle_timeout_secs=30):
	global mc

	configure(params, timeout_secs=idle_timeout_secs)

	# flask-socketio does not fire before_first_request(),
	# before_request(), or teardown_request(), so less can be
	# done here in lieu of more decorators around the socketio
	# event handlers.

	mc.start_closing_thread()

	if register_types:
		register_composite_types()

	return mc


####
# Internals from here on out
####

class ManagedConnection():
	# Singleton class managing db connection, transaction state,
	# and auto-closing the db connection after 30sec inactivity.
	def __init__(self, params, cursor_factory, timeout_secs=30):
		self.params = params
		self.cursor_factory = cursor_factory
		self.timeout_secs = timeout_secs
		self.con = None
		self.busy = False
		self.last_used = None
		self.commit_after_complete = True


	def start_closing_thread(self):
		def close_when_idle():
			while True:
				time.sleep(self.timeout_secs)
				if not self.busy \
						and self.con \
						and time.time() > self.last_used + self.timeout_secs:

					self.close()

		threading.Thread(target=close_when_idle).start()

	def close(self):
		if self.con:
			self.con.close()
			self.con = None

		self.busy = False


	def begin_transaction(self):
		if not self.con:
			self.con = self.__connection()

		if self.busy:
			# Wacky! Holdover from bad interaction with flask debugger
			# in devel mode and hitting a caught exception (in debugger)
			# on the prior request. Grr. The flask debugger is deeper in
			# flask wsgi server than our "with_transaction()" decorator,
			# so it doesn't have a chance to rollback itself.
			self.con.rollback()

		self.busy = True
		self.last_used = time.time()

		return self.con


	def set_rollback_only(self):
		# Indicate that the only way this TX should end is
		# via rollback, not commit. Observed by complete_transaction()
		self.commit_after_complete = False

	def complete_transaction(self):
		if self.con:
			if self.commit_after_complete:
				self.con.commit()
			else:
				self.con.rollback()
				self.commit_after_complete = True # clear it for next request.

		self.busy = False



	def with_transaction(self, func,):
		"""
			Decorator to use around web dispatched functions,
			useful for socketio event handlers, since flask's
			more natural way (before_request hooks) don't fire
			within a flask_socketio app. Grr.
		"""

		@wraps(func)
		def doit(*args):
			con = self.begin_transaction()
			try:
				func(con, *args)
			except Exception as e:
				self.set_rollback_only()
				raise e
			finally:
				self.complete_transaction()

		return doit

	def __connection(self):
		return psycopg2.connect(self.params, cursor_factory=self.cursor_factory)

# The singleton instance.
mc = None


def configure(params, timeout_secs=30,
							cursor_factory=psycopg2.extras.NamedTupleCursor):
	global mc

	mc = ManagedConnection(params, timeout_secs=timeout_secs,
									cursor_factory=cursor_factory)


def flask_connection():
	global mc

	if '_con' not in g:
		g._con = mc.begin_transaction()

	return g._con

def register_composite_types():
	###
	# Teach psycopg about any custom type oids hinted at in custom
	# recipe table 'psycopg_types', if needed in this project.
	#
	# Needed for both 'array of domain' and custom composite type
	# support, queries containing either of which will be returning
	# novel / surprising oids. So here we hint psycopg2 how to deal
	# with them.
	###
	con = mc.begin_transaction()

	typeconverters = [ o for o in psycopg2.__dict__.values() if
									type(o) is type(psycopg2.STRING) ]
	oid_to_typeconverter = {}
	for tc in typeconverters:
		for oid in tc.values:
			oid_to_typeconverter[oid] = tc

	cur = con.cursor()
	cur.execute('''
			select
				pt.type_to_register,
				pgt.typcategory,
				pgt.typarray,
				pgt.typbasetype,
				pgt_base.typname
			from psycopg_types pt
				join pg_catalog.pg_type pgt
					left join pg_catalog.pg_type pgt_base
						on (pgt.typbasetype = pgt_base.oid and pgt.typcategory='S')
			on pt.type_to_register = pgt.oid;
		''')

	for c in cur.fetchall():
		if c.typcategory == 'C':
			# Composite type
			psycopg2.extras.register_composite(c.type_to_register,
														con, globally=True)
		else:
			assert c.typcategory == 'S', \
							'Do not understand type category %s' % c.typcategory

			base_type_adaptor = oid_to_typeconverter.get(c.typbasetype)

			if not base_type_adaptor:
				raise Exception(
					'Unknown type adaptor for %s for domain array type %s' \
						% (c.typname, c.type_to_register))

			psycopg2.extensions.register_type(
				psycopg2.extensions.new_array_type(
					(c.typarray,), c.type_to_register + '[]', base_type_adaptor))

	cur.close()

	mc.complete_transaction()


