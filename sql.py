import psycopg2, psycopg2.extras

###
# James' convienence layer atop psycopg2.
#
# Take care of all cursor management, and instead return
# lists of whatever our cursor factory produces (or perhaps a single
# row, or a single column, or a single value ...)
def connection(conn_string):
	con = psycopg2.connect(conn_string,
						cursor_factory=psycopg2.extras.NamedTupleCursor)

	con.autocommit=False
	con.isolation_level = 'SERIALIZABLE' # Hey, a real ACID DB!

	return con

def query_single_column(con, stmt, params=None):
	###
	# Return list of the 1st column returned by query
	###
	cur = con.cursor()
	cur.execute(stmt, params)

	colvalues = [ r[0] for r in cur.fetchall() ]

	cur.close()

	return colvalues


def query_single_value(con, stmt, params=None):
	###
	# Return first row's first column, otherwise None.
	# Asserts no more than one row returned.
	###

	cur = con.cursor()
	cur.execute(stmt, params)

	assert cur.rowcount < 2
	if cur.rowcount == 1: # allow either 0 or 1 rows.
		r = cur.fetchone()[0]
	else:
		r = None

	cur.close()

	return r

def query_single_row(con, stmt, params=None):
	####
	# Return all of a single row.
	# Asserts no more than one row returned.
	###

	cur = con.cursor()
	cur.execute(stmt, params)

	assert cur.rowcount < 2 # allow either 0 or 1 rows.
	r = cur.fetchone()

	cur.close()

	return r

def query(con, stmt, params=None):
	###
	# Return all rows / columns for a query
	###

	cur = con.cursor()
	cur.execute(stmt, params)

	rows = cur.fetchall()

	cur.close()

	return rows

def query_json_strings(con, stmt, params=None):
	####
	# Wraps a query's results whose rows are being projected as JSON
	# strings (like via "select (t.*)::json from t")
	# in an overall string describing the rows as a JSON array.
	#
	# So, in the above, if table t was (id, name) and had 3
	# rows, then we'd produce a single string separating each
	# row's json spelling with a comma / newline pair:
	# 	'''[{id: 1, name: "mary"},
	#       {id: 2, name: "jane"},
	#       {id: 3, name: "convenience"}]'''
	#
	#
	# If no rows returned from query, then we return an empty json array.
	#

	results = query_single_column(con, stmt, params=params)
	if results:
		assert type(results[0]) is str

		# assemble into a big string smelling like a json array.
		buf = ['[']
		buf.extend(',\n '.join(results))
		buf.append(']')

		return ''.join(buf)
	else:
		return '[]' # smell like empty json array.

query_single = query_single_row

def execute(con, stmt, params=None):
	###
	# Run this statement, returning the rowcount instead of any results
	###
	cur = con.cursor()
	cur.execute(stmt, params)
	retval = cur.rowcount
	cur.close()
	return retval


def insert(con, tableName:str, rowDict:dict, excludeKeys=None,
                   return_columns=None):

	###
	# Build and execute an insert statement given a dict describing column/values.
	# Can be hinted to exclude certain keys in the dict, and can be asked
	# to return a list of the resulting (probably generated server-side)
	# values
	###

	cursor = con.cursor()
	nameList = sorted(rowDict.keys())
	colClause = []
	valueClause = []

	ccw = colClause.append
	vcw = valueClause.append

	if excludeKeys==None:
		excludeKeys=()

	for colName in nameList:
		if colName not in excludeKeys:
			val = rowDict[colName]
			# Don't need to add insert null records
			if val is not None:
				ccw(colName)

				if not isinstance(val, LiteralValue):
					vcw('%%(%s)s' % colName)
				else:
					# Want to embed literal expression, like 'now()' or so forth.
					vcw(val)


	colClause = ', '.join(colClause)
	valueClause = ', '.join(valueClause)

	if colClause:
		statement = 'insert into %s (%s) values (%s)' % \
							(tableName, colClause, valueClause)
	else:
		statement = 'insert into %s default values' % (tableName,)

	if return_columns:
		if not isinstance(return_columns, str):
			return_columns = ", ". join(return_columns)
		statement += ' returning ' + return_columns

	# Doit!
	try:
		cursor.execute(statement, rowDict)
		if return_columns:
			return cursor.fetchone()
		return cursor.rowcount
	except psycopg2.ProgrammingError as e:
		e.statement = cursor.statement
		raise

def update(con, table_name:str, where_columns_and_values:list,
			update_columns_and_values:list):

	# Should be of form [ ('foo=%s', 12), ('bar < %s', 55) ]
	# to build up where clause
	assert(all(len(p) == 2 and type(p[0]) is str
			and '%s' in p[0] for p in where_columns_and_values))

	# Should be of form [('blat', 45), ('sdf', 99)]
	# for columns to update + value to update to
	assert(all(len(p) == 2 and type(p[0]) is str
			and '%s' not in p[0] for p in update_columns_and_values))

	# "foo=%s, bar=%s" ...
	update_column_part = ', '.join('%s = %%s' % colname
								for colname, _ in update_columns_and_values)
	# (12, 'barvalue') ...
	values = [v for _, v in update_columns_and_values]

	where_column_part = ', '.join(colexpr for colexpr, _ in
										where_columns_and_values)

	values.extend(v for _, v in where_columns_and_values)

	# Psycopy desires a tuple wrapping values
	values_tuple = tuple(values)

	statement = 'update %s set %s where %s' % \
				(table_name, update_column_part, where_column_part)

	return execute(con, statement, values)


def get_pct_s_string(values):
	pcts = ['%s'] * len(values)
	return ','.join(pcts)

def bulk_insert(con, tableName:str, rowDictList:list,
			colList=None, excludeKeys=None,
			addToEveryRow=None, return_column=None):
	###
	#	Bulk-insert the rows in rowDictList using single-round trip
	#	  "insert into ... values (), (), ... ()"
	#
	#	One round-trip instead of embedding insert_builder inside of loops
	#	  for inserting rows into the same table.
	#
	#	Returns either the count of inserted rows [ default ],
	#	  or, if return_column specifies the name of a column to return [ 'id' ],
	#	  then will be a list of that column value, parallel to the rows in
	#	  rowDictList.
	###

	if not rowDictList:
		# Nothing to insert!
		return

	if colList is not None:
		colList = sorted(colList)
		# new list -- don't rearrange passed-in one under callers nose
	else:
		colList = sorted(rowDictList[0].keys())

	if excludeKeys is not None:
		colList = [k for k in colList if k not in excludeKeys]

	tableColList = colList

	if addToEveryRow:
		extraKeys = addToEveryRow.keys()
		every_row_values = addToEveryRow.values()
		extra_row_pct_s_s = get_pct_s_string(every_row_values)


		# Build new tableColList with these extra columns appended...
		# clone it -- don't append to colList, cause colList is used
		# to probe into regular row maps.
		tableColList = list(tableColList)
		for k in extraKeys:
			tableColList.append(k)

	row_pct_s = get_pct_s_string(colList)
	if addToEveryRow:
		row_pct_s += ',%s' % extra_row_pct_s_s

	# Wrap all these %s's in parens for the statement.
	row_pct_s = '(%s)' % row_pct_s


	if return_column:
		return_results = []

	rc = 0
	cursor = con.cursor()

	statement_buf = ['insert into %s (%s) values ' % \
									(tableName, ', '.join(tableColList)) ]
	statement_data = []
	value_rows_buf = []
	didOne = len(rowDictList) > 0

	for row in rowDictList:
		value_rows_buf.append(row_pct_s)
		statement_data.extend(row.get(k, None) for k in colList)

		if addToEveryRow:
			statement_data.extend(every_row_values)

	# Was at least one row, so do it.
	if didOne:
		values_clause = ',\n'.join(value_rows_buf)
		statement_buf.append(values_clause)

		if return_column:
			statement_buf.append('returning %s' % return_column)

		statement = '\n'.join(statement_buf)

		cursor.execute(statement, statement_data)
		rc = cursor.rowcount

		if return_column:
			return_results.extend(res[0] for res in cursor.fetchall())

		cursor.close()

	if return_column:
		return return_results
	else:
		# Just the rowcount
		return rc



class LiteralValue(str):
	###
	# Protect something like 'now()' from being quote-wrapped when passed
	# in a parameter list.
	###
	def getquoted(self):
		return self

	def __conform__(self, proto):
		if proto == psycopg2.extensions.ISQLQuote:
			return self
