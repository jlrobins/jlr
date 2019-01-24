from jlr.query_builder import QueryBuilder, AND, OR, AliasException



def test_no_where_clause():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('storage_type', 'count(*)') \
		.group_by(1)

	assert qb.statement == 'SELECT storage_type, count(*) FROM document GROUP BY 1', qb.statement


def test_having():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('count(*)') \
		.having('count(*) > %s', 123) \
		.where('document_id > %s', 999) \
		.project('storage_type') \
		.group_by('storage_type')

	# Should be observed in the proper order, not funcall order.
	assert qb.parameters == (999, 123)
	assert qb.statement == 'SELECT count(*), storage_type FROM document' \
			' WHERE document_id > %s GROUP BY storage_type' \
			' HAVING count(*) > %s'

def test_out_of_order_params():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('document_id') \
		.having('count(*) > %s', 999) \
		.where('a between %s and %s', 555, 666) \
		.join('foo f', on='f.id = d.id and f.id > %s and d.id < %s', params=(99, 22)) \
		.join('bar b', on='b.id = d.id and b.id in %s', params=(('a', 'b'),))

	# joins, then wheres, then havings. And don't mess up the
	# interior tuple for the 'in' test.
	expected_params = (99, 22, ('a', 'b'), 555, 666, 999)
	assert qb.parameters == expected_params, qb.parameters



def test_simple_where_clause():
	## test 2: a single where clause, no group by
	qb = QueryBuilder()

	qb.relation('document') \
		.project('count(*)') \
		.where("storage_type = %s", 'email')


	assert qb.statement == 'SELECT count(*) FROM document WHERE storage_type = %s', qb.statement

def test_compound_where_simple_or():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('count(*)') \
		.where(OR(("storage_type = %s", 'email'), ('date_entered < current_date')))

	assert qb.statement == 'SELECT count(*) FROM document WHERE (storage_type = %s) OR (date_entered < current_date)', qb.statement

def test_compound_where_simple_and():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('count(*)') \
		.where(AND(("storage_type = %s", 'email'), ('date_entered < current_date')))

	assert qb.statement == 'SELECT count(*) FROM document WHERE (storage_type = %s) AND (date_entered < current_date)', qb.statement

def test_compound_where_nested_expressions():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('count(*)') \
		.where("storage_type = %s", 'email') \
		.where(OR(('date_entered > current_date'),
							('document_id > %s', 4564)))

	assert qb.statement == 'SELECT count(*) FROM document WHERE (storage_type = %s) AND ((date_entered > current_date) OR (document_id > %s))', qb.statement
	assert qb.parameters == ('email', 4564), qb.parameters


def test_join_using():
	qb = QueryBuilder()

	qb.relation('document d') \
		.join('email_documents.email em', using='document_id') \
		.project('count(*)')

	assert qb.statement == 'SELECT count(*) FROM document d' \
							' INNER JOIN email_documents.email em' \
							' USING (document_id)', qb.statement

def test_join_on():
	qb = QueryBuilder()

	qb.relation('document d') \
		.join('email_documents.email em',
					on='d.document_id == em.document_id') \
		.project('count(*)')

	assert qb.statement == \
			'SELECT count(*) FROM document d' \
			' INNER JOIN email_documents.email em' \
			' ON (d.document_id == em.document_id)', qb.statement

def test_right_join_on():
	qb = QueryBuilder()

	qb.relation('document d') \
		.right_join('email_documents.email em',
					on='d.document_id = em.document_id') \
		.project('count(*)')

	assert qb.statement == \
			'SELECT count(*) FROM document d' \
			' RIGHT JOIN email_documents.email em' \
			' ON (d.document_id = em.document_id)', qb.statement


def test_full_outer_join_on():
	qb = QueryBuilder()

	qb.relation('document d') \
		.outer_join('email_documents.email em',
					on='d.document_id = em.document_id') \
		.project('count(*)')

	assert qb.statement == \
			'SELECT count(*) FROM document d' \
			' FULL OUTER JOIN email_documents.email em' \
			' ON (d.document_id = em.document_id)', qb.statement

def test_additional_joins():
	qb = QueryBuilder()

	qb.relation('document d') \
		.join('email_documents.email em',
					on='d.document_id = em.document_id') \
		.left_join('document_comment dc',
					using='legal_case_id, document_id') \
		.project('count(*)')

	assert qb.statement == \
			'SELECT count(*) FROM document d' \
			' INNER JOIN email_documents.email em' \
			' ON (d.document_id = em.document_id)' \
			' LEFT JOIN document_comment dc' \
			' USING (legal_case_id, document_id)', qb.statement

def test_multiple_join_noop():
	# Adding same exact join clause should be a no-op.
	qb = QueryBuilder()

	qb.relation('document d') \
		.join('email_documents.email em',
					on='d.document_id == em.document_id') \
		.join('email_documents.email em',
					on='d.document_id == em.document_id') \
		.project('count(*)')

	assert qb.statement == \
			'SELECT count(*) FROM document d' \
			' INNER JOIN email_documents.email em' \
			' ON (d.document_id == em.document_id)', qb.statement

def test_multiple_join_different_aliases_honored():
	# Adding same exact join clause should be a no-op.
	qb = QueryBuilder()

	qb.relation('document d') \
		.join('email_documents.email em',
					on='d.document_id == em.document_id') \
		.join('email_documents.email em2',
					on='d.document_id == em2.document_id and em2.document_id > 12') \
		.project('count(*)')

	assert qb.statement == \
			'SELECT count(*) FROM document d' \
			' INNER JOIN email_documents.email em' \
			' ON (d.document_id == em.document_id)' \
			' INNER JOIN email_documents.email em2' \
			' ON (d.document_id == em2.document_id' \
			' and em2.document_id > 12)', qb.statement

def test_complain_at_reused_alias():
	qb = QueryBuilder().relation('foo f')

	try:
		qb.join('foonly f', using='id')
		raised = False
	except AliasException as e:
		raised = True

	assert raised, 'Should have balked at using alias "f" for two different relations'

def test_limit_no_offset():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('document_id') \
		.where('document_id < %s', 200) \
		.limit(20)

	assert qb.statement == \
		'SELECT document_id FROM document WHERE document_id < %s LIMIT %s', \
		'Statement was: %s'  % qb.statement

	assert qb.parameters == (200, 20)

def test_limit_limit_plus_offset():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('document_id') \
		.where('document_id < %s', 200) \
		.limit(20, offset=10)

	assert qb.statement == \
		'SELECT document_id FROM document WHERE document_id < %s LIMIT %s OFFSET %s', \
		'Statement was: %s'  % qb.statement

	assert qb.parameters == (200, 20, 10)

def test_limit_separate_offset():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('document_id') \
		.where('document_id < %s', 200) \
		.limit(20) \
		.offset(10)

	assert qb.statement == \
		'SELECT document_id FROM document WHERE document_id < %s LIMIT %s OFFSET %s', \
		'Statement was: %s'  % qb.statement

	assert qb.parameters == (200, 20, 10)

def test_offset_requires_limit():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('document_id') \
		.where('document_id < %s', 200) \

	try:
		qb.offset(10)
		print(qb.statement)
		raised = False
	except AssertionError as e:
		raised = True

def test_second_call_to_relation_overwrites():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('document_id') \
		.where('document_id < %s', 200) \

	qb.relation('document_view')

	assert qb.statement == \
		'SELECT document_id FROM document_view WHERE document_id < %s', \
		'Statement was: %s'  % qb.statement


