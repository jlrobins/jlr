from jlr.query_builder import QueryBuilder, AND, OR



def test_no_where_clause():
	qb = QueryBuilder()

	qb.relation('document') \
		.project('storage_type', 'count(*)') \
		.group_by(1)

	assert qb.statement == 'SELECT storage_type, count(*) FROM document GROUP BY 1', qb.statement




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
		.where(AND(
				("storage_type = %s", 'email'),
				OR(('date_entered > current_date'),
							('document_id > %s', 4564))))

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
					on='d.document_id == em.document_id') \
		.project('count(*)')

	assert qb.statement == \
			'SELECT count(*) FROM document d' \
			' RIGHT JOIN email_documents.email em' \
			' ON (d.document_id == em.document_id)', qb.statement


def test_full_outer_join_on():
	qb = QueryBuilder()

	qb.relation('document d') \
		.outer_join('email_documents.email em',
					on='d.document_id == em.document_id') \
		.project('count(*)')

	assert qb.statement == \
			'SELECT count(*) FROM document d' \
			' FULL OUTER JOIN email_documents.email em' \
			' ON (d.document_id == em.document_id)', qb.statement

def test_multiple_joins():
	qb = QueryBuilder()

	qb.relation('document d') \
		.join('email_documents.email em',
					on='d.document_id == em.document_id') \
		.left_join('document_comment dc',
					using='legal_case_id, document_id')
		.project('count(*)')

	assert qb.statement == \
			'SELECT count(*) FROM document d' \
			' FULL OUTER JOIN email_documents.email em' \
			' ON (d.document_id == em.document_id)', qb.statement
