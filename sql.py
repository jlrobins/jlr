import psycopg2
import psycopg2.extras

import itertools

import json
from collections import defaultdict


from jlr.query_builder import QueryBuilder, AND, OR


###
# James' convienence layer atop psycopg2.
#
# Take care of all cursor management, and instead return
# lists of whatever our cursor factory produces (or perhaps a single
# row, or a single column, or a single value ...)
###


def connection(conn_string):
    con = psycopg2.connect(conn_string,
                           cursor_factory=psycopg2.extras.NamedTupleCursor)

    con.autocommit = False
    con.isolation_level = 'SERIALIZABLE'  # Hey, a real ACID DB!

    return con


def query_single_column(con, stmt, params=None):
    ###
    # Return list of the 1st column returned by query
    ###
    cur = con.cursor()
    cur.execute(stmt, params)

    colvalues = [r[0] for r in cur.fetchall()]

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
    if cur.rowcount == 1:  # allow either 0 or 1 rows.
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

    assert cur.rowcount < 2  # allow either 0 or 1 rows.
    r = cur.fetchone()

    cur.close()

    return r

query_single = query_single_row  # Alias.

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
    #   '''[{id: 1, name: "mary"},
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

    return '[]'  # smell like empty json array.

def query_as_json(con, stmt, params=None):
    ###
    # Take a vanilla query returning regular rows
    # ('select a, b, c from foo where x=%s')
    # and wrap it in a CTE which projects each row
    # as JSON -> string. Then pass that into
    # query_json_strings to ultimately return
    # a single string containing a json array of rows
    # ( '[{"a": 12, "b": 44, "c":14},
    #     {"a": 44, "b": 23, "c":65}]' )

    buf = []
    buf.append('with data as (')
    buf.append(stmt)
    buf.append(') select to_json(d.*)::text from data d')

    stmt = '\n'.join(buf)
    return query_json_strings(con, stmt, params)

def query_single_column_as_json_array(con, stmt, params=None):
    ###
    # Similar to query_as_json, but return a string
    # describing a JSON array of scalar
    ###

    res = query_single_column(con, stmt, params)
    # Just punt out to json.dumps for the rest.
    return json.dumps(res)

def execute(con, stmt, params=None):
    ###
    # Run this statement, returning the rowcount instead of any results
    ###
    cur = con.cursor()
    cur.execute(stmt, params)
    retval = cur.rowcount
    cur.close()
    return retval


def insert(con, tableName: str, rowDict: dict, excludeKeys=None,
           return_columns=None):

    ###
    # Build and execute an insert statement given a dict
    # describing column/values.
    #
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

    if excludeKeys is None:
        excludeKeys = ()

    for colName in nameList:
        if colName not in excludeKeys:
            val = rowDict[colName]
            # Don't need to add insert null records
            if val is not None:
                ccw(colName)

                if not isinstance(val, LiteralValue):
                    vcw('%%(%s)s' % colName)
                else:
                    # Want to embed literal expression,
                    # like 'now()' or so forth.
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


def update(con, table_name: str,
           where_columns_and_values: list,
           update_columns_and_values: list):

    # Should be of form [ ('foo=%s', 12), ('bar < %s', 55) ]
    # to build up where clause
    assert(all(len(p) == 2 and type(p[0]) is str and
           '%s' in p[0] for p in where_columns_and_values))

    # Should be of form [('blat', 45), ('sdf', 99)]
    # for columns to update + value to update to
    assert(all(len(p) == 2 and type(p[0]) is str and
           '%s' not in p[0] for p in update_columns_and_values))

    # "foo=%s, bar=%s" ...
    update_column_part = ', '.join('%s = %%s' % colname
                                   for colname, _
                                   in update_columns_and_values)
    # (12, 'barvalue') ...
    values = [v for _, v in update_columns_and_values]

    where_column_part = ', '.join(colexpr for colexpr, _
                                  in where_columns_and_values)

    values.extend(v for _, v in where_columns_and_values)

    # Psycopy desires a tuple wrapping values
    values_tuple = tuple(values)

    statement = 'update %s set %s where %s' % \
                (table_name, update_column_part, where_column_part)

    return execute(con, statement, values_tuple)


def get_pct_s_string(values):
    pcts = ['%s'] * len(values)
    return ','.join(pcts)

def batched_bulk_insert(con, tableName: str, rowDicts, batch_size=500, **kwargs):
    """ Call bulk_insert in batches of batch_size rows drained from
        rowDicts in a generator-friendly manner. """

    rows_iter = iter(rowDicts)

    batch = list(itertools.islice(rows_iter, batch_size))
    while batch:
        bulk_insert(con, tableName, batch, **kwargs)
        batch = list(itertools.islice(rows_iter, batch_size))



def bulk_insert(con, tableName: str, rowDictList: list,
                colList=None, excludeKeys=None,
                addToEveryRow=None, return_column=None,
                batch_size=None):
    ###
    #   Bulk-insert the rows in rowDictList using single-round trip
    #     "insert into ... values (), (), ... ()"
    #
    #   One round-trip instead of embedding insert() calls inside of loops
    #     for inserting rows into the same table.
    #
    #   Returns either the count of inserted rows [ default ],
    #     or, if return_column specifies the name of a column
    #     to return [ 'id' ], then will be a list of that column
    #     value, parallel to the rows in rowDictList.
    ###

    if not rowDictList:
        # Nothing to insert!
        return None

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

    statement_buf = ['insert into %s (%s) values ' %
                     (tableName, ', '.join(tableColList))]

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

    # Otherwise just the rowcount
    return rc

class QueryTool(QueryBuilder):
    #
    # A QueryBuilder which holds a connection and
    # has query(), query_one(), etc. methods to run
    # the built-up query.

    def __init__(self, con):
        QueryBuilder.__init__(self)
        self._con = con

    def query_single_value(self):
        return query_single_value(self._con, self.statement, self.parameters)

    def query_single_column(self):
        return query_single_column(self._con, self.statement, self.parameters)

    def query_single_row(self):
        return query_single_row(self._con, self.statement, self.parameters)

    query_single = query_single_row # Alias

    def query(self):
        return query(self._con, self.statement, self.parameters)

    def query_json_strings(self):
        return query_json_strings(self._con, self.statement, self.parameters)

    def query_as_json(self):
        return query_as_json(self._con, self.statement, self.parameters)

    def query_single_column_as_json_array(self):
        return query_single_column_as_json_array(self._con, self.statement, self.parameters)



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
        return None


def introspect_schema(conn, schema_name, fully_qualify_tables=False):

    # Learn about tables and columns
    tables = query(conn, """
        SELECT
            t.table_name
        FROM information_schema.tables t
        WHERE
            t.table_schema = %s
            and t.table_name != 'pg_stat_statements'
        order by 1
    """, (schema_name,))

    columns = query(conn, """
        select t.table_name, c.column_name, c.data_type
        from information_schema.columns c
            join information_schema.tables t using (table_schema, table_name)
        where
            c.table_schema = %s
            and t.table_name != 'pg_stat_statements'
        order by t.table_name, c.ordinal_position
    """, (schema_name,))

    # Stitch into objects.
    key_prefix=schema_name + '.' if fully_qualify_tables else ''
    tables_by_name = {}
    for t in tables:
        tables_by_name[key_prefix + t.table_name] = MetadataTable(schema_name, t.table_name)

    del t

    columns_by_table_name = defaultdict(list)
    for c in columns:
        columns_by_table_name[key_prefix + c.table_name].append(MetadataColumn(c.column_name, c.data_type))

    del c

    for tname, column_objs in columns_by_table_name.items():
        tables_by_name[tname].set_columns(column_objs)

    return tables_by_name

def introspect_table(conn, table_name):
    if '.' in table_name:
        schema_name, table_name = table_name.split('.')

        data = query(conn, """
            select c.column_name, c.data_type
            from information_schema.columns c
                join information_schema.tables t using (table_schema, table_name)
            where
                c.table_schema = %s
                and t.table_name = %s
            order by t.table_name, c.ordinal_position
        """, (schema_name, table_name))

    else:
        data = query(conn, """
            select c.table_schema, c.column_name, c.data_type
            from information_schema.columns c
                join information_schema.tables t using (table_schema, table_name)
            where
                t.table_name = %s
            order by t.table_name, c.ordinal_position
        """, (table_name, ))

        observed_schemas_with_that_table_name = set(d.table_schema for d in data)
        if len(observed_schemas_with_that_table_name) != 1:
            raise Exception('Multiple tables in database with name %s (schemas %s)! Please pass in fully qualified table name' %\
                    (table_name, observed_schemas_with_that_table_name))

    return [ MetadataColumn(d.column_name, d.data_type) for d in data]


class MetadataColumn:
    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type

    def __repr__(self):
        return '%s:%s' % (self.name, self.data_type)

class MetadataTable:
    def __init__(self, schema_name, table_name):
        self.schema_name = schema_name
        self.name = table_name
        self.columns = None

    def set_columns(self, columns):
        self.columns = columns

    def __repr__(self):
        return 'Table "%s.%s": %s' % (self.schema_name, self.name, self.columns)

