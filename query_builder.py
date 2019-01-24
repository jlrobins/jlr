import collections

class QueryBuilder:
    ###
    # Build up a SELECT programmatically.
    ###

    def __init__(self, toplevel_clause=None, kind='SELECT'):
        self._kind = kind

        if not toplevel_clause:
            toplevel_clause = AND()
        self._where = toplevel_clause

        self._projections = []
        self._main_relation = None
        self._joins = []

        self._group_by = []

        self._having = []
        self._having_params = []

        self._limit = None
        self._offset = None

        self._relation_aliases = set()

    def relation(self, main_relation_to_query):
        self._main_relation = main_relation_to_query
        self._scan_alias(main_relation_to_query)

        return self

    def join(self, relation, on=None, using=None, params=None, kind='INNER'):
        assert len(list(v for v in (on, using) if v)) == 1, \
                                    'At most one of on or using'

        join_tuple = (relation, on, using, kind, params)

        # Multiple calls to join() with same params become
        # no-ops. Lets distinct blocks of code needing to
        # add the same join in order to ultimately add in additional
        # where clauses happen w/o additional communication.
        if join_tuple not in self._joins:
            self._joins.append(join_tuple)
            self._scan_alias(relation)

        return self

    def left_join(self, relation, on=None, using=None, params=None):
        return self.join(relation, on=on, using=using,
                        kind='LEFT', params=params)

    def right_join(self, relation, on=None, using=None, params=None):
        return self.join(relation, on=on, using=using,
                         kind='RIGHT', params=params)

    def outer_join(self, relation, on=None, using=None, params=None):
        return self.join(relation, on=on, using=using,
                          kind='FULL OUTER', params=params)

    def project(self, *args):
        self._projections.extend(args)
        return self

    def having(self, expression, *params):
        self._having.append(expression)
        self._having_params.extend(params)
        return self

    def group_by(self, *args):
        self._group_by.extend(args)
        return self

    def where(self, *args):
        self._where.append(args)
        return self

    def limit(self, value: int, offset=None):
        assert isinstance(value, int)
        assert isinstance(offset, int) or offset is None
        assert value >= 0

        self._limit = value
        self._offset = offset
        return self

    def offset(self, offset):
        assert isinstance(offset, int) and offset > 0
        assert self._limit is not None
        self._offset = offset

        return self

    @property
    def statement(self):
        assert self._projections

        buf = [self._kind]
        buf.append(', '.join(self._projections))

        if self._main_relation:
            buf.append('FROM')
            buf.append(self._main_relation)

        if self._joins:
            assert self._main_relation, 'Can only join given a main relation'
            for relation, on, using, kind, params in self._joins:
                how_expression = on or using
                assert how_expression
                if on:
                    assert not using
                    how = 'ON'
                else:
                    assert using
                    how = 'USING'

                buf.append('%s JOIN %s %s (%s)' % (
                            kind, relation, how, how_expression))


        if self._where.expression:
            buf.append('WHERE')
            buf.append(self._where.expression)


        if self._group_by:
            buf.append('GROUP BY')
            buf.append(', '.join(str(gb) for gb in self._group_by))

        if self._having_params:
            buf.append('HAVING')
            buf.append(', '.join(str(h) for h in self._having))

        if self._limit:
            buf.append('LIMIT %s')
        if self._offset:
            buf.append('OFFSET %s')

        return ' '.join(buf)

    @property
    def parameters(self):
        params = []

        # join params are buried as the last member of
        # the _joins tuples. See .join().
        for j in self._joins:
            join_params = j[-1]
            if join_params:
                if isinstance(join_params, tuple):
                    params.extend(join_params)
                else:
                    params.append(join_params)

        params.extend(self._where.parameters)
        params.extend(self._having_params)

        if self._limit:
            params.append(self._limit)
            if self._offset:
                params.append(self._offset)

        return tuple(params)

    def _scan_alias(self, relation_expr):
        assert '"' not in relation_expr, \
            'Not smart enough for quoted relations, masochist!'

        if ' ' in relation_expr:
            # Not smart enough for 'as' kword yet, but that
            # goes in here when/if time comes. We'll vomit
            # if there's more than one space joined word
            # at least.
            relation, alias = relation_expr.split(' ')
            if alias in self._relation_aliases:
                raise AliasException('Already using relation alias %s' % alias)
            self._relation_aliases.add(alias)

class AliasException(Exception):
    pass

class ExpressionAndParams:
    def __init__(self, operator: str, operands):
        self.operator = operator
        self._operands = []

        if operands:
            assert isinstance(operands, collections.Sequence) \
                and not isinstance(operands, str)

            for op in operands:
                # make use of member-wise check in this method.
                self.append(op)

        # String, as in '(foo=%s) AND (bar like %s)', determined very late
        # in _expand() when diven by 1st dereference to either
        # .expression or .parameters properties.
        self._expression = None

        # Tuple, as in (42, 'Joe %'), , determined very late
        # in _expand() when diven by 1st dereference to either
        # .expression or .parameters properties.
        self._parameters = None


    def append(self, *args):
        # args should be one of:
        #
        #   0) a single tuple, from .where's *args. Process its single member
        #   1) expression string, tuple of multiple params
        #   2) expression string, param single non-tuple
        #   3) expression string only, no additional params.
        #   4) expression string, more than one arg not wrapped in tuple.
        #   5) a single subordinate ExpressionAndParams clause.

        # In the case of 2), we go ahead and wrap as a singleton tuple
        # just to make _expand() a little simpler in only needing to then
        # deal with two cases.

        # In case of 3) we also make things a little easier for _expand,
        # by tacking on an empty tuple as the params for that clause.

        # Likewise for 4) : wrap the trailing parameters in a single tuple.

        # Case 0:
        if len(args) == 1 and isinstance(args, tuple):
            args = args[0]

        # Case 1: expression string, tuple of multiple params
        if isinstance(args, tuple) and len(args) == 2 and isinstance(args[0], str) \
                and isinstance(args[1], tuple):
            self._operands.append(args)
        # Case 2: expression string, param single non-tuple
        elif isinstance(args, tuple) and len(args) == 2 and isinstance(args[0], str) \
                and not isinstance(args[1], tuple):
            self._operands.append(tuple((args[0], (args[1],))))

        # Case 3: expression string only in a tuple, no additional params.
        elif isinstance(args, tuple) and len(args) == 1 and isinstance(args[0], str):
            self._operands.append((args[0], ()))

        # Case 3a: bare expression string, no tuple wrapping
        elif isinstance(args, str):
            self._operands.append((args, ()))

        # Case 4: expression string, more than one parameter not wrapped
        # in a tuple. Bundle 'em, all up into a tuple.
        elif isinstance(args, tuple) and len(args) > 2 and isinstance(args[0], str):
            self._operands.append((args[0], tuple(args[1:])))
        # Case 5: a single subordinate ExpressionAndParams clause.
        elif isinstance(args, ExpressionAndParams):
            self._operands.append(args)
        # Case 6: a tuple containing an ExpressionAndParams
        elif isinstance(args, tuple) and len(args) == 1 \
                and isinstance(args[0], ExpressionAndParams):
            self._operands.append(args[0])
        else:
            raise TypeError("Don't know how to handle %r: %s %s" %
                    (args, len(args), isinstance(args[0], str),))


    @property
    def expression(self):
        if self._expression is None:
            self._expand()

        return self._expression

    @property
    def parameters(self):
        if self._parameters is None:
            self._expand()

        return self._parameters


    def _expand(self):

        ##
        # Assign to ._expression and ._params
        # by visiting all in ._operands
        ##

        params = []
        exp_buf = []

        for o in self._operands:
            # Sanity check operand. Should be either
            #   1) tuple of (str expression, tuple of params)
            #   2) subordinate ExpressionAndParams instances.
            assert (isinstance(o, tuple)
                        and len(o) == 2
                        and isinstance(o[0], str)
                        and isinstance(o[1], tuple)) or (
                    isinstance(o, ExpressionAndParams)
                    ), 'Unexpected member of ._operands: %r' % (o,)

            if isinstance(o, tuple):
                exp_buf.append(o[0])
                params.extend(o[1])
            elif isinstance(o, ExpressionAndParams):
                # Recurse into it ...
                exp_buf.append(o.expression)
                params.extend(o.parameters)
            else:
                raise Exception('Unknown operand: %r' % (arg,))

        if len(exp_buf) > 1:
            # operator (AND or OR) join these buggers
            spaced_op = ' %s ' % self.operator
            expression = spaced_op.join('(' + e + ')' for e in exp_buf)

        elif len(exp_buf) == 1:
            # Just one single clause, no need for joining together.
            expression = exp_buf[0]
        else:
            # No clause at all!
            expression = ''

        self._expression = expression
        self._parameters = params

def OR(*args):
    return ExpressionAndParams('OR', args)

def AND(*args):
    return ExpressionAndParams('AND', args)
