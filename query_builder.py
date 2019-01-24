class QueryBuilder:
    ###
    # Build up a SELECT programmatically.
    ###

    def __init__(self, kind='SELECT'):
        self._kind = kind

        self._projections = []
        self._main_relation = None
        self._joins = []
        self._where = None
        self._group_by = []

        self._parameters = []

    def relation(self, main_relation_to_query):
        self._main_relation = main_relation_to_query
        return self

    def join(self, relation, on=None, using=None, params=None, kind='INNER'):
        assert len(list(v for v in (on, using) if v)) == 1, \
                                    'At most one of on or using'

        self._joins.append((relation, on, using, kind, params))
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

    def group_by(self, *args):
        self._group_by.extend(args)
        return self

    def where(self, expression, *params):
        if isinstance(expression, ExpressionAndParams):
            assert not params
            self._where = expression.expression
            self._parameters.extend(expression.parameters)
        else:
            self._where = expression
            self._parameters.extend(params)

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
                if params:
                    if isinstance(params, tuple):
                        self._parameters.extend(params)
                    else:
                        self._parameters.append(params)

        if self._where:
            buf.append('WHERE')
            buf.append(self._where)


        if self._group_by:
            buf.append('GROUP BY')
            buf.append(', '.join(str(gb) for gb in self._group_by))

        return ' '.join(buf)

    @property
    def parameters(self):
        return tuple(self._parameters)

class ExpressionAndParams:
    def __init__(self, expression, parameters):
        self.expression = expression
        self.parameters = parameters

    @classmethod
    def binary_expression(cls, op, args):
        params = []
        exp_buf = []

        for arg in args:
            if isinstance(arg, str):
                # Just a bare expression, no param list.
                exp_buf.append(arg)
            elif isinstance(arg, tuple):
                assert len(arg) == 2, \
                    'Expect only a pair of (expression, param (str or tuple)'

                expr, prms = arg
                exp_buf.append(expr)
                if isinstance(prms, tuple):
                    # More than one param provided
                    params.extend(prms)
                else:
                    # Just one single param of one kind or another.
                    params.append(prms)
            elif isinstance(arg, ExpressionAndParams):
                exp_buf.append(arg.expression)
                params.extend(arg.parameters)

            else:
                raise Exception('Unknown parameter: %r' % arg)

        if len(exp_buf) > 1:
            # op (AND or OR) join these buggers
            spaced_op = ' %s ' % op
            expression = spaced_op.join('(' + e + ')' for e in exp_buf)

        else:
            # No need for or and parens.
            expression = exp_buf[0]

        # Compatible with being fed right into .where()
        return ExpressionAndParams(expression, params)

def OR(*args):
    return ExpressionAndParams.binary_expression('OR', args)

def AND(*args):
    return ExpressionAndParams.binary_expression('AND', args)