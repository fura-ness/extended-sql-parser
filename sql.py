import sqlparse


class SQL(object):

    def __init__(self, sql):
        self.sql = sql
        self.formatted_sql = sqlparse.format(self.sql, reindent=True, keyword_case='upper')
        self.formatted_sql = self.formatted_sql.strip()

        self.statements = sqlparse.parse(sql) 
        for stmt in self.statements:
            self.stmt = sqlparse.parse(sql)[0]

        self.tokens = [t for t in self.stmt.tokens if 'Whitespace' not in str(t.ttype)]
        self.tokens = [t for t in self.tokens if not isinstance(t, sqlparse.sql.Comment)]

        self.stack_depth = 0
        self.tables = []
        self.parse(self.stmt.tokens)


    def __str__(self):
        return self.formatted_sql

    def __hash__(self):
        return hash(self.formatted_sql)

    def __eq__(self, other):
        return hash(self) == hash(other)


    def get_original_sql(self):
        return sql

    def get_formatted_sql(self):
        return self.formatted_sql

    def get_statement_count(self):
        return len(self.statements)

    def is_ddl(self):
        for t in self.stmt.tokens:
            if str(t.ttype) == 'Token.Keyword.DDL':
                return True

        return False


    def is_dml(self):
        for t in self.stmt.tokens:
            if str(t.ttype) == 'Token.Keyword.DML':
                return True

        return False


    def is_read_only(self, tokens=None):

        self.stack_depth += 1

        if self.stack_depth == 1:
            self.ddl_dml_tokens = set()

        if not tokens:
            tokens = self.stmt.tokens

        for t in tokens:
            if str(t.ttype) == 'Token.Keyword.DML' or str(t.ttype) == 'Token.Keyword.DDL':
                self.ddl_dml_tokens.add(str(t).lower())
            elif isinstance(t, sqlparse.sql.Parenthesis):
                recurse_sql = str(t).strip()[1:-1]
                recurse_stmt = sqlparse.parse(recurse_sql)[0]
                self.is_read_only(recurse_stmt.tokens)

        self.stack_depth -= 1

        return self.ddl_dml_tokens == {'select'}


    def get_tables(self):
        return self.tables


    def append_table(self, t):
        self.tables.append(
            '%s.%s' % (t.get_parent_name(), t.get_real_name()) if t.get_parent_name() else t.get_real_name()
        )


    def parse(self, tokens):

        self.stack_depth += 1

        ST_BEFORE_FROM, ST_AFTER_FROM, ST_AFTER_TABLE_IDENT, ST_AFTER_JOIN, \
            ST_AFTER_SUBQUERY, ST_JOIN_STMT, ST_JOIN_COND, ST_AFTER_WHERE, \
            ST_SUBQUERY_ALIAS = range(9)

        state = ST_BEFORE_FROM

        for t in tokens:

            ttype = str(t.ttype)
            keyword = ttype == 'Token.Keyword'

            if 'Whitespace' in ttype or isinstance(t, sqlparse.sql.Comment):
                continue

            if state == ST_AFTER_WHERE:
                break

            elif state == ST_BEFORE_FROM:
                if keyword and str(t).lower() == 'from':
                    state = ST_AFTER_FROM

            elif state == ST_AFTER_FROM:

                if isinstance(t, sqlparse.sql.Identifier):
                    self.append_table(t)
                    state = ST_AFTER_TABLE_IDENT

            elif state == ST_AFTER_TABLE_IDENT:

                if keyword and 'join' in str(t).lower():
                    state = ST_JOIN_STMT
                elif keyword and str(t).lower() == 'on':
                    state = ST_JOIN_COND

            elif state == ST_JOIN_COND:

                if isinstance(t, sqlparse.sql.Identifier):
                    self.append_table(t)
                    state = ST_AFTER_TABLE_IDENT

            elif isinstance(t, sqlparse.sql.Where):
                state = ST_AFTER_WHERE


            elif state == ST_JOIN_STMT:

                if isinstance(t, sqlparse.sql.Parenthesis):
                    recurse_sql = str(t).strip()[1:-1]
                    recurse_stmt = sqlparse.parse(recurse_sql)[0]
                    self.parse(recurse_stmt.tokens)
                    state = ST_SUBQUERY_ALIAS

                elif isinstance(t, sqlparse.sql.Identifier):
                    self.append_table(t)
                    state = ST_AFTER_TABLE_IDENT

            elif state == ST_SUBQUERY_ALIAS:
                if isinstance(t, sqlparse.sql.Identifier):
                    state = ST_AFTER_TABLE_IDENT

        self.stack_depth -= 1

