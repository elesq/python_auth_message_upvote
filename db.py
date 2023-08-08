from psycopg2 import connect, sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from os import environ as env

# parses the .env file if found and load all the
# contained variables into the environment of the
# application.
load_dotenv()


class Database:
    def __init__(self):
        self.conn = None
        self.cursor = None

    @staticmethod
    def _compose_kv_and(separator=" AND ", kv_pairs=None):
        return sql.SQL(separator).join(
            sql.SQL(" {} = {} ").format(
                sql.Identifier(k), sql.Literal(v)) for k, v in kv_pairs
        )

    def open(self, url=None):
        if not url:
            url = env.get("CONNECTION_URL")
        self.conn = connect(url)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

    def write(self, tablename: str, cols: list[str], values: list):
        query = sql.SQL("insert into {} ({}) values ({}) returning id").format(
            sql.Identifier(tablename),
            sql.SQL(",").join(map(sql.Identifier, cols)),
            sql.SQL(",").join(map(sql.Literal, values))
        )
        self.cursor.execute(query)
        self.conn.commit()
        return self.cursor.fetchone().get('id')

    def get(self,
            tablename: str,
            cols: list[str],
            limit: int = None,
            where: dict = None,
            or_where: dict = None,
            contains: dict = None):
        query = sql.SQL("select {} from {}").format(
            sql.SQL(',').join(map(sql.Identifier, cols)),
            sql.Identifier(tablename)
        )
        if where:
            query += sql.SQL(" where {}").format(
                self._compose_kv_and(kv_pairs=where.items())
            )

        if contains:
            if where:
                query += sql.SQL(" and ")
            else:
                query += sql.SQL(" where ")

            query += sql.SQL(" or ").join(
                sql.SQL("{} like {}").format(
                    sql.Identifier(k),
                    sql.Literal(f"%{v}%")
                ) for k, v in contains.items()
            )

        if where and or_where:
            query += sql.SQL(" OR ({})").format(
                self._compose_kv_and(kv_pairs=or_where.items())
            )

        if limit:
            query += sql.SQL(" limit {}").format(sql.Literal(limit))

        print(query.as_string(self.conn))
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_one(self, tablename: str, cols: list[str], where: dict = None, or_where: dict = None):
        result = self.get(tablename, cols, 1, where, or_where)
        if len(result):
            return result[0]

    def get_contains(self, tablename: str,
                     cols: list[str],
                     search: str,
                     limit: int = None):
        query = sql.SQL("select {} from {} where {}").format(
            sql.SQL(',').join(
                map(sql.Identifier, cols)
            ),
            sql.Identifier(tablename),
            sql.SQL(" or ").join(
                sql.SQL("{} like {}").format(
                    sql.Identifier(k), sql.Literal(f"'%{search}%'")) for k in cols
            )
        )
        if limit:
            query += sql.SQL(" limit {}").format(sql.Literal(limit))
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def update(self, tablename: str, cols: list[str], data: list, where: dict = None):
        where_clause = sql.SQL("")
        if where:
            where_clause = sql.SQL("where {}").format(
                self._compose_kv_and(kv_pairs=where.items())
            )

        query = sql.SQL("update {} set {} {} ").format(
            sql.Identifier(tablename),
            self._compose_kv_and(separator=",", kv_pairs=zip(cols, data)),
            where_clause
        )
        self.cursor.execute(query)
        self.conn.commit()
        return self.cursor.rowcount

    def delete(self, tablename: str, where: dict = None):
        where_clause = sql.SQL("")
        if where:
            where_clause = sql.SQL(" where {}").format(
                self._compose_kv_and(kv_pairs=where.items())
            )

        query = sql.SQL("delete from {} {}").format(
            sql.Identifier(tablename),
            where_clause
        )
        self.cursor.execute(query)
        self.conn.commit()
        return self.cursor.rowcount

    def close(self):
        self.cursor.close()
        self.conn.close()
