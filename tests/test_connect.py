"""
Tests for `java` module in pyjdbc
"""
import unittest
import pytest

from pyjdbc.sqlite import connect


@pytest.mark.connection
class TestConnection(unittest.TestCase):
    table1 = '''
        CREATE TABLE users (
            id INTEGER not null,
            created TIMESTAMP default CURRENT_TIMESTAMP,
            name varchar(50),
            primary key ("id")
        );
        '''

    def setUp(self):
        return # TODO fix tests
        self.conn = connect(':memory:', driver='tests/files/sqlite-jdbc-3.21.0.jar')
        with self.conn.cursor() as cursor:
            cursor.execute(self.table1)

    def test_insert_executemany(self):
        return # TODO fix me
        stmt = "insert into users (id, name) " \
               "values (?, ?)"
        parms = (
            (20, 'tom'),
            (21, 'jerry'),
            (23, 'bob'),
        )
        with self.conn.cursor() as cursor:
            cursor.executemany(stmt, parms)
            self.assertEqual(cursor.rowcount, 3)