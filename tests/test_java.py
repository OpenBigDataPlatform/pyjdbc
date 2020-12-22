"""
Tests for `java` module in pyjdbc
"""
import unittest
import jpype
import jpype.imports

from pyjdbc.java import Classpath


class TestClasspath(unittest.TestCase):

    def test_jar_load(self):
        return
        self.assertTrue(jpype.isJVMStarted())
        SQLiteConnection = Classpath.load_jar_class('tests/files/sqlite-jdbc-3.21.0.jar', 'org.sqlite.SQLiteConnection')
        # fileName can be :memory:
        con = SQLiteConnection('jdbc:sqlite', ':memory:')
        con.prepareStatement('SELECT 1')