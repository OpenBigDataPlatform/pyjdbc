"""
Tests for `java` module in pyjdbc
"""
import unittest
import pytest
import jpype
import jpype.imports

from pyjdbc.java import Classpath


def deco(name=None):

    def wrapper(func):
        import inspect
        print(inspect.getfullargspec(func).args)
        print('>>>> NAME:', name)
        print('>>>>', func.__name__)
        return func

    return wrapper


class HiveConnectFn:

    @deco(name='asdf')
    def fancy_function(self):
        pass


class TestClasspath(unittest.TestCase):

    def test_jar_load(self):
        self.assertTrue(jpype.isJVMStarted())
        SQLiteConnection = Classpath.load_jar_class('sqlite-jdbc-3.21.0.jar', 'org.sqlite.SQLiteConnection')
        # fileName can be :memory:
        con = SQLiteConnection('jdbc:sqlite', ':memory:')
        stmt = con.createStatement()

    def test_arg_spec(self):
        hive = HiveConnectFn()
        hive.fancy_function()


