"""
sqlite module for pyjdbc

This module is for testing basic pyjdbc functionality and should NOT be used to actually interact with sqlite
The built-in sqlite library for Python should ALWAYS be preferred.
"""
__all__ = ['connect']

from pyjdbc.connect import ArgumentParser, ArgumentOpts, ConnectFunction, ConnectArguments
from pyjdbc.java import Properties
from pyjdbc.dbapi import JdbcConnection, JdbcCursor
from jpype import JClass


class SqliteArgParser(ArgumentParser):
    """
    Sqlite connection arguments and rules
    """
    file_name = ArgumentOpts(position=0, argtype=str, description='sqlite database file path, or ":memory:"')
    driver = ArgumentOpts(argtype=str, description='path to sqlite jdbc driver jar')
    properties = ArgumentOpts(argtype=dict, default={})


class SqliteConnect(ConnectFunction):
    SQLITE_URL = 'jdbc:sqlite'

    def handle_args(self, arguments: ConnectArguments):
        if arguments.get('driver'):
            self.driver_path = arguments.driver

    def get_connection(self, driver_class: JClass, args: ConnectArguments):
        SQLiteConnection = driver_class
        java_props = Properties.from_dict(args.properties)

        # SQLiteConnection(String url, String fileName, Properties prop)
        java_conn = SQLiteConnection(self.SQLITE_URL, args.file_name, java_props)
        return JdbcConnection(connection=java_conn,
                              cursor_class=self.cursor_class,
                              type_conversion=self.type_conversion())


connect = SqliteConnect(driver_path='sqlite-jdbc-3.21.0.jar',
                        driver_class='org.sqlite.SQLiteConnection',
                        parser=SqliteArgParser)