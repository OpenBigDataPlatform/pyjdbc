"""
dbapi.py - this module stores the base classes for Cursors, and Connection objects for Python's DB API 2.0 spec
Also known as Pep 249 - https://www.python.org/dev/peps/pep-0249/
"""
__all__ = ['JdbcConnection', 'JdbcCursor', 'JdbcDictCursor']
from jpype import JClass

from pyjdbc.types import JdbcTypeConversion
from pyjdbc.exceptions import (Error,
                               Warning as JdbcWarning,
                               InterfaceError,
                               DatabaseError,
                               InternalError,
                               OperationalError,
                               ProgrammingError,
                               IntegrityError,
                               DataError,
                               NotSupportedError)


class JdbcConnection:

    def __init__(self, connection, cursor_class, type_conversion=None):
        """

        :param connection: java.sql.Connection
        :param cursor_class: pyjdbc.dbapi.JdbcCursor or subclass
        :param type_conversion:
        """
        self._connection = connection
        self._cursor_class = cursor_class
        self._type_conversion = type_conversion

        if not issubclass(cursor_class, JdbcCursor):
            raise ValueError('"cursor_class" must be instance of {}, got: {}, {}'.format(
                JdbcCursor.__name__,
                type(cursor_class),
                getattr(cursor_class, '__name__')
            ))

        if not isinstance(type_conversion, JdbcTypeConversion):
            raise ValueError('"type_conversion" must be instance of {}, got: {}'.format(
                JdbcTypeConversion.__name__,
                type(type_conversion)
            ))

    def is_closed(self):
        return self._connection.isClosed()

    def jdbc_connection(self):
        return self._connection

    def close(self):
        if self._connection.isClosed():
            raise Error('Connection already closed')
        self.jconn.close()

    def commit(self):
        # TODO exception handling
        self._connection.commit()

    def rollback(self):
        # TODO exception handling
        self._connection.rollback()

    def cursor(self):
        """
        Create a new cursor

        :return:
        :rtype: JdbcCursor
        """
        return self._cursor_class(self, self._type_conversion)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class JdbcCursor:

    batch_size = 1

    def __init__(self, connection: JdbcConnection, type_conversion, rowcounts=True):
        self._connection = connection
        self._type_conversion = type_conversion
        self._description = None
        self._metadata = None
        self._statement = None
        self._resultset = None
        self._rowcount = -1
        self._get_rowcounts = rowcounts

        if not isinstance(connection, JdbcConnection):
            raise ValueError('"connection" arg must be instance of {}'.format(str(JdbcConnection)))

    @property
    def rowcount(self):
        """
        cursor rowcount

        returns -1 for unknown rowcount

        :return: rowcount
        :rtype: int
        """
        return self._rowcount

    @property
    def column_names(self):
        if not self._metadata:
            return None

        meta = self._metadata

        columns = range(1, meta.getColumnCount() + 1)
        names = [str(meta.getColumnName(col)) for col in columns]

        if not names:
            return

        # detect common prefix - if all columns are prefixed with the same table-name, remove that
        # prefix. There can be no ambiguity so the prefix is not needed.
        if '.' in names[0]:
            prefix = names[0].split('.', 1)[-1] + '.'
            if all([n.startswith(prefix) for n in names]):
                names = [names.replace(prefix, '', 1)]
            else:
                # it's also possible all the column names are unique without a prefix
                no_prefix = [n.split('.', 1)[1] for n in names]
                if len(set(no_prefix)) == len(names):
                    names = no_prefix

        return names

    @property
    def description(self):
        if self._description:
            return self._description
        if not self._metadata:
            return

        meta = self._metadata
        count = meta.getColumnCount()
        self._description = []
        for col in range(1, count + 1):
            size = meta.getColumnDisplaySize(col)
            jdbc_type = meta.getColumnType(col)
            if jdbc_type == 0:  # NULL
                type_desc = 'NULL'
            else:
                dbapi_type = self._type_conversion.py_type(jdbc_type)
                dbapi_type_str = getattr(dbapi_type, '__name__', None) or str(dbapi_type)
                jdbc_type_name = (self._type_conversion.jdbc_name(jdbc_type) or str(meta.getColumnTypeName(col)).upper())
                type_desc = '{} - JDBC:{}'.format(dbapi_type_str, jdbc_type_name)

            # some drivers return Integer.MAX_VALUE when the metadata is not present
            max_int = JClass('java.lang.Integer').MAX_VALUE

            size = size if size != max_int else None
            precision = meta.getPrecision(col)
            precision = precision if precision != max_int else None
            scale = meta.getScale(col)
            scale = scale if scale != max_int else None

            col_desc = (meta.getColumnName(col),  # name
                        type_desc,               # type_code
                        size,                     # display_size
                        size,                     # internal_size
                        precision,   # precision
                        scale,       # scale
                        meta.isNullable(col))     # null_ok

            self._description.append(col_desc)

        return self._description

    def _clear_metadata(self):
        self._metadata = None
        self._description = None

    def _clear_connection(self):
        self._connection = None

    def _close_statement(self):
        if self._statement:
            self._statement.close()
        self._statement = None

    def _close_resultset(self):
        if self._resultset_valid():
            self._resultset.close()
        self._resultset = None

    def _connection_valid(self):
        return not self._connection.is_closed()

    def _resultset_valid(self):
        return bool(self._resultset)

    def close(self):
        """
        close the cursor, the cursor cannot be used again.
        :return:
        """
        # close all open resources.
        self._clear_connection()
        self._clear_metadata()
        try:
            self._close_statement()
        except Exception:
            pass

        try:
            self._close_resultset()
        except Exception:
            pass

    def _reset(self):
        # close any previously used resources
        self._clear_metadata()
        self._close_statement()
        self._close_resultset()

    def _warnings(self):
        if not self._resultset_valid():
            return ''

        warning = self._resultset.getWarnings()
        if not warning:
            return ''

        try:
            return 'cursor warning: {}'.format(str(warning.getMessage()))
        except Exception:
            return ''

    def execute(self, operation, params=None):
        if not self._connection_valid():
            raise Error('the connection has been closed')

        self._reset()

        conn = self._connection.jdbc_connection()

        # prepare the statement
        self._statement = stmt = conn.prepareStatement(operation)

        for column, value in enumerate(params or [], start=1):
            # note that in JDBC the first column index is 1
            stmt.setObject(column, value)

        has_resultset = stmt.execute()  # TODO exception handling

        self._rowcount = -1
        if has_resultset:
            self._resultset = resultset = stmt.getResultSet()
            self._metadata = resultset.getMetaData()

            # get rowcount
            if self._get_rowcounts:
                try:
                    if self._resultset.last():  # if the cursor can be moved to the last row.
                        self._rowcount = resultset.getRow()
                    resultset.beforeFirst()
                except JClass('java.sql.SQLException'):
                    # ResultSet.last() is not supported
                    pass

        else:
            try:
                self._rowcount = stmt.getUpdateCount()
            except JClass('java.sql.SQLException'):
                # not supported
                pass

    def executemany(self, operation, seq_of_parameters):
        self._reset()
        conn = self._connection.jdbc_connection()
        self._statement = stmt = conn.prepareStatement(operation)
        for params in seq_of_parameters:
            for column, value in enumerate(params or [], start=1):
                stmt.setObject(column, value)
            self._statement.addBatch()
        batch_rowcounts = stmt.executeBatch()
        self._rowcount = sum(batch_rowcounts)
        self._reset()

    def fetchone(self):
        if not self._resultset_valid():
            raise DatabaseError('result set is no longer valid ' + self._warnings())

        if not self._resultset.next():
            return None  # end of result set

        row = []
        num_cols = self._metadata.getColumnCount()
        for column_number in range(1, num_cols + 1):
            jdbc_type = self._metadata.getColumnType(column_number)
            pyvalue = self._type_conversion.py_value(self._resultset, column_idx=column_number, jdbc_code=jdbc_type)
            row.append(pyvalue)

        return tuple(row)

    def fetchmany(self, size=None):
        # TODO implement this in a java class
        if not self._resultset_valid():
            raise DatabaseError('result set is no longer valid ' + self._warnings())

        if size is None:
            size = self.batch_size
        # TODO: handle SQLException if not supported by db
        self._resultset.setFetchSize(size)
        rows = []
        row = None
        for i in range(size):
            row = self.fetchone()
            if row is None:
                break
            else:
                rows.append(row)

        # reset fetch size
        if row:
            # TODO: handle SQLException if not supported by db
            self._resultset.setFetchSize(0)
        return rows

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            else:
                rows.append(row)
        return rows

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                break
            else:
                yield row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class JdbcDictCursor(JdbcCursor):

    def fetchone(self):
        row = super().fetchone()
        if row:
            names = self.column_names
            return dict(zip(names, row))

