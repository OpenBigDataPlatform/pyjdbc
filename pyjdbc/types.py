"""
JDBC Type handling
"""
import inspect
import datetime
from decimal import Decimal
from jpype import JClass


class JdbcType:

    def __init__(self, getter, setter, pytype, name=None, resultset=None, fn=None, decorator=None):
        """
        Conversion settings for a jdbc type.

        :param getter: the result-set method to retrieve the data-type
        :param setter: the result-set method to set the data-type
        :param pytype: the python type, if callable, and no `fn` is set the data type will be
                       passed through as a function, ie: ``str(value)``
                       a value of ``object`` indicates that any type can be returned and no evaluation
                       or comparison will take place.
        :param name: the datatype name in uppercase, must correspond to java type name
        :param resultset: indicates that ``fn`` expects a call signature with ``fn(resultset, column_idx)``
        :param fn: a function to be applied to the value from `getter`.
                   the function signature should be ``fn(value)`` where value is a variadic type
                   from the result-set ``getter`` method.
                   When this argument is defined any type can be returned, the return type is not validated
                   against ``pytype``
        :param decorator: for use by the decorator ``jdbctype``
        """
        self._name = None
        self._getter = None
        self._setter = None
        self._pytype = None
        self._resultset = None
        self._fn = None
        self._decorator = None

        self.name = name
        self.getter = getter
        self.setter = setter
        self.pytype = pytype
        self.resultset = resultset
        self.decorator = decorator

        if fn:
            self.fn = fn

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, typename):
        self._name = typename

    @property
    def getter(self):
        return self._getter

    @getter.setter
    def getter(self, method):
        self._getter = method

    @property
    def setter(self):
        return self._setter

    @setter.setter
    def setter(self, method):
        self._setter = method

    @property
    def resultset(self):
        return self._resultset

    @resultset.setter
    def resultset(self, use_resultset):
        self._resultset = use_resultset

    @property
    def pytype(self):
        return self._pytype

    @pytype.setter
    def pytype(self, pytype):
        self._pytype = pytype

    @property
    def fn(self):
        return self._fn

    @fn.setter
    def fn(self, fn):
        if not callable(fn):
            raise ValueError('"fn" must be callable, got: {}'.format(type(fn)))
        self._fn = fn

    @property
    def decorator(self):
        return self._decorator

    @decorator.setter
    def decorator(self, is_decorator):
        self._decorator = is_decorator


def jdbctype(name=None,
             getter=None,
             setter=None,
             resultset=None,
             pytype=None):
    """

    :param name:
    :param getter:
    :param setter:
    :param pytype:
    :return:
    """

    def decorator(func):
        if name is None:
            name2 = func.__name__
        else:
            name2 = name

        fn_args = inspect.getfullargspec(func).args
        if len(fn_args) < 2:
            raise ValueError('decorated function [{}] must accept at least 1 argument <value>'.format(str(func)))

        jdbctype = JdbcType(name=name2,
                            getter=getter,
                            setter=setter,
                            pytype=pytype,
                            resultset=resultset,
                            fn=func,
                            decorator=True)
        func._jdbctype = jdbctype
        return func

    return decorator


class JdbcTypeConversion:
    """
    Default configuration for JDBC type conversion all standard types are handled here.

    You may need subclass in your driver implementation to add custom handling for certain types.
    """
    VARCHAR = JdbcType(getter='getString', setter='setString', pytype=str)
    CHAR = JdbcType(getter='getString', setter='setString', pytype=str)
    LONGVARCHAR = JdbcType(getter='getString', setter='setString', pytype=str)
    BIT = JdbcType(getter='getBoolean', setter='setBoolean', pytype=bool)
    BOOLEAN = JdbcType(getter='getBoolean', setter='setBoolean', pytype=bool)
    NUMERIC = JdbcType(getter='getBigDecimal', setter='setBigDecimal', pytype=Decimal)
    TINYINT = JdbcType(getter='getByte', setter='setByte', pytype=int)
    SMALLINT = JdbcType(getter='getInt', setter='setInt', pytype=str)
    INTEGER = JdbcType(getter='getInt', setter='setInt', pytype=str)
    BIGINT = JdbcType(getter='getInt', setter='setInt', pytype=str)
    REAL = JdbcType(getter='getReal', setter='setReal', pytype=str)
    FLOAT = JdbcType(getter='getFloat', setter='setFloat', pytype=float)
    DOUBLE = JdbcType(getter='getDouble', setter='setDouble', pytype=float)
    #VARBINARY = JdbcType(getter='', setter='', pytype=str)
    #BINARY = JdbcType(getter='', setter='', pytype=str) # TODO
    #CLOB = JdbcType(getter='', setter='', pytype=str)
    #BLOB = JdbcType(getter='', setter='', pytype=str)
    #STRUCT = JdbcType(getter='', setter='', pytype=str)
    JDBC_DEFAULT = JdbcType(getter='getObject', setter='setObject', pytype=object)

    def __init__(self):
        self._jdbc_types = {}
        self._configure_jdbc()

    def _configure_jdbc(self):
        for cls in reversed(type(self).mro()):
            for obj_name, obj in cls.__dict__.items():
                jdbc = getattr(obj, '_jdbctype', None)

                if isinstance(obj, JdbcType):
                    jdbc = obj
                elif not jdbc or not isinstance(jdbc, JdbcType):
                    continue

                # if the argument has no name, the property that defines it is the name
                if not jdbc.name:
                    jdbc.name = obj_name

                self._jdbc_types[jdbc.name] = jdbc

    def jdbc_type(self, jdbc_name, default=None):
        """

        :param jdbc_name: the jdbc type name (capitalized)
        :param default: default value to return if jdbc type isn't matched
        :return: JdbcType object that contains type mapping logic
        :rtype: JdbcType
        """
        return self._jdbc_types.get(jdbc_name, default)

    def jdbc_value(self, pyvalue, resultset, jdbc_code, jdbc_name):
        """
        Convert a python value to a JDBC value

        :param pyvalue:
        :param resultset:
        :param jdbc_code:
        :param jdbc_name:
        :return:
        """
        return

    def jdbc_name(self, jdbc_code):
        """
        Given a jdbc type code return the type name

        :param jdbc_code:
        :return:
        """
        try:
            type_name = str(JClass('java.sql.JDBCType').valueOf(jdbc_code)).upper()
        except Exception as e:
            raise ValueError('unable to resolve jdbc type code: {}'.format(jdbc_code)) from e

        return type_name

    def py_type(self, jdbc_code=None, jdbc_name=None):
        """
        Get the python type associated with the given jdbc_type
        :param jdbc_code:
        :param jdbc_name:
        :return:
        """
        if jdbc_code is not None:
            type_name = self.jdbc_name(jdbc_code)
        elif jdbc_name is not None:
            type_name = jdbc_name.upper()
        else:
            raise ValueError('one of `jdbc_code` or `jdbc_name` must be provided')

        mapper = self.jdbc_type(type_name, self.JDBC_DEFAULT)
        return mapper.pytype

    def py_value(self, resultset, column_idx, jdbc_code=None, jdbc_name=None):
        """
        Retrieve the python value for the given type

        :param resultset: Java ResultSet object
        :param column: Column index to retrieve value from
        :param jdbc_code: JDBCType code
        :param jdbc_name: JDBCType name (optional)
        :return:
        """
        # if the value is null return None
        if resultset.wasNull():
            return

        if not isinstance(column_idx, int):
            raise ValueError('"column" invalid type, expected `int` got: {}'.format(type(column_idx)))
        if column_idx < 1:
            raise ValueError('"column" invalid value: {} - jdbc columns must be 1 or greater'.format(column_idx))

        if jdbc_code is not None:
            type_name = self.jdbc_name(jdbc_code)
        elif jdbc_name is not None:
            type_name = jdbc_name.upper()
        else:
            raise ValueError('one of `jdbc_code` or `jdbc_name` must be provided')

        # get "converter" the JdbcType object that will inform us how to map this value to python
        converter = self.jdbc_type(type_name, self.JDBC_DEFAULT)

        if converter.resultset:
            # if resultset is true, this means the function signature should pass (resultset, column-index)
            if not callable(converter.fn):
                raise ValueError('JdbcType "{}" has no function associated, but resultset=True'.format(converter.name))
            try:
                if converter.decorator:
                    return converter.fn(self, resultset, column_idx)
                else:
                    return converter.fn(resultset, column_idx)
            except Exception as e:
                raise ValueError('error converting "{}" to "{}" - via resultset function: "{}", Error: {}'.format(
                    type_name, converter.pytype, converter.name, e
                )) from e
        else:
            # it's our responsibility to use the "getter" to retrieve the value automatically
            try:
                get_value = getattr(resultset, converter.getter)
            except Exception as e:
                raise AttributeError('JdbcType: {} - unable to find method named {}'.format(
                    type_name, converter.getter
                )) from e

            try:
                jdbc_value = get_value(column_idx)
            except Exception as e:
                raise AttributeError('JdbcType: {} - unable to get value via {} - {}'.format(
                    type_name, converter.getter, str(e)
                )) from e

        # if the value is null return None
        if resultset.wasNull() or jdbc_value is None:
            return

        # call the conversion function if its been set.
        if converter.fn is not None:
            if converter.decorator:
                jdbc_value = converter.fn(self, jdbc_value)
            else:
                jdbc_value = converter.fn(jdbc_value)
        # don't try to apply a type if the type is set to "object" or if its None
        # object means "any type"
        elif converter.pytype is not None and converter.pytype is not object:
            try:
                if callable(converter.pytype):
                    jdbc_value = converter.pytype(jdbc_value)
                if not isinstance(jdbc_value, converter.pytype):
                    raise ValueError('unexpected type {}, expected: {}'.format(type(jdbc_value)), converter.pytype)
            except Exception as e:
                raise ValueError('error converting "{}" to "{}" - {}'.format(
                    type_name, converter.pytype, e
                )) from e

        return jdbc_value

    @jdbctype(getter='getDate', setter='setDate', pytype=datetime.datetime)
    def DATE(self, value):

        # The following code requires Python 3.3+ on dates before year 1900.
        dt = datetime.datetime.strptime(str(value)[:10], "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")

    @jdbctype(getter='getTime', setter='setTime', pytype=datetime.time)
    def TIME(self, value):
        # Jdbc Time format is in: hh:mm:ss
        time_str = value.toString()
        return datetime.time.strptime(time_str, "%H:%M:%S")

    @jdbctype(getter='getTimestamp', setter='setTimestamp', pytype=datetime.datetime)
    def TIMESTAMP(self, value):
        dt = datetime.datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(microsecond=int(str(value.getNanos())[:6]))
        return dt

    @jdbctype(getter='getObject', setter='setDecimal')
    def DECIMAL(self, value):
        return self.decimal_and_numeric(value)

    @jdbctype(getter='getObject', setter='setNumeric')
    def NUMERIC(self, value):
        return self.decimal_and_numeric(value)

    @jdbctype(getter='getArray', setter='setArray')
    def ARRAY(self, value):
        # JPype wraps arrays with JArray - which can be accessed just like python lists
        return list(value)

    @jdbctype(getter='getMap', setter='setMap')
    def MAP(self, value):
        raise NotImplementedError(str(value))

    @jdbctype(getter='getStruct', setter='setStruct')
    def STRUCT(self, value):
        raise NotImplementedError(str(value))

    def decimal_and_numeric(self, value):
        if hasattr(value, 'scale'):
            scale = value.scale()
            if scale == 0:
                return int(value.longValue())
            else:
                return Decimal(value.doubleValue())
        else:
            return Decimal(value)