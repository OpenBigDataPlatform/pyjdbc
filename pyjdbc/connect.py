"""
Functions to aid in the handling of the connect function creation
"""
import inspect
import textwrap
from os.path import isfile, isdir
from pyjdbc.helpers import doc_str
from pyjdbc.java import Classpath, Jvm
from pyjdbc.dbapi import JdbcConnection, JdbcCursor
from pyjdbc.types import JdbcTypeConversion
from pyjdbc.exceptions import DriverNotFoundError
from jpype import JClass

DECORATOR_ATTRIBUTE = '_argument'


class ArgumentOpts:
    """
    Describes a single arguments name and settings
    """

    def __init__(self,
                 name=None,
                 position=None,
                 argtype=None,
                 mandatory=None,
                 requires=None,
                 excludes=None,
                 default=None,
                 description=None,
                 secret=None,
                 choices=None,
                 fn=None):

        if position is not None and not isinstance(position, int):
            raise ValueError('{}: position must be `None` or `int`, got: {}'.format(name, type(position)))

        # positional arguments are mandatory
        if position is not None:
            mandatory=True

        if excludes is None:
            excludes = []

        if requires is None:
            requires = []

        if choices is None:
            choices = []

        if name is not None and not name.isidentifier():
            raise ValueError('argument `name` is not a valid python identifier name: {}'.format(name))

        if fn is not None and not callable(fn):
            raise ValueError('{}: fn must be `None` or a callable, got: {}'.format(name, type(fn)))

        if default is not None and mandatory:
            raise ValueError('{}: [mandatory=True] cannot be set when [default={}] is set'.format(name, default))

        self._name = name
        self._position = position
        self._argtype = argtype
        self._mandatory = mandatory
        self._requires = requires
        self._excludes = excludes
        self._default = default
        self._description = description
        self._secret = secret
        self._choices = choices
        self._fn = fn

    @property
    def name(self):
        """
        the name of the argument as it would appear in **kwargs
        :return: arg parameter name
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, keyword):
        self._name = keyword

    @property
    def position(self):
        """
        :return: index of this argument in *args list (or None)
        :rtype: int
        """
        return self._position

    @property
    def argtype(self):
        return self._argtype

    @property
    def requires(self):
        """
        Argument names that must be included when this argument is present
        :return: required names
        :rtype: list
        """
        return self._requires

    @property
    def excludes(self):
        """
        Argument names that must be excluded when this argument is present
        :return: excluded names
        :rtype: list
        """
        return self._excludes

    @property
    def mandatory(self):
        return self._mandatory

    @mandatory.setter
    def mandatory(self, is_mandatory):
        self._mandatory = is_mandatory

    @property
    def default(self):
        return self._default

    @property
    def description(self):
        return self._description

    @property
    def choices(self):
        return self._choices

    @property
    def secret(self):
        return self._secret

    @property
    def fn(self):
        return self._fn


class ArgumentParser:
    """
    Parses connection arguments, should be subclassed to implement your own arguments requirements
    """

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._named_args = {}
        self._position_args = {}

    def _register_args(self):
        # walk the class hierarchy in reverse
        for cls in reversed(type(self).mro()):
            for obj_name, obj in cls.__dict__.items():
                arg = getattr(obj, DECORATOR_ATTRIBUTE, None)

                if isinstance(obj, ArgumentOpts):
                    arg = obj

                if not isinstance(arg, ArgumentOpts):
                    continue

                # if the argument has no name, the property that defines it is the name
                if not arg.name:
                    arg.name = obj_name

                if arg.name in self._named_args:
                    raise NameError('argument is already defined: {}'.format(arg.name))

                self._named_args[arg.name] = arg

        self._register_positional()

    def _register_positional(self):
        # ensure there are not duplicate positions

        for arg in self._named_args.values():
            if arg.position is None:
                continue
            self._position_args[arg.position] = arg

        if self._position_args:
            pmax = max(self._position_args)

            inclusive = set(range(0, pmax+1))
            non_sequential = set(self._position_args) ^ inclusive
            if non_sequential:
                raise ValueError('positional arguments are not sequential, '
                                 'these positions are missing: {}'.format(list(non_sequential)))

    def _parse_args(self, *args, **kwargs):
        # TODO test for non-existant fields in requires, and excludes
        if self._position_args:
            num_positional_args = max(self._position_args.keys()) + 1
        else:
            num_positional_args = 0
        if len(args) > num_positional_args:
            raise ValueError('too many positional arguments, accepts at most: {}'.format(num_positional_args))

        # map argument-name to value for *args
        pos_values = {self._position_args[idx].name: val for idx, val in enumerate(args)}

        keyword_values = {}
        for name, value in kwargs.items():
            if name not in self._named_args:
                valid_keywords = '\n'.join(self._named_args)
                raise ValueError('invalid keyword argument: "{}", valid names:\n{}'.format(name, valid_keywords))
            if name in pos_values:
                raise ValueError('argument repeated by position and keyword: "{}"'.format(name))
            keyword_values[name] = value

        keyword_values.update(pos_values)

        # check for missing mandatory arguments
        mandatory = [arg.name for arg in self._named_args.values() if arg.mandatory and not arg.default]
        missing = set(mandatory) - set(keyword_values)
        if missing:
            missing_str = '\n'.join(missing)
            raise ValueError('required arguments missing:\n{}\n'
                             'note that all positional arguments are mandatory'.format(
                                 textwrap.indent(missing_str, ' '*4)))

        # check for valid types:
        for name, value in keyword_values.items():
            arg = self._named_args[name]
            if arg.argtype is None or value is None:
                continue

            # if argtype is a CLASS object test for subclass
            try:
                is_subclass = issubclass(value, arg.argtype)
            except TypeError:
                is_subclass = False

            # check the argument type, unless there is a function defined
            if not inspect.isclass(arg.argtype) and not isinstance(value, arg.argtype) and not is_subclass:
                raise TypeError('argument type invalid: "{}" expected {}, got: {}'.format(
                    name,
                    type(arg.argtype),
                    type(value)
                ))

        # check for includes/excludes
        for name in keyword_values:
            requires = self._named_args[name].requires
            excludes = self._named_args[name].excludes

            if requires:
                missing_required = set(requires) - set(keyword_values.keys())
                if missing_required:
                    missing_required = ', '.join(missing_required)
                    raise ValueError('argument: "{}" requires these args to ALSO be set: {}'.format(
                        name, missing_required))

            if excludes:
                present_excludes = set(excludes) & set(keyword_values.keys())
                if present_excludes:
                    present_excludes = ', '.join(present_excludes)
                    raise ValueError('argument: "{}" requires these args to NOT be set: {}'.format(
                        name, present_excludes))

        # apply defaults
        for name, arg in self._named_args.items():
            if arg.default is not None and keyword_values.get(name) is None:
                keyword_values[name] = arg.default

        # apply functions
        for name in keyword_values:
            arg = self._named_args[name]
            current_value = keyword_values[name]
            # don't compute functions for "null" values
            if current_value is None or arg.fn is None:
                continue

            # call the decorated function
            new_value = arg.fn(self, current_value)
            if arg.argtype is not None:
                if not isinstance(new_value, arg.argtype) and not arg.fn is None:
                    raise TypeError('argument function returned type invalid: "{}" expected {}, got: {}\n'.format(
                        name,
                        type(arg.argtype),
                        type(new_value)
                    ))

            keyword_values[name] = new_value

        for name, value in keyword_values.items():
            arg = self._named_args[name]
            if not arg.choices:
                continue
            if value not in arg.choices:
                raise ValueError('argument "{}" value "{}" invalid, must be one of: ({})'.format(
                    name,
                    value,
                    ', '.join(arg.choices)
                ))

        return keyword_values

    def add(self,
            name,
            position=None,
            argtype=None,
            mandatory=None,
            requires=None,
            excludes=None,
            default=None,
            description=None):
        """
        Add an argument to the parser

        :param name: the name of the argument, any kwarg with this name will be accepted
        :param position: (optional) if you want to support this variable as a positional argument
        :param argtype: (optional) the data type of the argument. `isinstance` will be used to test for this type
        :param mandatory: (optional) indicates is this is a required argument
        :param requires: (optional) a sequence of other fields by `name` that are required if this field is set
        :param excludes: (optional) a sequence of other fields by `name` that cannot be set if this field is defined
        :param default: (optional) defines a default value
        :param description: (optional) the description of the field
        :return:
        """
        if not isinstance(name, str):
            raise TypeError('name must be string, got: {}'.format(type(name)))

        if name in self._named_args:
            raise NameError('argument is already defined: {}'.format(name))

        self._named_args[name] = ArgumentOpts(name=name,
                                              position=position,
                                              argtype=argtype,
                                              mandatory=mandatory,
                                              requires=requires,
                                              excludes=excludes,
                                              default=default,
                                              description=description)

    def parse(self):
        """

        :return: connection arguments keyed by argument value
        :rtype: ConnectArguments
        """
        self._register_args()
        if not self._named_args:
            raise ValueError('the current parser: "{}" has no configured arguments\n'
                             'Any arguments passed to this parser will be ignored.'
                             'You may have forgot to configure a custom parser'.format(
                self.__class__.__name__
            ))
        args_dict = self._parse_args(*self._args, **self._kwargs)
        return ConnectArguments(args_dict)

    def __str__(self):
        pass
        # TODO string representation of arguments
        # textwrap.indent(text, prefix, predicate=None)


class ConnectArguments:
    """
    Stores connection arguments
    """

    def __init__(self, parsed_args: dict):

        if not isinstance(parsed_args, dict):
            raise ValueError('properties must be instanceof `parsed_args`')

        self.__dict__['_parsed'] = parsed_args

    def __getattr__(self, item: str):
        if item not in self.__dict__['_parsed']:
            valid = '\n'.join(self.__dict__['_parsed'].keys())
            raise NameError('invalid argument name: {}, valid arguments: {}'.format(item, valid))
        return self.__dict__['_parsed'][item]

    def __setattr__(self, key, value):
        self.__dict__['_parsed'][key] = value

    def __getitem__(self, key):
        return self.__dict__['_parsed'][key]

    def __setitem__(self, key, value):
        self.__dict__['_parsed'][key] = value

    def get(self, key, default=None):
        return self.__dict__['_parsed'].get(key, default)

    def __iter__(self):
        return iter(self.__dict__['_parsed'])

    def items(self):
        return self.__dict__['_parsed'].items()


class Decorator:
    """
    Contains decorator functions (static methods) for decorating functions to configure
    function arguments
    """

    @staticmethod
    def argument(name=None,
                 position=None,
                 argtype=None,
                 mandatory=False,
                 requires=(),
                 excludes=(),
                 default=None,
                 description=None,
                 choices=None,
                 secret=None):
        """
        Decorate a function to create an argument automatically

        :param name: The name of the argument, (kwarg name) if not provided will be the name of the function being
                     decorated.
        :param position: Position (if positional / required argument), positions start at 0.
                         positional arguments are automatically mandatory
        :param argtype: The python type of the argument, if the given argument is not an instance of this type
                        an exception will be raised.
        :param mandatory: Bool that indicates if the argument is required.
        :param requires: A sequence of other argument names - if this argument is set, this ensures the other arguments
                         mentioned by `requires` are also set.
        :param excludes: A sequence of other argument names - if this argument is set, the named arguments must NOT
                         be set.
        :param default: A default value for the argument, excludes "mandatory".
        :param description: Help text for the argument
        :param choices: Sequence defining the allowable set of options the argument value may be.
        :param secret: Indicates if the argument is sensitive / secret - the argument will be excluded from logging or
                       errors.
        :return: The decorated function (with a ``_argument`` attribute initialized as ``ArgumentOpts``
        """

        def decorator(func):
            if name is None:
                name2 = func.__name__
            else:
                name2 = name

            fn_args = inspect.getfullargspec(func).args
            if len(fn_args) < 2:
                raise ValueError('decorated function [{}] must accept at least 1 argument <value>'.format(str(func)))

            description2 = doc_str(func) if description is None else description
            if not description2:
                description2 = str(func)

            arg = ArgumentOpts(name=name2,
                               position=position,
                               argtype=argtype,
                               mandatory=mandatory,
                               requires=requires,
                               excludes=excludes,
                               default=default,
                               description=description2,
                               choices=choices,
                               secret=secret,
                               fn=func)

            # store the argument inside the function object.
            setattr(func, DECORATOR_ATTRIBUTE, arg)
            return func

        return decorator


class ConnectFunction:
    """
    Contains the logic to correctly implement a `connect` function for your db-api-2.0
    compliant jdbc wrapper.

    Generally implementations only need override __init__ arguments and `get_connection()`
    """
    _driver_path = None
    _driver_class = None
    _connection_class = None
    _cursor_class = None
    _parser = None
    _type_conversion = None
    _runtime_invocation_ok = None

    def __init__(self,
                 driver_path,
                 driver_class,
                 connection_class=JdbcConnection,
                 cursor_class=JdbcCursor,
                 parser=ArgumentParser,
                 type_conversion=JdbcTypeConversion,
                 runtime_invocation_ok=True):

        self.driver_path = driver_path
        self.driver_class = driver_class
        self.connection_class = connection_class
        self.cursor_class = cursor_class
        self.parser = parser
        self.type_conversion = type_conversion
        self.runtime_invocation_ok = runtime_invocation_ok

    @property
    def driver_path(self):
        return self._driver_path

    @driver_path.setter
    def driver_path(self, path: str):
        self._driver_path = path

    @property
    def driver_class(self):
        return self._driver_class

    @driver_class.setter
    def driver_class(self, cls: str):
        """
        The driver class to instantiate JDBC connections with.

        :param cls: a string giving the fully qualified Java class name
        :return:
        """
        self._driver_class = cls

    @property
    def connection_class(self):
        return self._connection_class

    @connection_class.setter
    def connection_class(self, cls):
        if not issubclass(cls, JdbcConnection):
            raise ValueError('"connection_class" must be class of {}'.format(str(JdbcConnection)))
        self._connection_class = cls

    @property
    def cursor_class(self):
        return self._cursor_class

    @cursor_class.setter
    def cursor_class(self, cls):
        if not issubclass(cls, JdbcCursor):
            raise ValueError('"connection_class" must be class of {}'.format(str(JdbcCursor)))
        self._cursor_class = cls

    @property
    def runtime_invocation_ok(self):
        return self._cursor_class

    @runtime_invocation_ok.setter
    def runtime_invocation_ok(self, is_ok):
        """
        Indicates if it is acceptable it invoke the JDBC Driver directly from the Jar file AFTER
        JVM startup.

        :param is_ok: True/False
        :type is_ok: bool
        :return:
        """
        if not isinstance(is_ok, bool):
            raise ValueError('"isok" must be bool, got: {}'.format(type(is_ok)))
        self._runtime_invocation_ok = is_ok

    @property
    def parser(self):
        """
        class object used to parse *args, and **kwargs passed into connect

        :return: parser class object
        :rtype: ArgumentParser
        """
        return self._parser

    @parser.setter
    def parser(self, parser: ArgumentParser):
        self._parser = parser

    @property
    def type_conversion(self):
        return self._type_conversion

    @type_conversion.setter
    def type_conversion(self, cls):
        if not issubclass(cls, JdbcTypeConversion):
            raise ValueError('"type_conversion" must be class of {}'.format(str(JdbcTypeConversion)))
        self._type_conversion = cls

    def get_connection(self, driver_class: JClass, arguments: ConnectArguments):
        """
        Creates and returns the python JdbcConnection object wrapper instance

        :return: JdbcConnection instance
        :rtype: JdbcConnection
        """
        raise NotImplementedError('this method must be defined by the implementation')

    def handle_args(self, arguments: ConnectArguments):
        """
        Optional method that can be implemented by subclasses

        Called immediately after arguments are successfully parsed, before
        any additional operations are performed.

        Useful for setting driver options from argument inputs.
        For example if you need to set the `driver_path` or `driver_class` at
        runtime perhaps based upon user configuration.
        :param arguments:
        :return:
        """
        pass

    def parse_args(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :rtype: ConnectArguments
        :return: returns an instance of ConnectArguments containing
        """
        ParserClass = self.parser
        parser = ParserClass(*args, **kwargs)
        args = parser.parse()
        self.handle_args(args)
        return args

    def load_driver(self):
        """
        Loads the driver defined via `driver_path` and `driver_class`

        if self.
        Classpath.load_jar_class('sqlite-jdbc-3.21.0.jar', 'org.sqlite.SQLiteConnection')
        :return: Java driver class object via JPype
        :rtype: JConnection
        """
        if self.driver_path and not (isfile(self.driver_path) or isdir(self.driver_path)):
            raise ValueError('"driver_path" is not a valid jar file or directory {}\n'
                             '"driver_path" can be set to `None` if the classpath is configured by'
                             'the user'.format(self.driver_path))

        if self.driver_class is None or not str(self.driver_class).strip():
            raise ValueError('"driver_class" must be set on {}'.format(self.__class__))

        if Jvm.is_running():
            # if the jvm is already running, it's possible the class we need has already been loaded before
            # or is already on the classpath
            try:
                driver = JClass(self.driver_class)
            except JClass('java.lang.ClassNotFoundException'):
                pass
            else:
                return driver

        # if the jvm has NOT been started, add the driver jar to the classpath explicitly
        # and try to load the class
        # this is designed this way to ensure the only time we use the users provided classpath
        # is when driver_path has NOT been given.
        if not Jvm.is_running() and not self.driver_path:
            Jvm.start()
            # try to load the driver normally, this assumes the classpath has been set for us by the user
            try:
                driver = JClass(self.driver_class)
            except JClass('java.lang.ClassNotFoundException'):
                raise DriverNotFoundError('The driver class "{}" could not be found on '
                                          'the classpath\n'
                                          'Note: no jar-path was passed to pyjdbc via '
                                          '"driver_path"\n'
                                          'Classpath:\n'
                                          '{}'.format(self.driver_class, Classpath.get()))
            else:
                return driver

        if not Jvm.is_running():
            # according to jpype a jar file can be added to the classpath directly:
            #   https://jpype.readthedocs.io/en/latest/quickguide.html#starting-jpype
            Classpath.add(self.driver_path)
            Jvm.start()

            try:
                driver = JClass(self.driver_class)
            except (TypeError, JClass('java.lang.ClassNotFoundException')):
                raise DriverNotFoundError('The driver class "{}" could not be found on the classpath.\n'
                                          '"driver_path" = {}\n'
                                          'Classpath:\n'
                                          '{}'.format(self.driver_class, self.driver_path, Classpath.get())) from None
            else:
                return driver

        else:
            if not self.runtime_invocation_ok:
                raise RuntimeError('Error in driver {} - runtime invocation of the pyjdbc based driver: "{}" '
                                   'is not allowed.\n'
                                   'you may need to set the classpath via the java.Classpath method before invoking'
                                   'the driver!'.format(self.driver_class, self.__class__.__name__)) from None

            try:
                driver = Classpath.load_jar_class(self.driver_path, self.driver_class)
            except (TypeError, JClass('java.lang.ClassNotFoundException')) as e:
                raise DriverNotFoundError('unable to load jdbc driver class: "{}" from {}'.format(
                    self.driver_class, self.driver_path
                )) from e
            return driver

    def connect(self, *args, **kwargs):
        """
        Loads driver class and retrieves connection

        Lower level interface than ``get_connection()``

        Driver implementations may override this method if they wish to override driver loading semantics

        :param args: driver specific positional arguments
        :param kwargs: driver specific keyword arguments
        :return: JdbcConnection instance or subclass of
        :rtype: JdbcConnection
        """
        arguments = self.parse_args(*args, **kwargs)
        driver_class = self.load_driver()
        return self.get_connection(driver_class, arguments)

    def __call__(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return: Python JdbcConnection instance
        """
        return self.connect(*args, **kwargs)