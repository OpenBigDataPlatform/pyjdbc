import jpype
from jpype import JClass

# Enable Java imports
from jpype import JArray


def get_url(path: str):
    """
    Formats a given path into a url object.

    Converts `/home/myfile.txt` into `file:/home/myfile.txt`

    :param path:
    :return:
    """
    Paths = JClass('java.nio.file.Paths')
    url = Paths.get(path).toUri().toURL()  # todo add exception handling if the path isnt valid
    return url


class System:
    """
    Wrapper around java.lang.System
    """

    @staticmethod
    def _system():
        return JClass('java.lang.System')

    @classmethod
    def get_property(cls, name):
        return cls._system().getProperty(name)

    @classmethod
    def set_property(cls, name, value):
        cls._system().setProperty(name, value)

    @classmethod
    def clear_property(cls, name):
        return cls._system().clearProperty(name)

    @classmethod
    def get_env(cls, name):
        return cls._system().getenv(name)


class Properties:
    PROPERTIES_CLASS = 'java.util.Properties'

    @classmethod
    def from_dict(cls, dictionary):
        JProperties = JClass(cls.PROPERTIES_CLASS)
        jprops = JProperties()
        for k, v in dictionary.items():
            if not isinstance(k, str):
                raise ValueError('[{}] keys must be strings, got: {}'.format(cls.PROPERTIES_CLASS, type(k)))
            jprops.setProperty(k, v)

        return jprops

    @classmethod
    def from_tuple(cls, sequence):
        as_dict = dict(sequence)
        return cls.from_dict(as_dict)

    @staticmethod
    def to_dict(properties):
        keys = properties.stringPropertyNames()
        dictionary = {key: properties.getProperty(key) for key in keys}
        return dictionary

    @staticmethod
    def to_tuple(properties):
        keys = properties.stringPropertyNames()
        sequence = [(key, properties.getProperty(key)) for key in keys]
        return sequence


class Classpath:
    """
    utilities for dealing with the java classpath
    """
    @staticmethod
    def add(*paths):
        for path in paths:
            jpype.addClassPath(path)

    @staticmethod
    def get():
        return jpype.getClassPath()

    @staticmethod
    def load_jar_class(jar_path, class_name):
        """
        Load a class at runtime directly from a jar file.
        Note that with some libraries this can cause problems because the library
        will not be *visible* to the default class loader.

        :param jar_path: Path to the `.jar` file.
        :param class_name: The fully qualified Class Name within the jar to load.
        :return:
        """
        URL = JClass('java.net.URL')
        URLClassLoader = JClass('java.net.URLClassLoader')
        Class = JClass('java.lang.Class')
        UrlArray = JArray(URL)
        urls = UrlArray(1)
        urls[0] = get_url(jar_path)
        java_class = JClass(Class.forName(class_name, True, URLClassLoader(urls)))
        return java_class


class Jvm:
    ARGS = {}

    @classmethod
    def add_argument(cls, identifier, option):
        """
        Add an argument to the jvm, (this must be used BEFORE the jvm is started)

        If the jvm is already running RuntimeError will be raised

        Set a jvm argument, examples include:
            -Xmx1099m
            -Djava.class.path=PATH_TO_JAR

        :param identifier: a string identifier so Jvm options aren't duplicated: ie: ``javax.net.ssl.trustStore``
        :type identifier: str
        :param option: the full jvm option argument, ie: ``-Djavax.net.ssl.trustStore=trust.jks``
        :type option: str
        :raises: RuntimeError
        :return:
        """
        if cls.is_running():
            raise RuntimeError('The JVM has been started, any options set after the JVM is started will have no '
                               'effect.\n'
                               'Set jvm options before acquiring any connections with `pyjdbc`')

        if not any((option.startswith('-D'), option.startswith('-X'))):
            raise ValueError('invalid argument "option": {}, jvm arguments must start with "-D" or "-X" \n'
                             'Examples:\n'
                             '    -Xmx1099m\n',
                             '    -Djavax.net.ssl.trustStore=trust.jks'.format(option))

        cls.ARGS[identifier] = option

    @classmethod
    def start(cls):
        # start the jvm
        jpype.startJVM(*cls.ARGS.values(), interrupt=True)

    @staticmethod
    def stop():
        jpype.shutdownJVM()

    @staticmethod
    def is_running():
        return jpype.isJVMStarted()

    @classmethod
    def check_running(cls):
        if cls.is_running():
            raise RuntimeError('The jvm is already running')

    @staticmethod
    def path():
        return jpype.getDefaultJVMPath()

    @staticmethod
    def version():
        return jpype.getJVMVersion()