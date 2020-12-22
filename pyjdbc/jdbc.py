"""
JDBC functionality, helpers, wrappers are contained in this module
"""

import logging
from pyjdbc.java import Properties
from jpype import JClass


class DriverManager:
    """
    A wrapper around java.sql.DriverManager that supports the different argument styles for
    ``DriverManager.getConnection()``

    Examples of the ``url`` argument for various jdbc drivers.

    Hive connection string:
        jdbc:hive2://<host>:<port>/<dbName>;transportMode=http;httpPath=<http_endpoint>;<otherSessionConfs>?<hiveConfs>#<hiveVars>

    Phoenix connection string:
        jdbcphoenix:thin:url={URL};authentication=SPNEGO;principal={PRINCIPAL};keytab={KEYTAB};serialization=PROTOBUF

    Postgres connection string:
        jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true

    Sqlite connection string:
        jdbc:sqlite:sample.db
        jdbc:sqlite::memory:
    """

    DRIVER_MANAGER_CLASS = 'java.sql.DriverManager'

    def __init__(self, url, properties=None, username=None, password=None):
        """
        The driver you intend to load must be on the classpath prior to calling this class.

        :param url: The jdbc connection url, many jdbc drivers embed connection arguemnts and credentials within the
                    url. For this reason this class will never show/expose the url in logs or exceptions
        :type url: str
        :param properties: (optional) a dictionary of properties, will be converted to a java properties object
                           automatically, some jdbc drivers do not use this argument.
        :type properties: dict
        :param username: (optional) the jdbc username - some jdbc drivers do not use this argument
        :type username: str
        :param password: (optional) the jdbc password - some jdbc drivers do not use this argument
        :type password: str
        """
        self._log = logging.getLogger(self.__class__.__name__)

        if not isinstance(url, str):
            raise ValueError('url must be `str`, got: {}'.format(type(url)))

        if not url.strip():
            raise ValueError('url must be non-empty string!')

        creds = (username, password)
        if any(creds) and not all(creds):
            raise ValueError('Both username and password must be set if used'
                             'username-present: {}'
                             'password-present: {}'.format(bool(self._username), bool(self._password)))

        if any(creds) and self._properties:
            raise ValueError('Unfortunately there is no signature for JDBC drivers that supports [username/password]'
                             'and [properties]. Usually the jdbc driver requires you to embed the credentials'
                             'within the url under these circumstances')
        self._url = url
        self._properties = properties
        self._username = username
        self._password = password

    def get_connection(self):
        JDriverManager = JClass(self.DRIVER_MANAGER_CLASS)

        creds = (self._username, self._password)
        if self._properties:
            self._log.debug('using signature: DriverManager.getConnection(String url, Properties info)')

            # convert property dictionary into java Properties object
            properties = Properties.from_dict(self._properties)
            java_connection = JDriverManager.getConnection(self._url, properties)

        # Signature 2
        elif self._url and creds:
            self._log.debug('using signature: DriverManager.getConnection(String url, String user, String password)')
            java_connection = JDriverManager.getConnection(self._url, self._username, self._password)

        # Signature 3
        else:
            self._log.debug('using signature: DriverManager.getConnection(String url)')
            java_connection = JDriverManager.getConnection(self._url)

        return java_connection