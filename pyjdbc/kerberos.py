"""
Utilities to interact with Kerberos

Hadoop features a class to simulate the functionality of "kinit" -

There are a few ways Java can figure out where your KDC is:
    - look for a krb5.conf or krb5.ini in the default locations
    - be told where krb5.conf is via environment variable
    - be told where krb5.conf is via jvm argument
        "java.security.krb5.conf=krb5.conf"
    - be told where the kdc is directly via jvm argument
        "java.security.krb5.kdc=myhost.com"
        "java.security.krb5.realm=EXAMPLE.COM

    we can then accept an argument for:
        krb5_conf
        OR
        krb5_kdc and krb5_realm

    if neither value is provided we can search in the default locations for krb5_conf
    if no value can be found

The ticket cache location can be defined by passing it to Java as well:

# krb5_get_init_creds_password(k5ctx, &cred, k5princ, password, NULL, NULL, 0, NULL, NULL);

Jaas configuration:
    java.security.auth.login.config


chain of classes:
    https://github.com/apache/hbase/blob/master/hbase-common/src/main/java/org/apache/hadoop/hbase/security/User.java
    >
    interacts with org.apache.hadoop.security.SecurityUtil;
    https://github.com/hanborq/hadoop/blob/master/src/core/org/apache/hadoop/security/SecurityUtil.java

    see: https://github.com/apache/hadoop/blob/a89ca56a1b0eb949f56e7c6c5c25fdf87914a02f/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/TestUGILoginFromKeytab.java
"""

__all__ = ['kerberos_login_keytab', 'configure_jaas', 'realm_from_principal']

import logging
from os.path import join
import tempfile
from jpype import JClass
import binascii

from pyjdbc.java import Jvm, System

log = logging.getLogger(__name__)

USER_GROUP_CLASS = 'org.apache.hadoop.security.UserGroupInformation'
KERBEROS_SECURITY = 'kerberos'


def kerberos_login_keytab(principal, keytab):
    Configuration = JClass('org.apache.hadoop.conf.Configuration')
    CommonConfigurationKeys = JClass('org.apache.hadoop.fs.CommonConfigurationKeys')

    # This setting below is required. If not enabled, UserGroupInformation will abort
    #  any attempt to `loginUserFromKeytab`.
    hadoop_conf = Configuration()
    hadoop_conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, KERBEROS_SECURITY)

    try:
        UserGroupInformation = JClass(USER_GROUP_CLASS)
    except Exception:
        raise ModuleNotFoundError('"{}" was not found on the classpath, '
                                  'ensure "hadoop-common.jar" is on the classpath'.format(USER_GROUP_CLASS))

    UserGroupInformation.loginUserFromKeytab(principal, keytab)


def find_krb5_conf():
    # see: https://github.com/frohoff/jdk8u-dev-jdk/blob/da0da73ab82ed714dc5be94acd2f0d00fbdfe2e9/src/share/classes/sun/security/krb5/Config.java
    pass


def realm_from_principal(principal):
    """
    Attempt to retrieve a realm name from a principal, if the principal is fully qualified.

    :param principal: A principal name: user@DOMAIN.COM
    :type: principal: str
    :return: realm if present, else None
    :rtype: str
    """
    if '@' not in principal:
        return
    else:
        parts = principal.split('@')
        if len(parts) < 2:
            return
        return parts[-1]


def configure_jaas(jaas_path=None,
                   use_password=None,  # uses javax.security.auth.login.password, javax.security.auth.login.name
                   principal=None,
                   keytab=None,
                   no_prompt=False,
                   ticket_cache=None,
                   use_ticket_cache=False):
    """
    Configures and sets a jaas configuration at runtime

    The configuration file is saved to <temp>

    :param jaas_path: file path where the jaas file will be written
    :param use_password: use the password defined in javax.security.auth.login.password
    :param principal: set the user principal to use in jaas
    :param keytab: set the user keytab to use in jaas
    :param no_prompt: disable prompting for user/password
    :param ticket_cache: use the default ticket cache
    :return:
    """
    lines = []
    lines.append('com.sun.security.auth.module.Krb5LoginModule required')
    if keytab:
        lines.append('keyTab="{}"'.format(keytab))
    if principal:
        lines.append('principal="{}"'.format(principal))
    if ticket_cache:
        lines.append('ticketCache={}'.format(ticket_cache))
    lines.append('useKeyTab={}'.format(str(bool(keytab)).lower()))
    lines.append('useTicketCache={}'.format(str(bool(use_ticket_cache)).lower()))
    lines.append('storeKey={}'.format(str(bool(keytab or not no_prompt)).lower()))
    lines.append('doNotPrompt={}'.format(str(bool(no_prompt)).lower()))
    lines.append('storePass={}'.format(str(bool(use_password)).lower()))
    lines.append('useFirstPass={}'.format(str(bool(use_password)).lower()))
    lines.append('tryFirstPass=false')

    lines = ['  ' + l for l in lines]
    lines[-1] += ';'

    lines.insert(0, 'com.sun.security.jgss.krb5.initiate {')
    lines.append('};')

    jaas_text = '\n'.join(lines)
    tempdir = tempfile.gettempdir()
    if not jaas_path:
        jaas_binary = jaas_text.encode('utf8', 'ignore')
        jaas_path = join(tempdir, 'pyjdbc-jaas-{}.conf'.format(binascii.crc32(jaas_binary)))

    log.debug('jaas configuration: {}\n{}'.format(jaas_path, jaas_text))

    with open(jaas_path, 'w') as fp:
        fp.writelines(lines)

    if not Jvm.is_running():
        Jvm.add_argument('java.security.auth.login.config', '-Djava.security.auth.login.config={}'.format(jaas_path))
    else:
        System.set_property('java.security.auth.login.config', jaas_path)