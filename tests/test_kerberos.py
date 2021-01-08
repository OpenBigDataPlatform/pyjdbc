"""
Tests for `kerberos` module in pyjdbc
"""
import unittest
import os
from os.path import dirname, samefile, isfile, expanduser, join
from stat import S_IREAD
import tempfile
import pytest

from pyjdbc.kerberos import configure_jaas


class TestJaas(unittest.TestCase):

    def test_jaas_defaults(self):
        # the default jaas configuration (no arguments) prevents java from prompting for credentials
        # when kerberos is being used
        jaas_path = configure_jaas()
        open(jaas_path)
        os.unlink(jaas_path)

        jaas_dir = dirname(jaas_path)
        temp_dir = tempfile.gettempdir()
        self.assertTrue(samefile(jaas_dir, temp_dir),
                        'jaas configuration should be written to `tempfile.gettempdir()` in this test')

    def test_jaas_read_only(self):
        """
        If the jaas config exists in the default temp location, and is read only, but identical
        it should be used without being overwritten or receiving an error.
        Because the file name is based on
        """
        jaas_conf1 = configure_jaas()
        self.assertTrue(isfile(jaas_conf1), 'jaas configuration is not a file')
        os.chmod(jaas_conf1, S_IREAD)
        try:
            jaas_conf2 = configure_jaas()
        except PermissionError as e:
            raise RuntimeError('jaas configuration - failed to reuse existing read-only file: {}'.format(e)) from e

        self.assertTrue(samefile(jaas_conf1, jaas_conf2),
                        'jaas configuration - failed to reuse existing read-only file, {} != {}'.format(
                            jaas_conf1, jaas_conf2
                        ))

    def test_jaas_manual_path(self):
        with tempfile.NamedTemporaryFile() as tmpfile:
            custom_path = tmpfile.name
            jaas_path = configure_jaas(jaas_path=custom_path)
            self.assertTrue(samefile(custom_path, jaas_path),
                            'jaas configuration - failed to use explicit custom path, {} != {}'.format(
                                custom_path,
                                jaas_path
                            ))