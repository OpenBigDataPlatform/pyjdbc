from setuptools import setup

setup(
    name='pyjdbc',
    version='0.1.0',
    author='OpenBigDataProject',
    license='Apache-2.0',
    url='https://github.com/OpenBigDataPlatform/pyjdbc',
    description=('Use JDBC drivers to provide DB API 2.0 python database interface'),
    long_description=open('README.md').read(),
    keywords=('dbapi jdbc'),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Java',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Java Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],

    packages=['pyjdbc'],
    install_requires=[
        'JPype1>=1.0.1',
    ],
)
