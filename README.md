# PyJdbc
`pyjdbc` provides a `db api 2.0` compliant interface around any `JDBC` driver.

`pyjdbc` is used by various other projects as the basis for their jdbc compatibility interface.

## Features
- wrap jdbc `DriverManager` functionality
- custom exception handlers to deliver better python-like exceptions and avoiding `java`
  stack traces whenever possible.
- provide basic `Cursor`, `DictoCursor`, `Connection` classes
- provide basic `TypeConversion` classes for common `jdbc` to `python` cases.
- interface for handling and validating `jdbc` arguments and formatting connection strings
- registry to support downloading and managing drivers locally.