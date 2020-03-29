## Version 1.0
  - implemented send_data and send_query wrappers for pandas sql-functionality

## Version 1.0.8
  - renamed to dbrequests

## Version 1.1
  - added handling of percentage signs
  - added possibility of automated comment removal
  - ensured the compatibility of raw SQL code with code sent via dbrequests

## Version 1.2
  -  changed static loading of the connection class to a flexible model, allowing to override connections for different SQL drivers

## Version 1.3
  - added mysql backend for specific mysql and mariadb support
  - added possibility for send_data to use infile for mysql databases
