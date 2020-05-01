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

## Version 1.3.8
  - dbrequests.mysql module:
    - see #11 for motivation on moving dialect specific implementation into extras
    - see #15 for mysql specific send_data via load data local infile

## Version 1.3.9
  - dbrequests.mysql:
    - see #20 for memory efficient send_data using datatable

## Version 1.3.10
  - dbrequests.mysql:
    - see #24 for bugfix while writing to csv

## Version 1.3.11
  - dbrequests.mysql:
    - see #28 for bugfix when sending escape sequences

## Version 1.3.12
  - dbrequests.mysql:
    - see #22 for performance improvements for send_query when reading large datasets
    - support for pymysql and mysqldb

## Version 1.3.13
  - dbrequests.mysql
    - see #30: fixes handling of None/NULL values in columns

## Version 1.3.14
  - dbrequests.mysql:
    - see #32 for bugfix in send_query for empty result sets
