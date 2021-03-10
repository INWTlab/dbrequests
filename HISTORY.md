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

## Version 1.3.15
  - dbrequests:
    - see #27 for bugfix in Database class when specifiying a port in a
      credentials object.
    - the argument 'creds' in the init method of a database class is now
      deprecated
    - the argument 'db_url' can now handle str and dict type; str is a
      sqlalchemy url; a dict a credentials object
    - credential objects can now have additional fields which will be used as
      elements in connect_args for sqlalchemies create_engine: see #12
  - dbrequests.mysql
    - see #36 for bugfix while sending an empty frame

## Version 1.3.16
  - dbrequests.mysql
    - see #35: New send_data modes: update_diffs, insert_diffs, replace_diffs

## Version 1.3.17
  - dbrequests.mysql
    - new function in database: send_delete for deleting rows in a database.
    - send_data in mode 'update' now allows to send only partial subset of
      columns.

## Version 1.4.0
  - dbrequests.mysql
    - bugfix in send_data with mode [update|insert|replace]_diffs: same as #36

## Version 1.4.1
  - dbrequests.mysql
    - bugfix for upstream bug in mariadb: sending diffs needs persisten tables
      instead of temporary.

## Version 1.4.2
  - dbrequests.mysql
    - creating temorary removing partitions and system versioned from temporary
      tables.
    - new mode for send data: sync_diffs: update differences and delete
      deprecated rows.
    - new mode for delete data: in_delete_col: mark rows to delete, then delete.
    - bugfix for temporary tables: now properly removes tables.

## Version 1.4.3-5
  - dbrequests.mysql
    - More stable and reliable Version of sync_diffs mode for sending data.
      Respects scarce resources on the mysql server.

## Version 1.4.6
  - dbrequests.mysql
    - HOTFIX #51: Bug with latest datatable version

## Version 1.4.7
  - dbrequests.mysql
    - bugfix of #52

## Version 1.4.8
  - dbrequests.mysql
    - fix for creating temporary files on Windows.

