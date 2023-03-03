## This directory is used to store all snowflake database tables related folders in curated db.

## Version pattern-

## Vx.a.b.c__<<schema_name>>.<<table_name>>.sql

# Vx - Version number - This will be changed only when the project changes.
# a - Table number - This integer represents the table number which is unique per table.
# b - File version number - This will change after every 20th subversion number. This is used to track the frequency of the file changes.
# c - File sub-version - This will change for every change in the file. It's range will be 1 to 20.