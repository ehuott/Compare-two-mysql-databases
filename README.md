Compare-two-mysql-databases
===========================

Perl script to compare two mysql databases

Requires

1. Perl:

- strict - Perl pragma to restrict unsafe constructs
- warnings - Perl pragma to control optional warnings
- English - use nice English (or awk) names for ugly punctuation variables
- DBI - the standard database interface module for Perl
- Date::Simple - a simple date object
- List::MoreUtils - Provide the stuff missing in List::Util
- List::Compare - Compare elements of two or more lists

2. Database

You need a user with minimum SELECT priviles to both databases.



Example output format

```

∴ perl compare_two_databases.pl
######################################
Table Schema verification: START

TABLE NAME                                        |Server 01 (db1)                                   |Server 02 (db2)                                   
------------------------------------------------------------------------------------------------------------------------------------------------------
t2                                                |BASE TABLE, InnoDB, Compact, utf8_general_ci      |DOESN'T EXIST                                     


Table Schema verification: DONE

######################################
Columns Schema verification: START
TABLE NAME                    |COLUMN NAME                   |Server 01                                                   |Server 02                                                   
t1                            |c3                            |Def: timestamp, Default: , IsNull: YES                      |Def: timestamp, Default: CURRENT_TIMESTAMP, IsNull: NO      


Columns Schema verification: DONE

######################################
Data value verification: START

#####################################################
TABLE NAME: t1 
INFO: Detected a difference in the number of rows in the table: t1: Server 01: 4 rows, Server 02: 5 rows
------------------------------------------------------
Element which exists in server01 but not in server02: 
Column name: c1, values: 4
------------------------------------------------------
Element which exists in server02 but not in server01: 
Column name: c1, values: 4,5
------------------------------------------------------
Rows which appear at least once in either the first or the second list, but not both: 
Column name: c1, values: '4','5'
GENERATE CSV file with differences. Files: db1_s01_for_t1.csv and db2_s02_for_t1.csv
Data value verification: DONE

```

Two new files are generated with differences:

```

∴ ll
drwxr-xr-x 3 ela ela  4096 2014-01-10 11:07 ./
drwxr-xr-x 3 ela ela  4096 2014-01-08 13:28 ../
-rw-r--r-- 1 ela ela 13533 2014-01-10 11:04 compare_two_databases.pl
-rw-r--r-- 1 ela ela    33 2014-01-10 11:06 db1_s01_for_t1.csv
-rw-r--r-- 1 ela ela    57 2014-01-10 11:06 db2_s02_for_t1.csv

```

