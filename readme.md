# tbl: for sql tables

python wrapper for sql tables.

```
t = tbl("<target in .dbc>")
print(t.tables())
```

documentation [here](https://numlims.github.io/tbl/).

## cli

list the tables in a db:

```
tbl <db> tables
```

get a human readable summary of a table and its columns:

```
tbl <db> table <table name>
```

## install

download tbl whl from
[here](https://github.com/numlims/tbl/releases). install whl with
pip:

```
pip install tbl-<version>.whl
```

see [dbcq](https://github.com/numlims/dbcq?tab=readme-ov-file#db-connection) for database connection setup.

## functions:

fields<br/>
fieldtypes<br/>
deletefrom<br/>
fk<br/>
fkfromt<br/>
fkfromtf<br/>
fktot<br/>
fktotf<br/>
identities<br/>
insert<br/>
update<br/>
pk<br/>
tables<br/>

supports mssql at the moment. fk, fields and tables also support sqlite.

## cli

use from the command line like:

```
tbl <db target> <subcommand>
```

with subcommands:

```
  fk           all foreign keys as json
  tables       all tables as json
  fields <tablename>   fields of a table as json
  table <tablename>    human readable table summary
```

# dev

get [codetext](https://github.com/tnustrings/codetext) for assembling init.ct.

build and install:

```
make install
```