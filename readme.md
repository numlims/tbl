# tbl: for sql tables

python wrapper for sql tables.

```
t = tbl("<target in .dbc>")
print(t.tables())
```

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

## setup

needs [dbcq](https://github.com/numlims/dbcq) for database connection.

