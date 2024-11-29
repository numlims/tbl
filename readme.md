# tbl: for sql tables

python wrapper for sql tables.

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

## setup

for now, throw the files from [dbcq](https://github.com/numlims/dbcq)
into the same directive, set up dbcq as described, then in code say:

```
t = tbl("<target in db.ini>")
print(t.tables())
```

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