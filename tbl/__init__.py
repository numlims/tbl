"""
tbl gives functions for sql table handling and inspection
table and field names are lower cased by default
see method comments for short description

# columns
# columntypes
# deletefrom
# fk
# fkfromt
# fkfromtc
# fktot
# fktotc
# identities
# insert
# update
# pk
# tables
# tablesummary

"""

from dbcq import dbcq
from tbl._tblhelp import *
import json
import sys
import jsonpickle 

# fk holds a foreign key
class fk:
    def __init__(self, ft, fc, tt, tc):
        "init a foreign key. ft: from table, fc: from column, tt: to table, tc: to column."

        self.ft = ft
        self.fc = fc
        self.tt = tt
        self.tc = tc

# tbl gives db table information
class tbl:

    def __init__(self, target):
        """
        init connects to the database target in db.ini
        todo maybe pass flag lower=False
        """
        
        self.db = dbcq(target)
        self.th = tblhelp(self.db)

    def columns(self, table=None):
        """
        columns returns the column names of table as an array of strings,
        if no table given, return dict by table with fields for every table 
        """
        return self.th.columns(table)

    def columntypes(self, tablename):
        """
        columntypes gives the column schema for tablename
        """

        return self.th.columntypes(tablename)

    def deletefrom(self, table, row):
        """
        deletefrom deletes the row in table identified by the post parameters
        if there are two rows with exactly the same values, it deletes both 
        """
        args = []
        q = "delete from [" + table + "] where " + self.th.wherestring(row, args)
        # do the deleting
        self.db.query(q, args) # todo uncomment

    def identities(self, table):
        """
        identities gives list of identity (auto-increment) columns for table
        """
        
        identities = self.th.identity_keys(table)
        return identities

    def insert(self, table, row):
        """
        insert inserts a line in a table
        """
        data = withoutidentity(table, row) # assume primary key is generated
        types = columntypes(table) # for parsing
        placeholders = [] # for query
        for key in data:
            # parse strings to numbers
            if types[key] in ["int", "bigint", "smallint", "tinyint", "bit", "decimal", "numeric", "money", "smallmoney"]:
                data[key] = int(data[key])
            elif types[key] in ["real", "float"]:
                data[key] = float(data[key])

            # a placeholder for the query
            placeholders.append("?")
    
        q = "insert into [" + table + "] ([" + "], [".join(list(data)) + "]) values (" + ", ".join(placeholders) + ")"

        # do the insertion
        self.db.query(q, *data.values()) 

    def fk(self):
        """
        fk gives all foreign keys as array of fks
        """

        # if myssql
        """ 
        from https://stackoverflow.com/a/201678

        SELECT
          TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME
          FROM
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
              REFERENCED_TABLE_SCHEMA = (SELECT DATABASE())
            """
        # return foreign keys for sqlite
        if self.th._issqlite():
            # from https://stackoverflow.com/a/59171912
            query = """
            SELECT master.name, pragma.*
            FROM sqlite_master master
            JOIN pragma_foreign_key_list(master.name) pragma ON master.name != pragma."table"
            WHERE master.type = 'table'
            ORDER BY master.name;
            """
            res = self.db.qfad(query)
            fks = []
            for row in res:
                fks.append(fk(row["name"].lower(), row["from"].lower(), row["table"].lower(), row["to"].lower())) # is lower() here a good idea?
            return fks

        # return foreign keys for mssql
        elif self.th._ismssql():
            # query from https://stackoverflow.com/a/17501870
            query = """
            SELECT 
            OBJECT_NAME(fk.parent_object_id) ft,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) fc,
            OBJECT_NAME(fk.referenced_object_id) tt,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) tc
            FROM 
            sys.foreign_keys AS fk
            INNER JOIN 
            sys.foreign_key_columns AS fkc 
            ON fk.OBJECT_ID = fkc.constraint_object_id
            INNER JOIN 
            sys.tables t 
            ON t.OBJECT_ID = fkc.referenced_object_id"""

            # return self.db.qfad(query) # todo return array of keys? let key have fields ft fc tt tc and fromtable fromcolumn totable tocolumn?
            rows = self.db.qfad(query) # todo return array of keys? let key have fields ft fc tt tc and fromtable fromcolumn totable tocolumn?
            # output foreign as objects
            a = []
            for row in rows:
                a.append(fk(row["ft"].lower(), row["fc"].lower(), row["tt"].lower(), row["tc"].lower())) # is lower() here a good idea?
                #a.append(fk(row["ft"], row["fc"], row["tt"], row["tc"])) 
                # a.append(row)
            return a
        else:
            print(f"fk not supported for {self.th._type()}")

    def fkfromt(self, fka=None):
        "fkfromt returns foreign keys by from-table dict key for every table in db"

        if fka == None:
            fka = self.fk()
        out = {}
        # return complete list of tables in db
        for t in self.tables():
            out[t] = []
        for key in fka:
            out[key.ft].append(key)
        return out

    def fkfromtc(self, fka=None):
        """fkfromtc returns foreign keys by from-table and from-column
        dict key for every table in db"""

        if fka == None:
            fka = self.fk()
        out = {}
        # return complete list of tables in db
        for t in self.tables():
            out[t] = {}
        for key in fka:
            # is there an entry for the key?
            if not key.fc in out[key.ft]:
                out[key.ft][key.fc] = []
            out[key.ft][key.fc].append(key)
        return out

    def fktot(self, fka=None):
        """fktot returns foreign keys by to-table
        dict key for every table in db"""

        if fka == None:
            fka = self.fk()
        out = {}
        # return complete list of tables in db
        for t in self.tables():
            out[t] = []
        for key in fka:
            out[key.tt].append(key)
        return out

    def fktotc(self, fka=None):
        """fktotc returns foreign keys by to-table and to-column
        dict key for every table in db"""

        if fka == None:
            fka = self.fk()
        out = {}
        # return complete list of tables in db
        for t in self.tables():
            out[t] = {}
        for key in fka:
            if not key.tc in out[key.tt]:
                out[key.tt][key.tc] = []
            out[key.tt][key.tc].append(key)
        return out

    def pk(self):
        """pk gives primary keys as list for each table"""

        return self.th.primary_keys()

    def tables(self):
        """tables gives the names of the tables in the db"""

        return self.th.tables()

    def update(self, table, fromrow, torow):
        """update updates data in a table row
    table: table name
    fromrow: dict used to select row
    torow: dict with new values
    dq returns dicts keyed by <tablename>.<fieldname>. to make edits on these dicts updatable without renaming the keys, this function also accept these dicts, if all keys in fromrow or torow are <tablename>.<fieldname>
        """
        fromdata = withidentity(table, fromrow)
        todata = withoutidentity(table, torow)

        # remove table names from dict keys if they are there
        fromdata = _rmtablename(table, fromdata)
        todata = _rmtablename(table, todata)

        # todata.pop(pk) # don't update the primary key
        updatepairs = []
        args = [] # successively fill the query args array
        # update pairs
        for key in todata:
            # build sql query
            updatepairs.append("[" + key + "] = ?")
            # the args
            args.append(todata[key])

        ws = self.th.wherestring(fromdata, args) # fill args along the way
        q = "update [" + table + "] set " + ", ".join(updatepairs) + " where " + ws

        # do the update
        self.db.query(q, args)

    def tablesummary(self, table:str):
        """tablesummary returns a human readable summary of table with columns and outgoing and incoming foreign keys."""
        
        # taken from ~/tbl-cxx/bytable.py

        out = ""

        # get fks
        fks = self.fk()
        fkfromtc = self.fkfromtc(fks)
        fktotc = self.fktotc(fks)

        # columns by table
        columns = self.columns()

        # print(table)
        out += table.upper() + "  "
        # print fields that are not fk
        if table in columns:
            columns[table].sort()
        for column in columns[table]:
            if not (table in fkfromtc and column in fkfromtc[table]):
                out += column.lower() + " "
        out += "\n"
        out += "\n"

        # outgoing foreign keys from this table
        if table in fkfromtc:
            cols = list(fkfromtc[table].keys())
            cols.sort()
            # print("from")
            for column in cols:
                fk = fkfromtc[table][column][0]
                out += "  " + fk.fc.lower() + "  " + fk.tt.lower() + "." + fk.tc.lower() + "\n"
            out += "\n"

        # incoming foreign keys to this table
        if table in fktotc and len(fktotc[table]) > 0:
            # sort
            a = list(fktotc[table].keys())
            a.sort()
            # print by to-field
            for column in a:
                out += "  to " + column.lower() + ":\n"
                # sort by from-table
                fktotc[table][column].sort(key=lambda x: x.ft)
                for fk in fktotc[table][column]:
                  out += "  " + fk.ft.lower() + "." + fk.fc.lower() + "\n"
            out += "\n"

        out += "\n"

        return out

def _rmtablename(tablename:str, row:dict) -> dict:
    """
    _rmtablenames removes table name from <tablename>.<columname> keys keys in row-dict and returns row-dict
    gives an error if only some columns are preceeded by a table name, and if table names don't match the given name
    """
    
    out = {}
    # find out whether the first key has a tablename
    key = keys(row)[0]
    if re.match("\.", key):
        withname = True  # all keys should be with table name
    else:
        withname = False # no key should be with table name
    for key in keys(row):
        # key with table name when it shouldn't be, or without table name when it should be with
        if re.match("\.", key) != withname:
            print("error: there must be a tablename for all keys or for none")
            exit
        # no edits to be done, continue
        if not withnames:
            continue

        # remove tablename
        a = key.split(".")
        thistablename = a[0]
        if thistablename != tablename:
            print(f"error: table name in {key} doesn't match {tablename}")
            exit
        colname = a[1]
        # put field in out without table name
        out[colname] = row[key]

    # table name got removed from every key
    if withnames:
        return out
    else:
        # no key had a table name, return unchanged
        return row

