from dbcq import *

# tblhelp.py gives helper functions for tbl

class tblhelp:

    # init tblhelp with a dbcq connection
    def __init__(self, db:dbcq):
        self.db = db

    # withidentity checks that there are no keys in data that are not fields in table, leaves identity columns in
    def withidentity(self, table, data):
        out = {}
        for k in self.fields(table):
            if k in data:
                out[k] = data[k]
        return out

    # withoutidentity checks that there are no keys in data that are not fields in table, throws identity columns out
    def withoutidentity(self, table, data):
        identities = self.identity_keys(table)
        for ident in identities:
            if ident in data:
                data.pop(ident)
        return self.withidentity(table, data) # identities is out

    # wherestring returns the keys and values in args as a sql where condition string
    # and appends the values to the args array
    def wherestring(self, dict, args):
        out = ""
        keys = list(dict)
        for i in range(0, len(keys)):
            # build sql query
            # if value None, say is NULL
            if dict[keys[i]] == None:
                out += "[" + keys[i] + "] is null"
            else:
                out += "[" + keys[i] + "] = ? "
            # no comma after last pair
            out += " AND " if i < len(keys)-1 else ""  
            # the args, if None, append nothing
            if dict[keys[i]] != None:
                args.append(dict[keys[i]])
        return out

    # primary_keys queries the primary keys for table and returns them
    # needed?
    # todo build like fields(self, table=None)
    def primary_keys(self, table):
        # if myssql
        # from https://stackoverflow.com/a/2341388
        "SHOW KEYS FROM table WHERE Key_name = 'PRIMARY'"

        # if mssql
        # from https://stackoverflow.com/a/10966944
        result = self.db.qfad("""
                  select column_name from information_schema.key_column_usage 
                  where table_name = ? 
                  and constraint_name like 'PK%'""", table)
        out = []
        for row in result:
            out.append(row["column_name"].lower())
        return out

    # primary_keys returns all primary keys as table-indexed dict
    def primary_keys(self):
        if self._ismssql():
            result = self.db.qfad("""
                      select table_name, column_name from information_schema.key_column_usage 
                      where constraint_name like 'PK%'""")
            out = {}
            for row in result:
                if not row["table_name"].lower() in out:
                    out[row["table_name"].lower()] = []
                out[row["table_name"].lower()].append(row["column_name"].lower())
            return out
        else:
            print(f"identity_keys not supported for {self._type()}")
            exit

    # identity_keys returns the identity columns of table as array
    def identity_keys(self, table):
        if self._ismssql():
            result = query_fetch_all_dict(
            """
            select column_name from information_schema.columns
            where columnproperty(object_id(table_schema + '.' + table_name), column_name, 'IsIdentity') = 1
            and table_name = ?""", table)
            out = []
            for row in result:
                out.append(row["column_name"].lower())
            return out
        else:
            print(f"identity_keys not supported for {self._type()}")
            exit

    # fields returns the column names of table as an array of
    # strings. if no table is given, it returns a dict by table that
    # holds the column names for every table. 
    def fields(self, table=None):
        # return for all tables with columns
        if table == None:
            result = self._columnsquery()

            tables = self.tables()
            out = {}
            # make an entry for every table in the dict, even if it doesn't have a column
            for table in tables:
                out[table.lower()] = []
            for row in result:
                t = row["table_name"].lower()
                c = row["column_name"].lower()
                if t not in tables: # e.g. schema_version is in result but not in tables
                    continue
                out[t].append(c)

            return out
        
        else: # return columns for specific table
            result = self._columnsquery(table)
            out = []
            for row in result:
                out.append(row["column_name"].lower())
            return out

    # _columnsquery returns dict array of tables and columns, either for one table or for all tables
    # this is an extra function because it can called with table name or without
    # we could also query all tables and colums, and filter afterward if only one table is wished
    # todo: bug for sqlite with table argument
    def _columnsquery(self, table=None):
        query = ""
        if self._issqlite():
            # from https://stackoverflow.com/a/50548508
            query = """
            SELECT m.name as table_name, 
            p.name as column_name
            FROM sqlite_master m
            left outer join pragma_table_info((m.name)) p
            on m.name <> p.name
            """
            if table != None:
                query += " where table_name = ?"
            query += """
            order by table_name, column_name
            """
        elif self._ismssql():
            query = """
            select table_name, column_name from information_schema.columns 
            """
            if table != None:
                query += " where table_name = ? "
            query += """
            order by ordinal_position
            """
        else:
            print(f"colums not supported for {self._type()}")
            exit

        # pass table or not
        if table == None:
            return self.db.qfad(query)
        else:
            #print("table: " + table)
            return self.db.qfad(query, table)

    # columntypes returns a dict of columnnames and types for table
    def columntypes(self, table):
        if self._ismssql():
            # query from https://www.mytecbits.com/microsoft/sql-server/list-of-column-names
            query = """
            SELECT
            COLUMN_NAME as name, DATA_TYPE as type
            FROM
            INFORMATION_SCHEMA.COLUMNS
            WHERE
            TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """   
            result = self.db.qfad(query, table) 
            out = {}
            for row in result:
                out[row["name"].lower()] = row["type"].lower()
            return out
        else:
            print(f"columntypes is not supported for {self._type()}")
            exit

    # tables gives the names of the tables in the db
    def tables(self):
        # for mssql
        if self._ismssql():
            result = self.db.qfad("exec sp_tables")
            tables = []
            for row in result:
                if row["table_owner"] == "dbo":
                    tables.append(row["table_name"].lower())
            return tables
        elif self._issqlite():
            # for sqlite
            # from https://stackoverflow.com/a/83195
            res = self.db.qfad("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row["name"] for row in res]
            return tables
        else:
            print(f"tables not supported for {self._type()}")
            exit

    # _type returns the db type
    def _type(self):
        return self.db.info['type']
    
    # _ismssql says whether database is mssql
    def _ismssql(self):
        return self.db.info["type"] == "mssql"

    # _issqlite says whether database is sqlite
    def _issqlite(self):
        return self.db.info["type"] == "sqlite"

