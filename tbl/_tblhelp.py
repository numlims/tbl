from dbcq import *

class tblhelp:
    def __init__(self, db:dbcq):
        self.db = db
    def withidentity(self, table, data):
        out = {}
        for k in self.columns(table):
            if k in data:
                out[k] = data[k]
        return out
    def withoutidentity(self, table, data):
        identities = self.identity_keys(table)
        for ident in identities:
            if ident in data:
                data.pop(ident)
        return self.withidentity(table, data) # identities is out
    def wherestring(self, dict, args):
        out = ""
        keys = list(dict)
        for i in range(0, len(keys)):
            
            no comma after last pair.
            
            
            the args, if None, append nothing.
            
            
            after the loop, return.
            
            ``/wherestring
                    return out
            
build sql query. if value None, say is NULL.

    def primary_keys(self, table):
        result = self.db.qfad("""
                  select column_name from information_schema.key_column_usage 
                  where table_name = ? 
                  and constraint_name like 'PK%'""", table)
        out = []
        for row in result:
            out.append(row["column_name"].lower())
        return out
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
    def columns(self, table=None):
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
        else: 
            result = self._columnsquery(table)
            out = []
            for row in result:
                out.append(row["column_name"].lower())
            out.sort()
            return out
    
        def _columnsquery(self, table=None):
            query = ""
            if self._issqlite():
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
            if table == None:
                return self.db.qfad(query)
            else:
                #print("table: " + table)
                return self.db.qfad(query, table)
    def columntypes(self, table):
        if self._ismssql():
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
    def tables(self):
        tables = []
        if self._ismssql():
            result = self.db.qfad("exec sp_tables")
            tables = []
            for row in result:
                if row["table_owner"] == "dbo":
                    tables.append(row["table_name"].lower())
        elif self._issqlite():
            res = self.db.qfad("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row["name"] for row in res]
        else:
            print(f"tables not supported for {self._type()}")
            exit
        tables.sort()
        return tables
    def _type(self):
        return self.db.info['type']
    def _ismssql(self):
        return self.db.info["type"] == "mssql"
    def _issqlite(self):
        return self.db.info["type"] == "sqlite"

