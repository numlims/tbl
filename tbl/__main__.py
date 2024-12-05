# __main__ runs tbl on cmd line

import sys
import simplejson as json
from tbl import tbl

# main starts tbl
def main():    

    target = sys.argv[1]

    # start tbl
    t = tbl(target)

    # json of all foreign keys
    if sys.argv[2] == "fk":
        #print(json.dumps(t.fk()))
        print(json.dumps(t.fk(), default=str))

    # json list of tables
    if sys.argv[2] == "tables":
        print(json.dumps(t.tables(), default=str))

    # json list of fields
    if sys.argv[2] == "fields":
        # table name given
        if len(sys.argv) == 4:
            table = sys.argv[3].lower()
            print(json.dumps(t.fields(table), default=str))
        else: # table name not given
            print(json.dumps(t.fields(), default=str))

    # human readable table summary with fields and fks
    if sys.argv[2] == "table":
        table = sys.argv[3].lower()
        print(t.tablesummary(table), end="")


sys.exit(main())
