import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_tables_order, drop_tables_order


def drop_tables(cur, conn):
    i = 0
    for query in drop_table_queries:
        try:
             cur.execute(query)
        except Exception as e:
            print ('Drop table', drop_tables_order[i], 'Error:', e)
        else:
            print ("Table", drop_tables_order[i], 'Droped')
        i += 1


def create_tables(cur, conn):
    i = 0
    for query in create_table_queries:
        try:
             cur.execute(query)
        except Exception as e:
            print ('Create table', create_tables_order[i], 'Error:', e)
        else:
            print ('Table', create_tables_order[i], 'Created')
        i += 1


def main():
    ## get parameters for Postgree connect (AWS_connect.py)
    config = configparser.ConfigParser()
    config.read('dwh1.cfg')
    
    conn_string  = config.get("POSTGREE", "POSTGREE_CONNECT")
    #Creating a cursor object using the cursor() method
    conn = psycopg2.connect(conn_string)
    
    try:
        cur = conn.cursor()
    except Exception as e:
        print ('9.1 Error set cursor', e)
    
    conn.autocommit = True
    
    drop_tables(cur, conn)
    print ('------------------------')
    
    create_tables(cur, conn)
    
    conn.close()

if __name__ == "__main__":
    main()