import configparser
import psycopg2

from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os

from sql_queries import copy_table_queries, copy_table_order, insert_table_queries, insert_table_order, count_song, graph

## acess s3 data
config = configparser.ConfigParser()
config.read_file(open('dwh1.cfg'))

S3_UDACITY_REGION      = config.get("S3", "UDACITY_REGION")
S3_UDACITY_BUCKET      = config.get("S3", "UDACITY_BUCKET")
S3_MYBUCKET            = config.get("S3", "MY_S3")
DWH_ROLE_ARN           = config.get("ARN","ARN_RULE")

def load_staging_tables(cur, conn):
    queries = []
    queries.append(copy_table_queries[0].format(S3_UDACITY_BUCKET, DWH_ROLE_ARN, S3_UDACITY_BUCKET, S3_UDACITY_REGION))
    queries.append(copy_table_queries[1].format(S3_MYBUCKET, DWH_ROLE_ARN, S3_UDACITY_REGION))

    i = 0
    
    for query in queries:
        try:
            cur.execute(query)
        except Exception as e:
            print ("Load staging - Error:", copy_table_order[i])
        else:
            print ("Loaded staging", copy_table_order[i])
        i += 1
            
## count rows in staging_songs                                    
def count_staging_song(cur, conn):
    total_count = []
    for query in count_song:
        try:
            cur.execute(query)
        except Exception as e:
            print ("Count table staging_songs - Error:", e)
        else:
            total_count.append(cur.fetchall())
                   
    print ('Total Song =', total_count[0])
    print ('Total Song_id Unique id =', total_count[1])
    print ('Total Song_Title not null =', total_count[2])
    print ('Total Song_Duration not null = ', total_count[3])

    if total_count[0] == total_count[1] == total_count[2] == total_count[3]:
        print ('Staging song has no null values')
    else:
        print ('Staging song has null values')

# FINAL TABLES
def insert_tables(cur, conn):
    i = 0
    for query in insert_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print ("Load table", insert_table_order[i], 'Error:', e)
        else:
            print ("Loaded table", insert_table_order[i])
        i += 1
                   
##Get a graph for songsplay                  
def graph_songplay(cur, conn):

    script_dir = os.path.dirname(__file__)
    results_dir = os.path.join(script_dir, 'Results/')
    file_name = "Song_level"

    if not os.path.isdir(results_dir):
        os.makedirs(results_dir)

    df = pd.read_sql(graph, conn)

    df.dropna(inplace=True)
    df['start_date'] = df['start_time'].dt.date
    df_level = df[['start_time', 'start_date', 'level']].value_counts().reset_index().rename(columns={0:'qty_level'})

    df_level.sort_values(by='start_date', inplace=True)

    p = sns.catplot(x="start_date", y="qty_level", hue='level', kind="bar", data=df_level, ci=None)
    p.set(title='Time Level Variation')
    plt.xticks(rotation=75)
    plt.gcf().set_size_inches(18, 6)

    p.savefig(results_dir + file_name)
    print ("See on", results_dir + file_name)

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
        print ('Error set cursor', e)
    
    conn.autocommit = True
    
    ## load staging events and songs
    print ('------ 1. Loading staging tables----------')               
    load_staging_tables(cur, conn)
                   
    ## count rows in staging_songs
    print ('------ 2. Count staging songs----------')    
    count_staging_song(cur, conn)               
    
    ## load sparkfy tables               
    print ('------ 3. Loading sparkfy tables----------')    
    insert_tables(cur, conn)
    
    ## get graph for songsplay
    graph_songplay(cur, conn)               
                   
    ## end              
    conn.close()
    print ('-------------- ETL ENDED -----------')
       

if __name__ == "__main__":
    main()