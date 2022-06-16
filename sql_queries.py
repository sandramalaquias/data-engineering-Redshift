### SQL used for ETL process and create_tables.py

# DROP TABLES
schema_staging_drop       = (""" DROP SCHEMA IF EXISTS staging CASCADE""")
staging_events_table_drop = (""" DROP TABLE IF EXISTS staging.staging_events;""")
staging_songs_table_drop  = (""" DROP TABLE IF EXISTS staging.staging_songs;""")


schema_sparkfy_drop       = (""" DROP SCHEMA IF EXISTS sparkfy CASCADE""")
songplay_table_drop       = (""" DROP TABLE IF EXISTS sparkfy.songsplay;""")
user_table_drop           = (""" DROP TABLE IF EXISTS sparkfy.users;""")
song_table_drop           = (""" DROP TABLE IF EXISTS sparkfy.songs;""")
artist_table_drop         = (""" DROP TABLE IF EXISTS sparkfy.artists;""")
time_table_drop           = (""" DROP TABLE IF EXISTS sparkfy.time;""")

# CREATE TABLES

staging_schema_create = ("""CREATE SCHEMA IF NOT EXISTS staging;
                            SET search_path TO staging;""")

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS "staging_events" (
                              "artist" varchar, 
                              "auth" varchar, 
                              "firstName" varchar,
                              "gender" varchar, 
                              "itemInSession" varchar, 
                              "lastName" varchar, 
                              "length" DECIMAL, 
                              "level" varchar, 
                              "location" varchar, 
                              "method" varchar, 
                              "page" varchar, 
                              "registration" varchar,
                              "sessionId" varchar,
                              "song" varchar, 
                              "status" varchar,
                              "ts" varchar,
                              "userAgent" varchar, 
                              "userId" int);                  
                    """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS "staging_songs" (
                    "song_id" varchar,
                    "num_songs" int,
                    "title" varchar,
                    "artist_name" varchar,
                    "artist_latitude" DOUBLE PRECISION,
                    "year" int,
                    "duration" DOUBLE PRECISION,
                    "artist_id" varchar,
                    "artist_longitude" DOUBLE PRECISION,
                    "artist_location" varchar);
                """)

sparkfy_schema_create = ("""CREATE SCHEMA IF NOT EXISTS sparkfy;
                    SET search_path TO sparkfy;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS "songs" (
                    "song_id" varchar,
                    "title" varchar, 
                    "artist_id" varchar,
                    "year" int,
                    "duration" DOUBLE PRECISION);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS "artists" (
                    "artist_id" varchar,
                    "name" varchar,
                    "location" varchar,
                    "latitude" DOUBLE PRECISION,
                    "longitude" DOUBLE PRECISION);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS "time" (
                    "start_time" timestamp DISTKEY,
                    "hour" int,
                    "day" int,
                    "week" int,
                    "month" int,
                    "year" int,
                    "weekday" int,
                    "dayname" varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS "users" (
                    "user_id" int,
                    "first_name" varchar,
                    "last_name" varchar,
                    "gender" varchar,
                    "level" varchar);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS "songsplay" (
                    "start_time" timestamp SORTKEY,
                    "user_id" int,
                    "level" varchar,
                    "song_id" varchar,
                    "artist_id" varchar,
                    "session_id" varchar,
                    "location" varchar,
                    "user_agent" varchar);""")


# STAGING TABLES

staging_events_copy = ("""copy staging.staging_events
                        from 's3://{}/log-data/'
                        credentials 'aws_iam_role={}'
                        json 's3://{}/log_json_path.json'
                        region '{}'
                        compupdate off;
                        """)


staging_songs_copy = staging_songs_copy = ("""copy staging.staging_songs from 's3://{}/song.manifest'
                                              credentials 'aws_iam_role={}'
                                              region '{}' 
                                              compupdate off 
                                              JSON 'auto' truncatecolumns
                                              manifest;
                                          """)

# FINAL TABLES

songplay_table_insert = ("""insert into sparkfy.songsplay (start_time, user_id, level, song_id, artist_id, session_id, 
                            location, user_agent)
                        (select  
                            TIMESTAMP 'epoch' + event.ts/1000 *INTERVAL '1 second' as start_time,
                            event.userid, event.level, song.song_id, artist.artist_id, event.sessionid, 
                            event.location, event.useragent
                        from staging.staging_events as event
                        left outer join sparkfy.songs as song on event.song = song.title
                        left outer join sparkfy.artists as artist on event.artist = artist.name
                        where event.page = 'NextSong');
                        """)

user_table_insert = ("""select distinct userid, firstname, lastname, gender, level
                     from staging.staging_events
                     where userid is not null;
                     """)


song_table_insert = ("""insert into sparkfy.songs (song_id, title, artist_id, year, duration)
                    (select distinct song_id, title, artist_id, year, duration
                     from staging.staging_songs
                     where song_id is not null and
                     title is not null);
                     """)


artist_table_insert = ("""insert into sparkfy.artists (artist_id, name, location, latitude, longitude)
                       (select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                        from staging.staging_songs
                        where artist_id is not null and
                        artist_name is not null);
                        """)

time_table_insert = ("""insert into sparkfy.time (start_time, hour, day, week, month, year, weekday, dayname)
                    with start_time as 
                        (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as ts
                            FROM staging.staging_events 
                        where ts is not null)    
                    select ts as ts, 
                        extract (hour from ts) as hour,
                        extract (day from ts) as day,
                        extract (week from ts) as week,
                        extract (month from ts) as month,
                        extract (year from ts) as year,
                        extract (weekday from ts) as weekday,
                        to_char(ts, 'Day') as dayname
                    from start_time;
                    """)


# COUNT SONG STAGING 
count_song_staging = ("""Select count(*) from dwh.staging.staging_songs""")

### unique song_id (distinct filter the null values)
count_song_id = ("""select count(DISTINCT song_id)
                    from dwh.staging.staging_songs""")  


### count song_tilte with no null values
count_song_title = ("""select count(*)
                    from dwh.staging.staging_songs
                    where title is not null""")

### count song_duration with no null values
count_song_duration = ("""select count(*)
                    from dwh.staging.staging_songs
                    where duration is not null""")


# QUERY LISTS
##-----------------------------------
create_table_queries = [staging_schema_create, staging_events_table_create, staging_songs_table_create,
                        sparkfy_schema_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, 
                        time_table_create]

create_tables_order = ['staging_schema', 'staging_events', 'staging_songs', 'parkfy_schema', 'songplay', 
                       'users', 'songs', 'artists', 'time']
##----------------------------------------

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, 
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop,
                     schema_staging_drop, schema_sparkfy_drop]

drop_tables_order = ['staging_events', 'staging_songs', 'songplay', 'users', 'songs', 'artists', 'time',
                    'staging_schema', 'sparkfy_schema']

##----------------------------------------

copy_table_queries = [staging_events_copy, staging_songs_copy]
copy_table_order = ['staging_events', 'staging_songs']

##-------------------------------------------------------------

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_order   = ['songplay', 'user', 'song', 'artist', 'time']

##------------------------------------

count_song = [count_song_staging, count_song_id, count_song_title, count_song_duration]

###Get a graph for songsplay
graph = ("""SELECT start_time, level from sparkfy.songsplay""")