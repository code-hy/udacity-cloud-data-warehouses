## this stores all the queries used for drop, staging table creates, and fact-dimension 
## table creates

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplays_table_drop = "DROP TABLE IF EXISTS fact_songplays"
users_table_drop = "DROP TABLE IF EXISTS dim_users"
songs_table_drop = "DROP TABLE IF EXISTS dim_songs"
artists_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
   CREATE TABLE staging_events
   (
   artist               varchar,
   auth                 varchar, 
   firstName            varchar,
   gender               varchar,
   iteminSession        varchar,
   lastName             varchar,   
   length               varchar,
   level                varchar,
   location             varchar,
   method               varchar,
   page                 varchar,
   registration         varchar,
   sessionid            int ,
   song                 varchar,
   status               int,
   ts                   bigint,  
   userAgent            varchar,
   userid               int
   ) diststyle auto 
     sortkey auto;
""")

staging_songs_table_create = ("""
   CREATE TABLE staging_songs
   (
    num_songs           int,
    artist_id           varchar,
    artist_latitude     varchar,
    artist_longitude    varchar,
    artist_location     varchar,
    artist_name         varchar,
    song_id             varchar,
    title               varchar,
    duration            numeric(14,7),
    year                int      
   ) diststyle auto
     sortkey auto;
""")

fact_songplays_table_create = ("""
    CREATE TABLE fact_songplays
    (
    songplay_id          int identity(0,1) primary key ,
    start_time           timestamp,
    user_id              int,
    level                varchar,
    song_id              varchar NOT NULL,
    artist_id            varchar NOT NULL,
    session_id           int,
    location             varchar,
    user_agent           varchar
    );
""")

dim_users_table_create = ("""
   CREATE TABLE dim_users
   (
   user_id               int NOT NULL primary key,
   first_name            varchar,
   last_name             varchar,
   gender                varchar,
   level                 varchar
   );
""")

dim_songs_table_create = ("""
   CREATE TABLE dim_songs
   (
   song_id               varchar NOT NULL primary key,
   title                 varchar,
   artist_id             varchar NOT NULL,
   year                  int,
   duration              varchar
   );
""")

dim_artists_table_create = ("""
   CREATE TABLE dim_artists
   (
   artist_id             varchar NOT NULL primary key,
   name                  varchar,
   location              varchar,
   latitude              varchar,
   longitude             varchar
   );
""")

dim_time_table_create = ("""
   CREATE TABLE dim_time
   (
   start_time            timestamp NOT NULL primary key,
   hour                  int,
   day                   int,
   week                  int,
   month                 int,
   year                  int,
   weekday               varchar
   );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    IAM_ROLE '{}'
    format as json {}
    region 'us-west-2';
""").format(LOG_DATA, IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    IAM_ROLE '{}'
    json 'auto'
    compupdate on region 'us-west-2';
""").format(SONG_DATA,IAM_ROLE)

# FINAL TABLES

fact_songplays_table_insert = ("""
   INSERT INTO fact_songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
   SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts /1000) * INTERVAL '1 second' ,
       se.userid    as user_id,
       se.level     as level,
       ss.song_id   as song_id,
       ss.artist_id as artist_id,
       se.sessionid as session_id,
       se.location  as location,
       se.useragent as user_agent
   FROM staging_events se
   JOIN staging_songs ss ON ss.title = se.song AND ss.artist_name = se.artist
   WHERE se.page = 'NextSong'
   AND user_id is not null
   AND song_id is not null;    
""")

dim_users_table_insert = ("""
   INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
   SELECT DISTINCT
      se.userid    as user_id,
      se.firstName as first_name,
      se.lastName  as last_name,
      se.gender    as gender,
      se.level     as level
   FROM staging_events se
   WHERE se.userid is not null
   and se.page = 'NextSong';      
""")

dim_songs_table_insert = ("""
   INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
   SELECT DISTINCT
      ss.song_id   as song_id,
      ss.title     as title,
      ss.artist_id as artist_id,
      ss.year      as year,
      ss.duration  as duration
   FROM staging_songs ss
   WHERE ss.song_id IS NOT NULL;
""")

dim_artists_table_insert = ("""
   INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
   SELECT DISTINCT
      ss.artist_id        as artist_id,
      ss.artist_name      as name,
      ss.artist_location  as location,
      ss.artist_latitude  as latitude,
      ss.artist_longitude as longitude
   FROM staging_songs ss
   WHERE ss.artist_id is not null;
""")

dim_time_table_insert = ("""
   INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
   SELECT DISTINCT
      TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
      EXTRACT(HOUR from start_time) as hour,
      EXTRACT(DAY  from start_time) as day,
      EXTRACT(WEEKS from start_time) as week,
      EXTRACT(MONTH from start_time) as month,
      EXTRACT(YEAR from start_time)  as year,
      EXTRACT(WEEKDAY from start_time) as weekday
   FROM staging_events se
   WHERE se.ts is not null
""")

fact_songplays_select = ("""
   SELECT * from fact_songplays limit 10
""")
dim_users_select = ("""
   SELECT * from dim_users limit 10
""")
dim_songs_select = ("""
   SELECT * from dim_songs limit 10
""")
dim_artists_select = ("""
   SELECT * from dim_artists limit 10
""")
dim_time_select = ("""
   SELECT * from dim_time limit 10
""")

fact_songplays_count = ("""
   SELECT count(*) from fact_songplays 
""")
dim_users_count = ("""
   SELECT count(*) from dim_users
""")
dim_songs_count = ("""
   SELECT count(*) from dim_songs 
""")
dim_artists_count = ("""
   SELECT count(*) from dim_artists 
""")
dim_time_count = ("""
   SELECT count(*) from dim_time 
""")
staging_events_count = ("""
   SELECT count(*) from staging_events
""")
staging_songs_count = ("""
   SELECT count(*) from staging_songs
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, fact_songplays_table_create, dim_users_table_create, dim_songs_table_create, dim_artists_table_create, dim_time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [fact_songplays_table_insert, dim_users_table_insert, dim_songs_table_insert, dim_artists_table_insert, dim_time_table_insert]
select_table_queries = [fact_songplays_select, dim_users_select, dim_songs_select, dim_artists_select, dim_time_select]
count_table_queries = [fact_songplays_count, dim_users_count, dim_songs_count, dim_artists_count,
dim_time_count, staging_events_count, staging_songs_count]