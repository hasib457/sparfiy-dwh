# pylint: disable=all

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")
DWH_ROLE_ARN = config.get("CLUSTER", "DWH_ROLE_ARN")

# DROP TABLES

# staging_events_table_drop = "DROP TABLE IF EXISTS staging.staging_events"
# staging_songs_table_drop = "DROP TABLE IF EXISTS staging.staging_songs"
# songplay_table_drop = "DROP TABLE IF EXISTS sparkify.songplays;"
# user_table_drop = " DROP TABLE IF EXISTS sparkify.users;"
# song_table_drop = "DROP TABLE IF EXISTS sparkify.songs;"
# artist_table_drop = "DROP TABLE IF EXISTS sparkify.artists;"
# time_table_drop = " DROP TABLE IF EXISTS sparkify.time;"

drop_staging_schema = 'DROP SCHEMA IF EXISTS staging CASCADE;'
drop_sparkify_schema = 'DROP SCHEMA IF EXISTS sparkify CASCADE;'


# CREATE STAGING TABLES

staging_events_table_create = """
    CREATE SCHEMA IF NOT EXISTS staging;
    SET search_path TO staging;
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR(255),
        auth VARCHAR(255),
        first_name VARCHAR(255),
        gender VARCHAR(1),
        item_in_session INTEGER,
        last_name VARCHAR(255),
        length NUMERIC(18,5),
        level VARCHAR(10),
        location VARCHAR(255),
        method VARCHAR(10),
        page VARCHAR(50),
        registration TIMESTAMP,
        session_id INTEGER,
        song VARCHAR(255),
        status INTEGER,
        ts TIMESTAMP,
        user_agent VARCHAR(255),
        user_id INTEGER
);

"""

staging_songs_table_create = """
    CREATE SCHEMA IF NOT EXISTS staging;
    SET search_path TO staging;    
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INTEGER, 
        artist_id VARCHAR(255), 
        artist_latitude NUMERIC, 
        artist_longitude NUMERIC, 
        artist_location VARCHAR(255), 
        artist_name VARCHAR(255),
        song_id VARCHAR(255), 
        title VARCHAR(255),
        duration NUMERIC, 
        year INTEGER);
"""

# CREATE FINAL TABLES

songplay_table_create = """
    CREATE SCHEMA IF NOT EXISTS sparkify;
    SET search_path TO sparkify; 
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL REFERENCES "time" (start_time), 
        user_id INTEGER NOT NULL REFERENCES "users" (user_id) , 
        level VARCHAR(255),
        song_id VARCHAR(255) REFERENCES "songs" (song_id) ,
        artist_id VARCHAR(255) REFERENCES "artists" (artist_id) ,
        session_id INTEGER,
        location VARCHAR(255),
        user_agent VARCHAR(255)
    )
    DISTSTYLE KEY
    DISTKEY(song_id);
"""

user_table_create = """
    CREATE SCHEMA IF NOT EXISTS sparkify;
    SET search_path TO sparkify; 
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        gender VARCHAR(255),
        level VARCHAR(255)
    )DISTSTYLE ALL;
"""

song_table_create = """
    CREATE SCHEMA IF NOT EXISTS sparkify;
    SET search_path TO sparkify; 
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR(255) PRIMARY KEY,
        title VARCHAR(255), 
        artist_id VARCHAR(255),
        year INTEGER,
        duration NUMERIC
    )DISTSTYLE KEY
    DISTKEY(artist_id);
"""

artist_table_create = """
    CREATE SCHEMA IF NOT EXISTS sparkify;
    SET search_path TO sparkify; 
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255), 
        location VARCHAR(255), 
        latitude NUMERIC, 
        longitude NUMERIC
        )DISTSTYLE ALL;
"""

time_table_create = """
    CREATE SCHEMA IF NOT EXISTS sparkify;
    SET search_path TO sparkify; 
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour SMALLINT, 
        day SMALLINT, 
        week SMALLINT, 
        month SMALLINT,  
        year SMALLINT, 
        weekday SMALLINT
    )DISTSTYLE ALL;
"""

# STAGING TABLES

staging_events_copy = ("""
    SET search_path TO staging;
    COPY staging_events FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={}'
    JSON 's3://udacity-dend/log_json_path.json'
    TIMEFORMAT as 'epochmillisecs'
    compupdate off region 'us-west-2';
    DELETE FROM staging_events WHERE page != 'NextSong';

"""
).format(DWH_ROLE_ARN)

staging_songs_copy = ("""
    SET search_path TO staging;
    COPY staging_songs FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={}'
    FORMAT JSON 'auto'
    compupdate off region 'us-west-2';
"""
).format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO sparkify.songplays(start_time, user_id , level, song_id , artist_id , session_id, location , user_agent)
    (   SELECT
            ts, 
            user_id, 
            level, 
            s.song_id, 
            s.artist_id, 
            session_id, 
            location , 
            user_agent
        FROM staging.staging_events e
        LEFT JOIN staging.staging_songs s ON (e.artist= s.artist_name AND e.song=title)
    );
"""

user_table_insert = """
    INSERT INTO sparkify.users(
        SELECT s.user_id, s.first_name, s.last_name, s.gender, s.level
        FROM staging.staging_events s
        JOIN (
            SELECT user_id, MAX(ts) AS max_ts
            FROM staging.staging_events
            GROUP BY user_id
            ) t
        ON s.user_id = t.user_id AND s.ts = t.max_ts
    );
"""

song_table_insert = """
    INSERT INTO sparkify.songs(
        SELECT 
            DISTINCIT song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging.staging_songs 
    );
"""

artist_table_insert = """
    INSERT INTO sparkify.artists(
        SELECT 
            DISTINCIT artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging.staging_songs
    );
"""

time_table_insert = """
    INSERT INTO sparkify.time(
        SELECT ts, 
            DATE_PART('hour', ts),
            DATE_PART('day', ts),
            DATE_PART('week', ts),
            DATE_PART('month', ts),
            DATE_PART('year', ts),
            DATE_PART('dow', ts)
        FROM staging.staging_events
    );
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    # staging_events_table_drop,
    # staging_songs_table_drop,
    # songplay_table_drop,
    # user_table_drop,
    # song_table_drop,
    # artist_table_drop,
    # time_table_drop,
    drop_staging_schema,
    drop_sparkify_schema
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert,
]
