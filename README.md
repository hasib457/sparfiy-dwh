# Sparkify Data Warehouse
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The purpose of this project is to create data warehouse on an AWS Redshift cluster, then extracting data from two S3 buckets, one containing songs and the other containing user activity logs, load it in a staging tables, and finally transforming it and inserting it into the final tables.

# Project files
1. `create_tables.py` : This file is responsible for creating the staging and final tables.
2. `etl.py`: This file performs the ETL pipeline by copying data from S3 into staging tables, transforming the data, and inserting it into the final tables.
3. `dwh.cfg`: This configuration file contains all necessary information to connect to the Redshift cluster, such as host, database name, username, password, and IAM role.
4. `sql_queries.py`: This file contains all the SQL queries to create and drop tables, copy data from S3 to staging tables, and insert data into final tables.
5. `create_dwh_infrastructure.py`: This file is responsible for creating or deleting an Amazon Redshift cluster on AWS

# Sparkify Data
Sparkify data consist of 2 primary data sources songs data and user log data.
1. `Songs data` - consists of log files in JSON format.
 Each file is in JSON format and contains metadata about a song and the artist of that song. 
 The files are partitioned by the first three letters of each song's track ID. 
 For example, here are file paths to two files in this dataset.

        s3://udacity-dend/song_data/A/B/C/TRABCEI128F424C983.json
        s3://udacity-dend/song_data/A/A/B/TRAABJL12903CDCF1A.json
    And below is an example of what a single song file, `TRAABJL12903CDCF1A.json`, looks like.
        
        {
            "num_songs": 1, 
            "artist_id": "ARD7TVE1187B99BFB1", 
            "artist_latitude": null, 
            "artist_longitude": null, 
            "artist_location": "California - LA", 
            "artist_name": "Casual", 
            "song_id": "SOMZWCG12A8C13C480", 
            "title": "I Didn't Mean To", 
            "duration": 218.93179, 
            "year": 0
        }
2. `User Log Data` - consists of log files in JSON format These simulate activity logs from a music streaming app based on specified configurations. 
The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

        s3://udacity-dend/log_data/2018/11/2018-11-12-events.json
        s3://udacity-dend/log_data/2018/11/2018-11-13-events.json

    And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

        {
            "artist":null,
            "auth":"LoggedIn",
            "firstName":"Walter",
            "gender":"M",
            "itemInSession":0,
            "lastName":"Frye",
            "length":null,
            "level":"free",
            "location":"San Francisco-Oakland-Hayward, CA",
            "method":"GET",
            "page":"Home",
            "registration":1540919166796.0,
            "sessionId":38,
            "song":null,
            "status":200,
            "ts":1541105830796,
            "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
            "userId":"39"
        }


# Data Model
In this project we will use the star schema data model. The star schema is a multi-dimensional data model used to organize data in a database or data warehouse so that it is easy to understand and analyze and it's optimized for querying large data sets.

It uses a single large fact table to store transactional or measured data in this case is `songplays`:

<!-- ![](sparkifydb_erd.png)
<p align="center">
        <img src="sparkifydb_erd.png">
</p> -->

1. `songplays` - records in log data associated with song plays i.e. records with page NextSong and used  `DISTSTYLE KEY`

        - songplay_id PRIMARY KEY
        - start_time
        - user_id
        - level
        - song_id DISTKEY
        - artist_id
        - session_id
        - location
        - user_agent   

And smaller dimensional tables that store attributes about the data in this case we have 4 dimensional tables:

1. `users` - users in the app and used 

        user_id PRIMARY KEY, first_name, last_name, gender, level
2. `songs` - songs in music database and used `DISTSTYLE KEY`
        
        song_id PRIMARY KEY, title, artist_id DISTKEY, year, duration
3. `artists` - artists in music database
        
        artist_id PRIMARY KEY, name, location, latitude, longitude
4. `time` - timestamps of records in songplays broken down into specific units and used `DISTSTYLE ALL`

        start_time PRIMARY KEY, hour, day, week, month, year, weekday

# ETL Pipeline
1. **Extract data from files in `s3` and load it in staging tables on redshift**
2. **Process `Song Data`:** perform ETL on the first dataset, `song_data`, to create the `songs` and `artists` dimensional tables.

    1. `Song` Table

        - Extract columns for song_id, title, artist_id, year, and duration
        - Insert Record into `Song` Table
    2. `artists` Table

        - Extract columns for artist ID, name, location, latitude, and longitude
        -  Insert Record into `Artist` Table
3. **Process `Log Data`:** perform ETL on the second dataset, `log_data`, to create the `time` and `users` dimensional tables, as well as the `songplays` fact table.

    1. `time` Table

        - Extract the timestamp, hour, day, week of year, month, year, and weekday from the `ts` 
        - Insert Records into `Time` Table
    2. `users` Table

        - Extract columns for user ID, first name, last name, gender and level
        - Insert Records into `users` table
    3. `songplays` Table
        
        - This one is a little more complicated since information from the staging songs table and staging log table are all needed for the `songplays` table. 
        - The staging log table does not specify an ID for either the song or the artist.
        - Get the song ID and artist ID by joining staging_log and staging_songs tables to find matches based on song title, artist name.
        - Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to `songplay_data`.
        - Insert Records into `Songplays` Table

# How to run the project
1. Fill in the `dwh.cfg` file with your own Redshift cluster information and IAM role ARN.
2. Run `create_dwh_infrastructure.py` for create the redshift culster

        python3 create_dwh_infrastructure.py --create
        python3 create_dwh_infrastructure.py --delete

3. Run `create_tables.py` to create the staging and final tables.
4. Run `etl.py` to perform the ETL pipeline.

