import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES
drop_staging_events_table = "DROP TABLE IF EXISTS staging_events;"
drop_staging_songs_table = "DROP TABLE IF EXISTS staging_songs;"
drop_songplay_table = "DROP TABLE IF EXISTS songplays;"
drop_users_table = "DROP TABLE IF EXISTS users;"
drop_songs_table = "DROP TABLE IF EXISTS songs;"
drop_artists_table = "DROP TABLE IF EXISTS artists;"
drop_time_table = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

create_staging_events_table= (
    """
    CREATE TABLE staging_events 
    (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR,
        length DECIMAL,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts VARCHAR,
        userAgent VARCHAR,
        userId INTEGER
    );
    """
)

create_staging_songs_table= (
    """
    CREATE TABLE staging_songs 
    (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration DECIMAL,
        year INTEGER
    );
    """
)

create_playsong_table=(
    """
    CREATE TABLE IF NOT EXISTS songplays 
    (
        songplay_id INTEGER 	IDENTITY(0,1) 	PRIMARY KEY,
		start_time	TIMESTAMP 	NOT NULL 		SORTKEY,
		user_id		INTEGER		NOT NULL 		REFERENCES users(user_id),
		LEVEL		VARCHAR,
		song_id		VARCHAR		NOT NULL 		REFERENCES	songs(song_id),
		artist_id	VARCHAR		NOT NULL 		REFERENCES	artists(artist_id),
		session_id	INTEGER		NOT NULL,
		location	VARCHAR, 
		user_agent	VARCHAR
    );
    """
)

create_artists_table=(
    """
    CREATE TABLE IF NOT EXISTS artists 
    (
        artist_id	VARCHAR		DISTKEY 	PRIMARY KEY,
		name		VARCHAR 	NOT NULL,
		location	VARCHAR,
		latitude	DECIMAL,
		longitude	DECIMAL
    );
    """
)

create_users_table=(
    """
    CREATE TABLE IF NOT EXISTS users 
    (
        user_id		INTEGER		SORTKEY 	PRIMARY KEY,
		first_name	VARCHAR 	NOT NULL,
		last_name	VARCHAR 	NOT NULL,
		gender		CHAR(1),
        level 		VARCHAR NOT NULL
    );
    """
)

create_songs_table=(
    """
    CREATE TABLE IF NOT EXISTS songs 
    (
        song_id		VARCHAR		SORTKEY 	PRIMARY KEY,
		title		VARCHAR 	NOT NULL,
		artist_id	VARCHAR		NOT NULL 	REFERENCES	artists(artist_id),
		year		INTEGER		NOT NULL,
		duration	INTEGER		NOT NULL
    );
    """
)

create_time_table=(
    """
    CREATE TABLE IF NOT EXISTS time 
    (
        start_time	TIMESTAMP	SORTKEY 	PRIMARY KEY,
		hour		NUMERIC 	NOT NULL,
		day			NUMERIC		NOT NULL,
		week 		NUMERIC		NOT NULL,
		month		NUMERIC		NOT NULL,
		year		NUMERIC		NOT NULL,
		weekday		NUMERIC		NOT NULL
    );
    """
)


# STAGING TABLES

copy_staging_events = (
    """
    COPY staging_events 
    FROM {}
    iam_role '{}'
    FORMAT AS json {}
    """).format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)

copy_staging_songs = (
    """
    COPY staging_songs 
    FROM {}
    iam_role '{}'
    FORMAT AS json 'auto'
    """).format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)



# FINAL TABLES

insert_into_songplay_table = (
    """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second' AS start_time,
        e.userId AS user_id,
        e.level AS level,
        s.song_id AS song_id,
        s.artist_id AS artist_id,
        e.sessionId AS session_id,
        e.location AS location,
        e.userAgent AS user_agent
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  =  'NextSong'
    """
)

insert_into_users_table = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong' 
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
    """
)

insert_into_songs_table = (
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
    """
)

insert_into_artists_table = (
    """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) as artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
    """
)

insert_into_time_table = (
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second' AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) as weekday
    FROM staging_events e;
    """
)




# QUERY LISTS

create_tables_order = ['staging events', 'staging songs', 'time', 'users', 'artists', 'songs', 'songplays']
create_table_queries = [create_staging_events_table, create_staging_songs_table, create_time_table, create_users_table,\
                        create_artists_table, create_songs_table, create_playsong_table]
                        
drop_tables_order = ['staging events', 'staging songs', 'songplays', 'users', 'songs', 'artists', 'time']
drop_table_queries = [drop_staging_events_table, drop_staging_songs_table, drop_songplay_table, drop_users_table, \
                      drop_songs_table, drop_artists_table, drop_time_table]

staging_tables_order = ['staging events', 'staging songs']
copy_table_queries = [copy_staging_events, copy_staging_songs]

dwh_tables_order = ['artists', 'songs', 'time', 'users', 'songplays']
insert_table_queries = [insert_into_artists_table, insert_into_songs_table, insert_into_time_table, insert_into_users_table, insert_into_songplay_table]
