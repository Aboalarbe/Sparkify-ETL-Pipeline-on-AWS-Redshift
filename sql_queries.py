import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= "CREATE TABLE IF NOT EXISTS staging_events \
  ( \
     artist VARCHAR, \
     auth VARCHAR, \
     firstName VARCHAR, \
     gender CHAR(1), \
     itemSession INT, \
     lastName VARCHAR, \
     length NUMERIC, \
     level VARCHAR, \
     location  VARCHAR, \
     method VARCHAR, \
     page VARCHAR, \
     registeration NUMERIC, \
     sessionId INT, \
     song VARCHAR, \
     status INT, \
     ts NUMERIC, \
     userAgent VARCHAR, \
     userId INT \
  );"

staging_songs_table_create = "CREATE TABLE IF NOT EXISTS staging_songs \
  ( \
     num_songs INT, \
     artist_id VARCHAR, \
     artist_latitude NUMERIC, \
     artist_longitude NUMERIC, \
     artist_location VARCHAR, \
     artist_name VARCHAR, \
     song_id VARCHAR, \
     title VARCHAR, \
     duration  NUMERIC, \
     year INT \
  );"

songplay_table_create = "CREATE TABLE IF NOT EXISTS songplays \
  ( \
     songplay_id INT IDENTITY(0,1) PRIMARY KEY, \
     start_time  NUMERIC NOT NULL SORTKEY \
     user_id     INT DISTKEY \
     level       VARCHAR NOT NULL, \
     song_id     VARCHAR, \
     artist_id   VARCHAR, \
     session_id  INT NOT NULL, \
     location    VARCHAR, \
     user_agent  VARCHAR NOT NULL \
  );"

user_table_create = "CREATE TABLE IF NOT EXISTS users \
( \
user_id INT SORTKEY, \
first_name VARCHAR, \
last_name VARCHAR, \
gender CHAR(1), \
level VARCHAR \
);"

song_table_create = "CREATE TABLE IF NOT EXISTS songs ( \
song_id VARCHAR PRIMARY KEY SORTKEY, \
title VARCHAR NOT NULL, \
artist_id VARCHAR NOT NULL, \
year INT NOT NULL, \
duration NUMERIC NOT NULL \
);"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists ( \
artist_id VARCHAR PRIMARY KEY SORTKEY, \
name VARCHAR NOT NULL, \
location VARCHAR, \
latitude VARCHAR, \
longitude VARCHAR \
);"

time_table_create = "CREATE TABLE IF NOT EXISTS time ( \
start_time TIMESTAMP PRIMARY KEY SORTKEY, \
hour INT, \
day INT, \
week INT, \
month INT, \
year INT, \
weekday INT \
);"

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {}
IAM_ROLE '{}'
FORMAT AS JSON {}
compupdate off region 'us-west-2'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs from {}
IAM_ROLE '{}'
FORMAT AS JSON 'auto'
compupdate off region 'us-west-2'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = "INSERT INTO songplays\
(start_time, user_id, level, song_id, artist_id, \
session_id, location, user_agent) \
SELECT DISTINCT l.ts, l.userId, l.level, s.song_id, \
s.artist_id, l.sessionId, l.location, l.userAgent \
FROM staging_events l JOIN staging_songs s \
ON l.song = s.title AND l.artist = s.artist_name;"

user_table_insert = "INSERT INTO users \
(user_id, first_name, last_name, gender, level) \
SELECT DISTINCT userId, firstName, lastName, gender, level \
FROM staging_events;"

song_table_insert = "INSERT INTO songs \
(song_id, title, artist_id, year, duration) \
SELECT DISTINCT song_id, title, artist_id, year, duration \
FROM staging_songs;"

artist_table_insert = "INSERT INTO artists \
(artist_id, name, location, latitude, longitude) \
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude \
FROM staging_songs;"

time_table_insert = "INSERT INTO time \
(start_time, hour, day, week, month, year, weekday) \
WITH time_ts AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events) \
SELECT DISTINCT ts, extract(hour from ts), extract(day from ts), extract(week from ts), \
extract(month from ts), extract(year from ts), extract(weekday from ts) from time_ts;"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
