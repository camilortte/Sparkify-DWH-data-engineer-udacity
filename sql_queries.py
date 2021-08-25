import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events_staging (
  artist VARCHAR, 
  auth VARCHAR, 
  firstName VARCHAR,   
  gender VARCHAR, 
  itemInSession INTEGER, 
  lastName VARCHAR, 
  length NUMERIC, 
  level VARCHAR, 
  location VARCHAR,
  method VARCHAR,
  page VARCHAR,
  registration NUMERIC,
  sessionId INTEGER,
  song VARCHAR,
  status INTEGER,
  ts BIGINT,
  userAgent VARCHAR,
  userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_staging (
  num_songs INTEGER, 
  artist_id VARCHAR, 
  artist_latitude NUMERIC, 
  artist_longitude NUMERIC, 
  artist_location VARCHAR, 
  artist_name VARCHAR, 
  song_id VARCHAR, 
  title VARCHAR, 
  duration NUMERIC,
  year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
  songplay_id INTEGER identity(0, 1), 
  start_time TIMESTAMP NOT NULL, 
  user_id INTEGER NOT NULL, 
  level VARCHAR, 
  song_id VARCHAR, 
  artist_id VARCHAR, 
  session_id INTEGER, 
  location VARCHAR, 
  usert_agent VARCHAR  
);
""")

user_table_create = ("""
CREATE TABLE users 
(
  user_id      INTEGER NOT NULL,
  first_name   VARCHAR,
  last_name    VARCHAR,
  gender       CHAR(1),
  level        VARCHAR(15) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs 
(
  song_id     VARCHAR NOT NULL,
  title       VARCHAR NOT NULL,
  artist_id   VARCHAR NOT NULL,
  year        INTEGER,
  duration    NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR NOT NULL, 
  name VARCHAR NOT NULL, 
  location VARCHAR, 
  latitude NUMERIC, 
  longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  start_time TIMESTAMP NOT NULL, 
  hour INTEGER NOT NULL, 
  day INTEGER NOT NULL, 
  week INTEGER NOT NULL, 
  month INTEGER NOT NULL, 
  year INTEGER NOT NULL, 
  weekday varchar NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = f"""
COPY events_staging FROM 's3://udacity-dend/log_data' 
  credentials 'aws_iam_role={ARN_ROLE}'
  REGION 'us-west-2' 
  json 'auto ignorecase';
"""

staging_songs_copy = f"""
COPY songs_staging FROM 's3://udacity-dend/song_data' 
  credentials 'aws_iam_role={ARN_ROLE}'
  REGION 'us-west-2'
  json 'auto ignorecase';
"""

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
  songplay_id, 
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    usert_agent
  )
  SELECT 
    song_id as songplay_id,
    start_time as name, --- PENDING
    userId as user_id,
    level,
    song_id,
    artist_id,
    sessionId as session_id,
    location,
    usert_agent
  FROM songs_staging
  INNER JOIN events_staging ON 
    events_staging.song = songs_staging.title AND  
    events_staging.artist = songs_staging.artist_name
  WHERE 
    events_staging.song iS NOT NULL AND
    events_staging.artist iS NOT NULL AND
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
  SELECT 
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender as gender,
    level as level
  FROM events_staging
  WHERE userId is not null
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
  SELECT 
    song_id, 
    title, 
    artist_id,
    year,
    duration    
  FROM songs_staging
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
  SELECT 
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
  FROM songs_staging
""")

time_table_insert = ("""
INSERT INTO time (
  date_key,
  start_time,
  hour,
  day,
  week,
  month,
  year,
  weekday
)
  SELECT 
    DISTINCT(TO_CHAR(ts :: DATE, 'yyyyMMDD')::integer) AS date_key,
       date(ts)                                           AS start_time,
       EXTRACT(year FROM ts)                              AS year,
       EXTRACT(quarter FROM ts)                           AS quarter,
       EXTRACT(month FROM ts)                             AS month,
       EXTRACT(day FROM ts)                               AS day,
       EXTRACT(week FROM ts)                              AS week,
       CASE WHEN EXTRACT(ISODOW FROM ts) IN (6, 7) THEN true ELSE false END AS is_weekend
  FROM events_staging
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
