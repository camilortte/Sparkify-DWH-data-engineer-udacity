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
  ts VARCHAR,
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
  songplay_id INTEGER identity(0, 1) PRIMARY KEY, 
  start_time TIMESTAMP NOT NULL, 
  user_id INTEGER NOT NULL, 
  level VARCHAR, 
  song_id VARCHAR, 
  artist_id VARCHAR, 
  session_id INTEGER, 
  location VARCHAR, 
  usert_agent VARCHAR,

  CONSTRAINT fk_start_time
   FOREIGN KEY(start_time) 
      REFERENCES time(start_time),
  
  CONSTRAINT fk_user_id
   FOREIGN KEY(user_id) 
      REFERENCES users(user_id),
  
  CONSTRAINT fk_artist_id
   FOREIGN KEY(artist_id) 
      REFERENCES artists(artist_id),
  
  CONSTRAINT fk_song_id
   FOREIGN KEY(song_id) 
      REFERENCES songs(song_id)
);
""")

user_table_create = ("""
CREATE TABLE users 
(
  user_id      INTEGER PRIMARY KEY,
  first_name   VARCHAR,
  last_name    VARCHAR,
  gender       CHAR(1),
  level        VARCHAR(15) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs 
(
  song_id     VARCHAR PRIMARY KEY,
  title       VARCHAR NOT NULL,
  artist_id   VARCHAR NOT NULL,
  year        INTEGER,
  duration    NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR PRIMARY KEY, 
  name VARCHAR NOT NULL, 
  location VARCHAR, 
  latitude NUMERIC, 
  longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  start_time TIMESTAMP PRIMARY KEY, 
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
  FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
"""

staging_songs_copy = f"""
COPY songs_staging FROM 's3://udacity-dend/song_data' 
  credentials 'aws_iam_role={ARN_ROLE}'
  REGION 'us-west-2'
  json 'auto';
"""

# FINAL TABLES

songplay_table_insert = ("""
  INSERT INTO songplays (  
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
    (timestamp 'epoch' + events_staging.ts/1000 * interval '1 second') as start_time,
    events_staging.userId as user_id,
    events_staging.level,
    songs_staging.song_id,
    songs_staging.artist_id,
    events_staging.sessionId as session_id,
    songs_staging.artist_location,
    events_staging.userAgent as user_agent
  FROM songs_staging
  INNER JOIN events_staging ON 
    events_staging.song = songs_staging.title AND  
    events_staging.artist = songs_staging.artist_name
  WHERE 
    events_staging.song iS NOT NULL AND
    events_staging.artist iS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
  SELECT 
    DISTINCT userId as user_id,
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
    DISTINCT song_id, 
    title, 
    artist_id,
    year,
    duration    
  FROM songs_staging
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
  SELECT 
    DISTINCT artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
  FROM songs_staging
""")

time_table_insert = ("""
INSERT INTO time (
  start_time,
  hour,
  day,
  week,
  month,
  year,
  weekday
)
  SELECT 
       DISTINCT(timestamp 'epoch' + ts/1000 * interval '1 second') as start_time,
       EXTRACT(hour FROM start_time)                              AS hour,
       EXTRACT(day FROM start_time)                              AS day,
       EXTRACT(week FROM start_time)                              AS week,
       EXTRACT(month FROM start_time)                              AS month,
       EXTRACT(year FROM start_time)                              AS year,
       EXTRACT(weekday FROM start_time)                              AS weekday
  FROM events_staging
  WHERE ts is NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
