# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int GENERATED ALWAYS AS IDENTITY,
    start_time timestamp,
    user_id int,
    level varchar(4),
    song_id varchar(18), 
    artist_id varchar(18),
    session_id int,
    location varchar,
    user_agent varchar,
    PRIMARY KEY(songplay_id)
);  
""")

user_table_create = ("""

CREATE TABLE IF NOT EXISTS users (
    user_id int,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar(4),
    PRIMARY KEY(user_id)
);

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar(18),
    title varchar,
    artist_id varchar(18),
    year int,
    duration float,
    PRIMARY KEY(song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar(18),
    name varchar,
    location varchar,
    latitude float,
    longitude float,
    PRIMARY KEY(artist_id)
);
""")

# Maybe add FK here
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp,
    hour int,
    day int,
    week int,
    month int,
    weekday varchar,
    PRIMARY KEY(start_time)
);

""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT
DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO NOTHING;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    weekday
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time)
DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id
FROM songs s
LEFT JOIN artists a
ON s.artist_id = a.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s
"""
)

# SELECT DATA FOR CLEANING

nullify_song_year = ("""
UPDATE songs SET year = NULL
WHERE year < 1000;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]