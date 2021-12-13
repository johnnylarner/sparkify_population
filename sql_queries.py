# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    start_time timestamp,
    user_id int,
    level varchar(4),
    song_id varchar(18),
    artist_id varchar(18),
    session_id int,
    location varchar,
    user_agent varchar
);  
""")

user_table_create = ("""

CREATE TABLE IF NOT EXISTS users (
    user_id int,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar(4)
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
    weekday varchar
    );

""")


# ADD ID COLUMNS FOR DELETING DUPLICATES

add_time_id = ("ALTER TABLE time ADD id int GENERATED ALWAYS AS IDENTITY;")
add_user_id = ("ALTER TABLE users ADD id int GENERATED ALWAYS AS IDENTITY;")
add_songplay_id = ("ALTER TABLE songplays ADD id int GENERATED ALWAYS AS IDENTITY;")
 
# IDENTIFY AND DELETE DUPLICATES
# https://stackoverflow.com/questions/18390574/how-to-delete-duplicate-rows-in-sql-server

remove_time_duplicates = ("""
WITH CTE AS(
   SELECT *,
    ROW_NUMBER()OVER(PARTITION BY start_time ORDER BY start_time) as rn
   FROM time
)
DELETE FROM time WHERE id in (SELECT id FROM CTE WHERE rn >1);
""")

remove_user_duplicates = ("""
WITH CTE AS(
   SELECT *,
    ROW_NUMBER()OVER(PARTITION BY user_id ORDER BY user_id) as rn
   FROM users
)
DELETE FROM users WHERE id in (SELECT id FROM CTE WHERE rn >1);
""")

remove_songplay_duplicates = ("""
WITH CTE AS(
   SELECT *,
    ROW_NUMBER()OVER(PARTITION BY 
        start_time, user_id, level, song_id, 
        artist_id, session_id, location, user_agent) as rn
   FROM songplays
)
DELETE FROM songplays WHERE id in (SELECT id FROM CTE WHERE rn >1);
""")


# ADD PK CONTRAINT AFTER DUPLICATES REMOVED

add_time_pk_constraint = ("""
ALTER TABLE time ADD PRIMARY KEY(start_time);
""")

add_user_pk_constraint = ("""
ALTER TABLE users ADD PRIMARY KEY(user_id);
""")

add_songplay_pk_constraint = ("""
ALTER TABLE songplays ADD PRIMARY KEY(id);
""")

# REMOVE ID COLUMNS AS PK CONTRAINTS ADDED
remove_time_id = ("ALTER TABLE time DROP id;")
remove_user_id = ("ALTER TABLE users DROP id;")

# RESET AUTO INCREMENT FOR SONGPLAYS
reset_songplay_id = ("ALTER SEQUENCE songplays_id_seq RESTART WITH 1")


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

song_select_all = ("""
SELECT s.song_id, s.title, a.artist_id, a.name, s.duration
FROM songs s
LEFT JOIN artists a
ON s.artist_id = a.artist_id;
""")

# SELECT DATA FOR CLEANING

nullify_song_year = ("""
UPDATE songs SET year = NULL
WHERE year < 1000;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]