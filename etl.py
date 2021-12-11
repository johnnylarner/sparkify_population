import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from env_settings import get_env_vars


def process_song_file(cur, filepath):
    """
    Reads json file as dataframe and inserts into song and artists tables.

    Args:
    - cur: psycopy2 cursor object with connection to sparkifydb.
    - filepath: str leading to json song file.

    Raises:
    - KeyError if mandatory columns are missing from json data.
    """

    song_cols = [
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ]

    artist_cols = [
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ]

    # open song file
    df = pd.read_json(filepath, lines=True)

    # Extract first row and export as list
    try:
        np_song_data = df[song_cols].iloc[0].to_numpy()

    except KeyError as e: # If json is missing mandatory col
        print("Song json missing column")
        print(e)
        return None
    
    # Convert numpy data types to native ones
    py_song_data = []
    for np_val in np_song_data:
        try:
            py_val = np_val.item() # https://stackoverflow.com/questions/9452775/converting-numpy-dtypes-to-native-python-types/11389998#11389998
        except AttributeError: # if native type, do nothing
            py_val = np_val

        py_song_data.append(py_val)


    cur.execute(song_table_insert, py_song_data)

    # insert artist record
    try:
        artist_data = df[artist_cols].iloc[0].to_numpy().tolist()

    except KeyError as e:
        print("Song json missing column")
        print(e)
        return None

    cur.execute(artist_table_insert, artist_data)

def clean_song_year(cur):
    """
    Nullifies year for songs with year less than 1000.

    Args:
    - cur: psycopy2 cursor object with connection to sparkifydb.
    """

    # Select & nullify song years

    cur.execute(nullify_song_year)

    updated_rows = cur.rowcount
    print("{} rows had their song years set to NULL.".format(updated_rows))


def process_log_file(cur, filepath):
    """
    
    """



    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong"]

    # Add date parts to DF
    df["sng_start_time"] = pd.to_datetime(df["ts"], unit="ms", yearfirst=True)
    df["hour"] = df["sng_start_time"].dt.hour
    df["day"] = df["sng_start_time"].dt.day
    df["week"] = df["sng_start_time"].dt.isocalendar().week.astype(int, errors="raise") # dt.week depreciated
    df["month"] = df["sng_start_time"].dt.month
    df["weekday"] = df["sng_start_time"].dt.weekday

    # insert time data records
    time_cols = [
        "sng_start_time",
        "hour",
        "day",
        "week",
        "month",
        "weekday"
    ]

    # Filter to time cols
    time_df = df[time_cols].loc[:]

    for row in time_df.itertuples(index=False, name=None):
        cur.execute(time_table_insert, row)

    # load user table
    user_cols = [
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level"
    ]

    user_df = df[user_cols].loc[:]

    # insert user records
    for row in user_df.itertuples(index=False):
        cur.execute(user_table_insert, row)


    # insert songplay records
    for row in df.itertuples(index=False):

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))

        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.sng_start_time, 
            row.userId, 
            row.level, 
            songid, 
            artistid, 
            row.sessionId,
            row.location,
            row.userAgent
            )

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():

    env_vars = get_env_vars()
    user = env_vars["DB_USER"]
    password = env_vars["DB_PASSWORD"]

    conn = psycopg2.connect(f"host=127.0.0.1 dbname=sparkifydb user={user} password={password}")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    clean_song_year(cur)

    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()