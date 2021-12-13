import os
import glob
import psycopg2
import pandas as pd
from io import StringIO
from sql_queries import *
from env_settings import get_env_vars

def copy_df_to_db(df, target_table, cur):
    """
    Copy csv written to buffer memory to sparkifydb.
    '|' used as delimiter to improve csv parsing stability.

    Args:
    - df: pandas DataFrame.
    - target_table: str containing target table in sparkifydb.
    - cur: psycopy2 cursor object with connection to sparkifydb.
    """

    buffer = StringIO() # Write to memory to avoid file handling
    df.to_csv(
        buffer, sep="|", 
        na_rep='null', 
        quotechar="'",  
        escapechar="|", 
        header=False, 
        index=False
    )
    buffer.seek(0)

    try:
        cur.copy_from(buffer, target_table, null='null', sep="|")

    except psycopg2.DatabaseError as e:
        raise e


def clean_song_year(cur, conn):
    """
    Nullifies year for songs with year less than 1000.

    Args:
    - cur: psycopy2 cursor object with connection to sparkifydb.
    - conn: psycopy2 conn object with connection to sparkifydb.
    """

    # Select & nullify song years

    try:
        cur.execute(nullify_song_year)
        conn.commit()
        updated_rows = cur.rowcount
    except psycopg2.Error as e:
        print(e)

    print("{} rows had their song years set to NULL.".format(updated_rows))

def add_time_pk(cur):
    """
    Remove duplicates and add PK for time table.

    Args:
    - cur: psycopy2 cursor object with connection to sparkifydb.
    """
    try:
        cur.execute(add_time_id)

    except psycopg2.Error as e:
        print(e)

    try:
        cur.execute(remove_time_duplicates)
        dpls = cur.rowcount
        print("{} duplicates removed from time table".format(dpls))

    except psycopg2.Error as e:
        print(e)

    try:
        cur.execute(add_time_pk_constraint)

    except psycopg2.Error as e:
        print(e)

    try:
        cur.execute(remove_time_id)

    except psycopg2.Error as e:
        print(e)

    print("PKs added for time table.")

def add_user_pk(cur):
    """
    Remove duplicates and add PK for user table.

    Args:
    - cur: psycopy2 cursor object with connection to sparkifydb.
    """
    try:
        cur.execute(add_user_id)

    except psycopg2.Error as e:
        print(e)

    try:
        cur.execute(remove_user_duplicates)
        dpls = cur.rowcount
        print("{} duplicates removed from user table".format(dpls))
    except psycopg2.Error as e:
        print(e)

    try:
        cur.execute(add_user_pk_constraint)

    except psycopg2.Error as e:
        print(e)

    try:
        cur.execute(remove_user_id)

    except psycopg2.Error as e:
        print(e)

    print("PKs added for user table.")

def add_songplay_pk(cur):
    """
    Remove duplicates and add PK for songplay table.

    Args:
    - cur: psycopy2 cursor object with connection to sparkifydb.
    """
    try:
        cur.execute(add_songplay_id)

    except psycopg2.Error as e:
        print(e)

    try:
        cur.execute(remove_songplay_duplicates)
        dpls = cur.rowcount
        print("{} duplicates removed from songplay table".format(dpls))
    except psycopg2.Error as e:
        print(e)

    try:
        cur.execute(add_songplay_pk_constraint)

    except psycopg2.Error as e:
        print(e)

    try:
        cur.execute(reset_songplay_id)

    except psycopg2.Error as e:
        print("pk", e)

    print("PKs added for songplays table.")

def add_pks(cur, conn):
    """
    Wrapper for adding PKs to songplay, time and user tables.

    For each table:
    - Add ids to faciliate row deletion
    - Use CTE to check for and remove duplicates in target PK columns.
    - Add PK contraint

    Additional steps:
    - time & user functions: remove id col as not PK
    - songplay function: keep and reset id column as PK

    Args:
    - cur: psycopy2 cursor object with connection to sparkifydb.
    - conn: psycopy2 conn object with connection to sparkifydb.
    """

    try:
        add_time_pk(cur)
        conn.commit()

    except psycopg2.Error as e:
        print(e)

    try:
        add_user_pk(cur)
        conn.commit()

    except psycopg2.Error as e:
        print(e)

    try:
        add_songplay_pk(cur)
        conn.commit()

    except psycopg2.Error as e:
        print(e)


def process_song_file(cur, filepath):
    """
    Reads json file as dataframe and inserts into songs and artists tables.

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

def process_log_file(cur, filepath):
    """
    Reads and copies data from log file into time, user and songplay tables.

    - Read in json and filter in pandas to relevant page type 'NextSong'.
    - Break timestamp into components in df.
    - Filter and copy respective datasets to db.

     Args:
    - cur: psycopy2 cursor object with connection to sparkifydb.
    - filepath: str leading to json song file.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df["page"] == "NextSong"]

    # Add date parts to DF
    df["start_time"] = pd.to_datetime(df["ts"], unit="ms", yearfirst=True)
    df["hour"] = df["start_time"].dt.hour
    df["day"] = df["start_time"].dt.day
    df["week"] = df["start_time"].dt.isocalendar().week.astype(int, errors="raise") # dt.week depreciated
    df["month"] = df["start_time"].dt.month
    df["weekday"] = df["start_time"].dt.weekday

    # Filter time cols and copy to db
    time_cols = [
        "start_time",
        "hour",
        "day",
        "week",
        "month",
        "weekday"
    ]

    time_df = df[time_cols].loc[:]
    copy_df_to_db(time_df, "time", cur)

    # Filter user cols and copy to db
    user_cols = [
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level"
    ]

    user_df = df[user_cols].loc[:]
    copy_df_to_db(user_df, "users", cur)

    # Get all song ids, artist ids and song durations
    cur.execute(song_select_all)
    results = cur.fetchall() 

    song_df = pd.DataFrame.from_records(
        data = results,
        columns=["song_id", "title", "artist_id", "name", "duration"]
    )

    df = df.merge(
        song_df,
        "left",
        left_on=["song", "artist", "length"],
        right_on=["title", "name", "duration"]
    )

    songplay_cols = [
        "start_time",
        "userId",
        "level",
        "song_id",
        "artist_id",
        "sessionId",
        "location",
        "userAgent"
    ]

    songplay_df = df[songplay_cols].loc[:]
    copy_df_to_db(songplay_df, "songplays", cur)

def process_data(cur, conn, filepath, func):
    """
    Traverse JSON files and pass to functions that write to sparkifydb.

    Args:
    - cur: psycopy2 cursor object with connection to sparkifydb.
    - conn: psycopy2 conn object with connection to sparkifydb.
    - filepath: str leading to json song file.
    - func: function either process_song_file or process_log_file.
    """

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
    """
    Populates sparkify db based on data in data folder.

    - Establish connection based on user variables in .ENV.
    - Populate database
    - Clean data and primary keys.
    """

    env_vars = get_env_vars()
    user = env_vars["DB_USER"]
    password = env_vars["DB_PASSWORD"]

    conn = psycopg2.connect(f"host=127.0.0.1 dbname=sparkifydb user={user} password={password}")
    cur = conn.cursor()

    # Populate database
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # Clean and finalise data
    clean_song_year(cur, conn)
    add_pks(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()