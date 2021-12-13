
# Sparkify DB population
This repository creates and populates a DB based on locally stored json files from the Million Song Dataset.

All SQL queries can be found in the sql_queries.py file. Table creation takes place in create_tables.py. The remaining etl functions are in etl.py. env_settings.py restricts ENV handling to one location.

## Summary
First, run create_tables.py to crop existing and create 5 tables: songplays (fact), songs, artists, users and time (dimension). This tables have a star stucture, where the songplays table is a fact table surrounded by 4 other dimension tables. Then execute etl.py to populate the sparkify DB with clean and queriable data.

## How the ETL works
The ETL process is based on the two JSON data formats provided by the Million Song Dataset: song_data and log_data.

### Build song and artist tables from song_data
The song and artist tables are based entirely on the song_data. To build these tables, we handle each individual song_data json file as a dataframe with one row. We filter according to the song and artist table requirements and INSERT each subrow. Duplicates do not replace existing records. See the process_song_file function in etl.py. Once all song and log files have been processed, the clean_song_year function sets song_year values under 1000 to NULL to improve data quality. To improve performance, one could consider cumulating each row in a dataframe and then copying all rows to the database. 

### Build time, user and songplay tables from log_data
Processing the log files required additional transformation. The time, users and songplays tables are primarily based on this data. When processing the log files we also map songplay events to a song and artist from the corresponding tables using a SELECT and pandas merge statements. For each log file found in the process_files function, we filter out irrelevant events, convert and denormalise the timestamp and then filter the dataset in accordance with the relevant table requirements and copy it to the db. To copy the data, we save it as CSV in a bufferstring. '|' must be used as a delimiter due to the punctuation issues in the location and user_agent fields. Once all data has been copied to the DB, we add primary keys (and remove duplicates). Note that COPY does not have a ON CONSTRAINT clause, meaning PKs and duplicates must be handled after insertion. This takes place within the add_pk.

## Installation
1. Clone repo
2. Install requirements as per the requirements.txt file.
3. Ensure you have an localhost instance of Postgres running on your machine called studentdb.
4. Add a .env file with the keys DB_USER and DB_PASSWORD to the repo root folder.
    i. These should allow you to connect to studentdb
5. Execute create_tables.py followed by etl.py
6. Have fun querying the sparkifydb!
