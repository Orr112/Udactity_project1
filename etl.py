import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

pd.set_option('mode.chained_assignment', None)

def process_song_file(cur, filepath):
    """ This functions uses takes in postgress cursor parameter and song file path.
    The function converts data from song files into a dataframe and then extracts required columns \
    and inserts that data into the songs and artists tables respectively """

    """ open song file
       drop duplicates
       set NAs to Zero """
    df = pd.read_json(filepath, lines=True)
    df.drop_duplicates(subset=['song_id','artist_id'], keep = 'first')
    df['artist_latitude'] = df['artist_latitude'].fillna(0)
    df['artist_longitude'] = df['artist_longitude'].fillna(0)


    """ Extract columns for dataframe for song table
      drop duplicates before performing insert
     convert dataframe to a list for insert """

    song_data = (df[['song_id','title','artist_id','year','duration']])
    song_data.drop_duplicates(subset='song_id',keep ='first',inplace = True)
    song_data = (song_data.values).tolist()
    song_data = song_data[0]
    # insert song record
    cur.execute(song_table_insert,song_data)

    """ Extract columns for dataframe for artist table,
      drop duplicates before performing insert
     convert dataframe to a list for insert """

    artist_data = (df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']])
    artist_data.drop_duplicates(subset='artist_id',keep ='first',inplace = True)
    artist_data = (artist_data.values).tolist()
    artist_data = artist_data[0]
    # insert artist record
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ This functions takes in postgress cursor parameter and log file path.
     The function converts data from log files into a dataframe and then extracts required columns \
     and inserts that data into the users, time songplays tables """

    # open log file
    df = pd.read_json(filepath, lines=True)
    df2 = pd.read_json(filepath, lines=True)

    # filter by NextSong action for missing data
    df2 = df2[df2['page']=='NextSong']



    # insert missing records into Song and Artist Table
    for i, row in df2.iterrows():
        cur.execute(artist_table_insert, (row.artist + str(i), row.artist, row.location, 0, 0))
    for i, row in df2.iterrows():
        cur.execute(song_table_insert, (row.song + str(i), row.song, row.artist + str(i), 0, row.length))

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # extract time data from timestamp
    time_data = {'start_time': t,'hour': pd.Series(t).dt.hour, 'day':pd.Series(t).dt.day,
                 'month': pd.Series(t).dt.month, 'year': pd.Series(t).dt.year,
                 'weekday': pd.Series(t).dt.dayofweek}
    #column_labels = []
    # insert time data records
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName','lastName','gender','level']]
    user_df.drop_duplicates(subset='userId',keep ='first',inplace = True)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        print(cur.mogrify(song_select, (row.song, row.artist, row.length)))
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results[0],results[1]
        else:
            songid, artistid = "None" + str(index), "None" + str(index)

        # insert songplay record
        songplay_data = (df[['ts', 'userId', 'level', 'sessionId','location','userAgent' ]])
        songplay_data['ts'] = pd.to_datetime(df['ts'], unit='ms')
        cur.execute(songplay_table_insert, (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent))
        #conn.commit()

def process_data(cur, conn, filepath, func):
    """ This functions takes in postgress cursor and connection, file path directory \
    and appropiate funcion as parameters.
    The function creates a list, and then walks through the directory appending \
    the files to the list. After appending all files to the list -
    the function iterates over the list of files, performs the function provided and commits the data.  """

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
    try:
        for i, datafile in enumerate(all_files, 1):
            func(cur, datafile)
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))
    except Exception as e:
        print(e)

def main():

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.set_session(autocommit=True)


    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
