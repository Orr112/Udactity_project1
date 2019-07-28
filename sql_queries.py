# DROP TABLES

songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"

# CREATE TABLES
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int Primary Key, start_time bigint NOT NULL , \
user_id int NOT NULL , level varchar, song_id varchar NOT NULL, artist_id varchar NOT NULL , session_id int, \
location varchar, user_agent varchar); """)


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int Primary Key, first_name varchar, last_name varchar, gender varchar, level varchar) ;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar Primary Key, title varchar, artist_id varchar NOT NULL , year int, duration int);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar Primary Key , name varchar, location varchar, lattitude int, longitude int) ;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour int, day varchar, week int, month int, weekday int);
""")





# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, \
user_agent)Values(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) Values(%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) Values(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude) Values(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week , month, weekday) values(%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""")

# FIND SONGS

song_select = ("""select s.song_id, a.artist_id, s.duration from songs s JOIN artists a ON s.artist_id = a.artist_id \
            WHERE s.title = %s and a.name = %s and s.duration = Round(%s)""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
