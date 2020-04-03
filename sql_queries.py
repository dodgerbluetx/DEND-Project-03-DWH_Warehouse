import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

"""
sample event data:

"artist": null,
"auth": "Logged In",
"firstName": "Walter",
"gender": "M",
"itemInSession": 0,
"lastName": "Frye",
"length": null,
"level": "free",
"location": "San Francisco-Oakland-Hayward, CA",
"method": "GET",
"page": "Home",
"registration": 1540919166796.0,
"sessionId": 38,
"song": null,
"status": 200,
"ts": 1541105830796,
"userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
"userId": "39"
"""

staging_events_table_create = ("""
    create table staging_events
    (
        artist varchar,
        auth varchar,
        first_name varchar,
        gender varchar,
        item_in_session int,
        last_name varchar,
        length double precision,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        session_id bigint,
        song_title varchar,
        status int,
        ts bigint,
        user_agent varchar,
        user_id varchar
    )
""")

"""
    Sample song data:

    "artist_id": "ARJNIUY12298900C91",
    "artist_latitude": null,
    "artist_location": "",
    "artist_longitude": null,
    "artist_name": "Adelitas Way",
    "duration": 213.9424,
    "num_songs": 1,
    "song_id": "SOBLFFE12AF72AA5BA",
    "title": "Scream",
    "year": 2009
"""

staging_songs_table_create = ("""
    create table staging_songs
    (
        song_id varchar,
        num_songs int,
        artist_id varchar,
        artist_latitude double precision,
        artist_longitude double precision,
        artist_location varchar,
        artist_name varchar,
        title varchar,
        duration double precision,
        year int
    )
""")

songplay_table_create = ("""
    create table songplays
    (
        songplay_id int identity(0,1) primary key,
        start_time timestamp,
        user_id varchar,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id bigint,
        location varchar,
        user_agent varchar
    )
""")

user_table_create = ("""
    create table users
    (
        user_id varchar primary key,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
""")

song_table_create = ("""
    create table songs
    (
        song_id varchar primary key,
        title varchar,
        artist_id varchar,
        year int,
        duration double precision
    )
""")

artist_table_create = ("""
    create table artists
    (
        artist_id varchar primary key,
        name varchar,
        location varchar,
        latitude double precision,
        longitude double precision
    )
""")

time_table_create = ("""
    create table time
    (
        start_time timestamp primary key,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    credentials 'aws_iam_role={}'
    json {}
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs
    from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
""").format(config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays
    (
        start_time, user_id, level, song_id, artist_id, session_id, location,
        user_agent
    )
    select
        timestamp 'epoch' + se.ts / 1000 * interval '1 second' as start_time,
        se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id,
        se.location, se.user_agent
    from staging_events se
    left join staging_songs ss
        on se.artist = ss.artist_name
        and se.song_title = ss.title
    where se.page = 'NextSong'
""")

user_table_insert = ("""
    insert into users
    (
        user_id, first_name, last_name, gender, level
    )
    select
        distinct user_id, first_name, last_name, gender, level
    from staging_events
    where page = 'NextSong'
""")

song_table_insert = ("""
    insert into songs
    (
        song_id, title, artist_id, year, duration
    )
    select
        song_id, title, artist_id, year, duration
    from staging_songs
""")

artist_table_insert = ("""
    insert into artists
    (
        artist_id, name, location, latitude, longitude
    )
    select
        distinct artist_id, artist_name, artist_location, artist_latitude,
        artist_longitude
    from staging_songs
""")

time_table_insert = ("""
    insert into time
    (
        start_time, hour, day, week, month, year, weekday
    )
    select
        dt, extract(hour from dt), extract(day from dt), extract(week from dt),
        extract(month from dt), extract(year from dt), extract(weekday from dt)
    from
    (
    	select
            distinct timestamp 'epoch' + se.ts / 1000 * interval '1 second' as dt,
            page
        from staging_events se
    )
    where page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]


insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
