import pandas as pd
import requests
import json
from datetime import datetime
from datetime import timezone
import datetime
from secrets import secrets
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy
from refresh import Refresh


def valid_data_check(df) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution...")
        return False

    # Check Primary Keys
    if pd.Series(df['timestamp']).is_unique == False:
        raise Exception("Primary Key Check is violated. Duplicates exist.")

    # Check if Nulls exist
    if df.isnull().values.any():
        raise Exception("Null value(s) found")

    # Check all songs in dataframe have a timestamp from the last 24 hours
#    yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
#    print(yesterday)
#    for timestamp in timestamps:
#        if datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f") < yesterday:
#            raise Exception(
#                "One or more songs in this dataframe were not played within the last 24 hours")
#    return True


def call_refresh():
    print("Refreshing Token...")
    refreshCaller = Refresh()
    new_token = refreshCaller.refresh()
    return new_token


def run_spotify_etl():
    token = call_refresh()

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=token)
    }

    # Get unix timestamp for 24 hours ago

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix = int(yesterday.timestamp()) * 1000

    request = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix), headers=headers)

    data = request.json()

    song_names = []
    artist_names = []
    album_names = []
    timestamps = []
    dates = []

    #    print(data["items"][0]["track"]["album"]["name"])
    #    print(json.dumps(data, indent=2))

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        album_names.append(song["track"]["album"]["name"])
        if len(song["played_at"]) < 21:
            timestamps.append(
                (song["played_at"][:-1] + ".000000").replace("T", " "))
        else:
            timestamps.append(
                (song["played_at"][:-1] + "000").replace("T", " "))
        dates.append(song["played_at"][0:10])

    track_info = {
        "song_name": song_names,
        "artist_name": artist_names,
        "album_name": album_names,
        "timestamp": timestamps,
        "date": dates
    }

    track_df = pd.DataFrame(track_info, columns=[
                            "song_name", "artist_name", "album_name", "timestamp", "date"])

    print(track_df)

    # Validate
    if valid_data_check(track_df):
        print("Data valid, proceed to Load stage")

    # Load
    secret_pw = secrets.get('password')
    secret_user = secrets.get('username')
    secret_database = secrets.get('database')

    connect_string = "postgresql+psycopg2://" + secret_user + \
        ":"+secret_pw+"@"+"127.0.0.1:5432/SPOTIFYDB"

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        album_name VARCHAR(200),
        timestamp VARCHAR(200),
        date VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (timestamp)
    );
    """
    engine = sqlalchemy.create_engine(connect_string)
    conn = psycopg2.connect(dbname=secret_database,
                            user=secret_user, password=secret_pw, host="127.0.0.1", port="5432")
    cur = conn.cursor()

    cur.execute(sql_query)
    print("Opened database sucessfully")
    conn.commit()

    try:
        print("Attempting to append recent songs to table")
        track_df.to_sql("my_played_tracks", engine,
                        index=False, if_exists='append')
        print("Success! Data Successfully Loaded.")
        conn.commit()
        print("")
    except:
        print("Data already exists in the database")

    cur.close()
    conn.close()
