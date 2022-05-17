import datetime

import spotipy
import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
from spotipy.oauth2 import SpotifyOAuth

#Client ID 42381f5852bf4b24b88273a4663b9dec
#Client Secret 09b5643521724c7d9ff130d21d636be2
CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID");
CLIENT_SECRET_ID = os.getenv("SPOTIPY_CLIENT_SECRET");
print(CLIENT_ID);
print(CLIENT_SECRET_ID);

def spotify_extract_info():
    spotip_redirect_url = "http://localhost:8080"
    sp_connect = spotipy.Spotify(oauth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET_ID,
        redirect_uri=spotip_redirect_url,
        scope="user-read-recently-played"
    ))

    data = sp_connect.current_user_recently_played(limit=50)
    #print(data)
    if len(data) ==0:
        print("No data")
    else:
        #Album
        album_list = []

        for row in data['items']:
            albun_id = row['track']['album']['id']
            album_name =  row['track']['album']['name']
            album_url = row['track']['album']['external_urls']['spotify']
            album_release_date = row['track']['album']['release_date']
            album_total_tracks = row['track']['album']['total_tracks']

            element = {
                'albun_id':albun_id,
                'album_name':album_name,
                'album_url':album_url,
                'album_release_date':album_release_date,
                'album_total_tracks':album_total_tracks
            }

            album_list.append(element)
        #Artistas
        artist_dict ={}
        artist_list = []
        id_list = []
        name_list = []
        url_list = []

        for item in data['items']:
            for key,value in item.items():
                if key == "track":
                    for point in value['artists']:
                        data_point = point
                        id_list.append(point['id'])
                        name_list.append(point['name'])
                        url_list.append(point['external_urls']['spotify'])
        artist_dict = {
            'artist_id':id_list,
            'artist_name':name_list,
            'url_list':url_list
        }
        artist_list.append(artist_dict)

        #canciones
        #id,nombre,url,popularity,duration_ms, album_id, artist_id , played_at
        songs_dict = {}
        songs_list = []
        id_list_songs = []
        name_list_songs = []
        url_list_songs = []
        duration_ms_list_songs = []
        album_id_list = []
        artist_id_list = []
        played_at_list = []



        for item in data['items']:
            for key,value in item.items():
                if key == "track":

                    id_list_songs = value['id']
                    name_list_songs = value['name']
                    url_list_songs = value['popularity']
                    duration_ms_list_songs = value['duration_ms']
                    album_id_list = value['album']['id']
                    artist_id_list = value['artists']

                unico = str(id_list_songs) + str(name_list_songs) + str(url_list_songs) + str(duration_ms_list_songs) + str(album_id_list)

                songs_dict = {
                    'id_list_songs': id_list_songs,
                    'name_list_songs': name_list_songs,
                    'url_list_songs': url_list_songs,
                    'duration_ms_list_songs': duration_ms_list_songs,
                    'album_id_list' : album_id_list,
                    'artist_id_list' : artist_id_list,
                    'unico' : unico
                }


                songs_list.append(songs_dict)
        #Dataframes

        #Cargar los datos desde la entidad a un dataframe

        album_df = pd.DataFrame.from_dict(data=album_list)

        songs_df = pd.DataFrame.from_dict(data=songs_list)

        artist_df = pd.DataFrame.from_dict(data=artist_list)

        songs_df['load_data'] = datetime.datetime.now()
        songs_df['load_data'] = songs_df['load_data'].dt.strftime('%m/%d/%Y')

        #album_df = album_df.drop_duplicates(subset=['albun_id'])

        songs_df = songs_df.drop_duplicates(subset=['unico'])


        print(songs_df)

        #songs_df.to_svc('songs.xlsx', index=False, header=True)

        songs_df.to_excel('songs.xlsx', index=False, header=True)





spotify_extract_info()