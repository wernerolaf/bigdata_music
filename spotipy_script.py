#!/usr/bin/python3

import requests
import json
import os
from pathlib import Path

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from kafka import KafkaProducer
from kafka.errors import KafkaError

from credentials import client_id, client_secret

auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(auth_manager=auth_manager)

producer = KafkaProducer(bootstrap_servers=['sandbox.hortonworks.com:6667'], client_id='group1')

# PLAYLIST_IDS_PATH = Path('playlists.txt')
# RESULTS_PATH = Path('spotify_data_chunk.json')
# ARTIST_IDS_PATH = Path('spotify_ids_chunk.txt')

PLAYLIST_IDS_PATH = 'playlists.txt'
RESULTS_PATH = 'spotify_data_chunk.json'
ARTIST_IDS_PATH = 'spotify_ids_chunk.txt'

def get_hdfs(file_name):
    response = requests.get("http://localhost:50070/webhdfs/v1/user/bigdata_music/" + file_name + "?user.name=hdfs&op=OPEN")
    
    if response.status_code == 200:
        return response.text
    else:
        return None

def delete_hdfs(file_name):
    response = requests.delete("http://localhost:50070/webhdfs/v1/user/bigdata_music/" + file_name + "?user.name=hdfs&op=DELETE")
    return response

def put_hdfs(data_dict, file_name):
    data = json.dumps(data_dict)
    response = requests.put("http://localhost:50070/webhdfs/v1/user/bigdata_music/" + file_name + "?user.name=hdfs&op=CREATE", data=data)
    return response

def divide_chunks(l, n): 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 

def get_artists_ids_from_playlist(playlist_id):
    """
    playlist_id: spotify:playlist:37i9dQZF1DX1ShvsspLokt
    """
    playlists_items = sp.playlist_items(playlist_id)
    ids = [artist['id'] for item in playlists_items['items'] for artist in item['track']['artists']]
    
    return ids

def get_artists(ids):
    artists_all = {}
    for ids_chunk in list(divide_chunks(ids, 49)):
        artists = sp.artists(ids_chunk)
        if artists['artists'] is not None:
            artists_all.update({
                artist['id']: artist for artist in artists['artists']
            })
    return artists_all

#------------------------------------------------------------------------------------------------------

ps = get_hdfs(PLAYLIST_IDS_PATH)

if ps:
    ps = [p.strip() for p in ps.split(',')]

    delete_hdfs(PLAYLIST_IDS_PATH)

    artist_ids = []
    for p in ps:
        artist_ids.extend(get_artists_ids_from_playlist(p))
        
    for a_id in artist_ids:
        future = producer.send('spotify_ids', a_id.encode())

    artists = get_artists(list(set(artist_ids)))
    
    if artists:
        for a_id, a_data in artists.items():
            future = producer.send('spotify', json.dumps(a_data).encode())
    
#     if artists:
#         put_hdfs(json.dumps(artists), RESULTS_PATH)

#         put_hdfs(', '.join(artist_ids), ARTIST_IDS_PATH)
