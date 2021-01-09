#!/usr/bin/python3

import requests
import json
from pathlib import Path

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from credentials import client_id, client_secret

auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(auth_manager=auth_manager)

PLAYLIST_IDS_PATH = 'playlists.txt'
RESULTS_PATH = 'spotify_data_chunk.json'

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
        artists_all.update({
            artist['id']: artist for artist in artists['artists']
        })
    return artists_all


with open(Path(PLAYLIST_IDS_PATH)) as f:
    ps = f.read()
    ps = [p.strip() for p in ps.split(',')]
    
artist_ids = []
for p in ps:
    artist_ids.extend(get_artists_ids_from_playlist(p))

artists = get_artists(list(set(artist_ids)))

with open(Path(RESULTS_PATH), 'w') as f:
    json.dump(artists, f)