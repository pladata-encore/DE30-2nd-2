import requests
import pandas as pd
import datetime
import json

def call_playlist(playlist_id, country_name):
    # Client API ID, Password
    cid = "07fef1c8724f4753b8861f6d14d4facd" # removed for security
    secret = "175174566de64c148c00a783a4ce5e36" # removed for security
    
    # generating the access token for the spotify api
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_response = requests.post(auth_url, {'grant_type': 'client_credentials',
                                             'client_id': cid,
                                             'client_secret': secret})
    auth_response_data = auth_response.json()
    access_token = auth_response_data['access_token']
    
    headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}
    base_url = 'https://api.spotify.com/v1/'
    
    playlist_request = requests.get(base_url + 'playlists/' + playlist_id + '/tracks', headers=headers)
    playlist = playlist_request.json()['items']
    tracks = []
    
    for i, track in enumerate(playlist):
        daily_rank = i + 1
        track_name = track['track']['name']
        track_id = track['track']['id']
        artists = [artist['name'] for artist in track['track']['artists']]
        country = country_name
        snapshot_date = datetime.datetime.today().strftime("%Y-%m-%d")
        popularity = track['track']['popularity']
        explicit = track['track']['explicit']
        duration_ms = track['track']['duration_ms']
        album_name = track['track']['album']['name']
        album_date = track['track']['album']['release_date']
        tracks.append([track_id, track_name, artists, daily_rank, country, snapshot_date, popularity, explicit, duration_ms, album_name, album_date])
    
    tracks_columns = ['spotify_id', 'name', 'artists', 'daily_rank', 'country', 'snapshot_date', 'popularity', 'is_explicit', 'duration_ms', 'album_name', 'album_release_date']
    tracks_df = pd.DataFrame(tracks, columns=tracks_columns)
    
    all_track_ids = tracks_df['spotify_id'].to_list()
    track_ids = [t_id for t_id in all_track_ids if t_id is not None]
    
    features = []
    for t_id in track_ids:
        features_request = requests.get(base_url + 'audio-features/' + t_id, headers=headers)
        audio_features = features_request.json()
        features.append(audio_features)
    
    features_columns = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature']
    features_df = pd.DataFrame(features, columns=features_columns)
    
    playlist_df = pd.concat([tracks_df, features_df], axis=1)
    playlist_df['artists'] = playlist_df['artists'].apply(lambda x: ', '.join(x))
    
    return playlist_df