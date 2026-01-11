from datetime import datetime
import os
import pandas as pd
import requests

CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
PLAYLIST_ID = '37i9dQZEVXbMDoHDwVN2tF'

def get_access_token():
    auth_url = 'https://accounts.spotify.com/api/token'
    response = requests.post(auth_url, data = {'grant_type': 'client_credentials'}, 
                             auth = (CLIENT_ID, CLIENT_SECRET))
    
    if response.status_code != 200:
        raise Exception(f"Authentication failed: {response.text}")
    
    return response.json()['access_token']

def extract_transform_load():
    token = get_access_token()
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://api.spotify.com/v1/playlists/{PLAYLIST_ID}"

    data = requests.get(url, headers = headers).json()
    rows = []
    curr_date = datetime.now.strftime('%Y-%m-%d')

    for rank, item in enumerate(data['tracks']['items'], start = 1):
        track = item['track']
        row = {'date': curr_date,
               'rank': rank,
               'song': track['name'],
               'artist': ", ".join([artist]['name'] for artist in track['artists']),
               'popularity': track['popularity'],
               'duration_min': round(track['duration'] / 60000, 2)}
        rows.append(row)

    df = pd.DataFrame(rows)
    fname = 'global_top_50.csv'
    df.to_csv(fname, mode = 'a', header = not os.path.isfile(fname), index = False)
    print(f"Data appended to {fname}")
    
    top_10 = df.head(10)[['rank', 'song', 'artist']]
    table = top_10.to_markdown(index = False)

    with open('LATEST_UPDATE.md', 'w') as f:
        f.write(f"# Daily Update: {curr_date}\n\n")
        f.write(table)

    print(f"Report updated in LATEST_UPDATE.md")

if __name__ == "__main__":
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Missing environment variables.")
    
    extract_transform_load()