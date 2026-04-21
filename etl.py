import boto3
from datetime import datetime
import os
import pandas as pd
import requests
from io import BytesIO

#Retrieves credentials from environment variables to avoid hardcoding secrets
API_KEY = os.environ.get('TMDB_API_KEY')
AWS_BUCKET = os.environ.get('AWS_BUCKET_NAME')

def extract_data():
    if not API_KEY:
        raise ValueError("Missing TMDB API Key.")

    url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={API_KEY}"
    response = requests.get(url)

    return response.json()

def transform_data(data, curr_date):
    rows = []

    #Flattens JSON response into a consistent schema for tabular storage
    for rank, item in enumerate(data['results'], start=1):
        row = {'date': curr_date,
               'rank': rank,
               'title': item['title'],
               'genre_ids': item['genre_ids'],
               'popularity': item['popularity'],
               'vote_average': item['vote_average'],
               'release_date': item.get('release_date', 'N/A')}
        rows.append(row)

    return rows

def load_data(rows, curr_date):
    df = pd.DataFrame(rows)
    parquet_buffer = BytesIO()

    df.to_parquet(parquet_buffer, index = False)

    ymd = curr_date.split('-')
    year = ymd[0]
    month = ymd[1]
    s3_key = f"cleaned_data/{year}/{month}/{curr_date}_movies.parquet"
    s3 = boto3.client('s3')

    s3.put_object(Bucket=AWS_BUCKET, Key=s3_key, Body=parquet_buffer.getvalue())

    #Generates a static view for quick consumption
    top_5 = df.head(5)[['rank', 'title', 'vote_average']]
    top_5 = top_5.rename(columns = {'rank': 'Rank', 'title': 'Movie', 'vote_average': 'Rating'})
    top_5['Rating'] = top_5['Rating'].astype(float)
    top_5['Rating'] = top_5['Rating'].apply(lambda x: 'N/A' if x == 0.0 else x)
    top_5 = top_5.round(1)
    
    with open('LATEST_UPDATE.md', 'w') as f:
        f.write(f"# Daily Movie Trends: {rows[0]['date']}\n\n")
        f.write(top_5.to_markdown(index=False))
    
if __name__ == "__main__":
    curr_date = datetime.now().strftime('%Y-%m-%d')
    raw_data = extract_data()
    clean_data = transform_data(raw_data, curr_date)
    load_data(clean_data, curr_date)



