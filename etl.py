from datetime import datetime
import os
import pandas as pd
import requests

#Retrieves credentials from environment variables to avoid hardcoding secrets
API_KEY = os.environ.get('TMDB_API_KEY')

def extract_data():
    if not API_KEY:
        raise ValueError("Missing TMDB API Key.")

    url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={API_KEY}"
    response = requests.get(url)

    return response.json()

def transform_data(data, curr_date):
    rows = []

    #Flattens JSON response into a consistent schema for tabular storage
    for rank, item in enumerate(data['results'], start = 1):
        row = {'date': curr_date,
               'rank': rank,
               'title': item['title'],
               'popularity': item['popularity'],
               'vote_average': item['vote_average'],
               'release_date': item.get('release_date', 'N/A')}
        rows.append(row)

    return rows

def load_data(rows):
    df = pd.DataFrame(rows)
    fname = 'daily_movie_trends.csv'

    #Append mode 'a' enables incremental loading
    df.to_csv(fname, mode = 'a', header = not os.path.exists(fname), index = False)

    #Generates a static view for quick consumption
    top_5 = df.head(5)[['rank', 'title', 'vote_average']]
    top_5 = top_5.rename(columns = {'rank': 'Rank', 'title': 'Movie', 'vote_average': 'Rating'})
    top_5['Rating'] = top_5['Rating'].apply(lambda x: 'N/A' if x == 0.0 else x)
    
    with open('LATEST_UPDATE.md', 'w') as f:
        f.write(f"# Daily Movie Trends: {rows[0]['date']}\n\n")
        f.write(top_5.to_markdown(index = False, floatfmt = ".1f"))
    
if __name__ == "__main__":
    curr_date = datetime.now().strftime('%Y-%m-%d')
    raw_data = extract_data()
    clean_data = transform_data(raw_data, curr_date)
    load_data(clean_data)


