from etl import transform_data

def test_transform_valid_data():
    test_json = {'results':
                    [{'title': "Test Movie",
                        'genre_ids': [1],
                        'popularity': 100.0,
                        'vote_average': 5.0,
                        'release_date': "1999-12-31"}]}
    
    res = transform_data(test_json, "2000-01-01")

    assert len(res) == 1
    assert res[0]['title'] == "Test Movie"
    assert res[0]['genre_ids'] == [1]
    assert res[0]['popularity'] == 100.0
    assert res[0]['vote_average'] == 5.0
    assert res[0]['release_date'] == "1999-12-31"

def test_transform_missing_data():
    test_json = {'results':
                    [{'title': "Test Movie",
                     'popularity': 100.0,
                     'vote_average': 5.0}]}
    
    res = transform_data(test_json, "2000-01-01")

    assert res[0]['title'] == "Test Movie"
    assert res[0]['release_date'] == "N/A"
