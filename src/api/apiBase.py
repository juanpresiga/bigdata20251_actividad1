import requests

def fetch_data_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()