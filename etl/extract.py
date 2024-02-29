from config import URL

import requests
def get_data(country, date, hour):
    params = {'country': country, 'year': date.split('-')[0], 'month': date.split('-')[1], 'day': date.split('-')[2], 'hour': hour }
    try:
        response = requests.get(URL, params=params)
        response.raise_for_status()
        data = response.json()
        data.update(params)
        print(data)
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return None


if __name__ == "__main__":
    get_data('fr', '2023-01-01', '01')