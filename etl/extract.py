from datetime import datetime, timedelta

import requests


def get_data(country, start_date_hour, end_date_hour):

    data_list = []

    start_date_hour = datetime.strptime(start_date_hour, "%Y-%m-%d-%H")
    end_date_hour = datetime.strptime(end_date_hour, "%Y-%m-%d-%H")

    while start_date_hour <= end_date_hour:

        params = {
            "country": country,
            "year": start_date_hour.year,
            "month": start_date_hour.month,
            "day": start_date_hour.day,
            "hour": start_date_hour.hour,
        }

        try:
            response = requests.get("https://ecommerce-data-generator.onrender.com", params=params)
            response.raise_for_status()
            data = response.json()
            data.update(params)
            data_list.append(data)
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            return None

        start_date_hour += timedelta(hours=1)
    
    return data_list
    

#if __name__ == "__main__":
#    get_data("fr", "2023-01-01-01", "2023-01-01-02")