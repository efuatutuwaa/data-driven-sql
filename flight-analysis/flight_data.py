# importing libraries
import requests

# retrieving flight data
url = 'https://api.aviationstack.com/v1/flights?access_key=cf1f52d1a8d8209e9c27eee6eafc994d'
response = requests.get(url)

# Checking for a successful response
if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Failed to retrieve data. HTTP status code: {response.status_code}")

