 #This example uses Python 2.7 and the python-request library.

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

url = 'https://pro-api.coinmarketcap.com/v1/exchange/map'
parameters = {
  'slug':'binance'
}
headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': '6bba5ca9-e83a-4712-821b-bc8d76f1e91c',
}

session = Session()
session.headers.update(headers)

try:
  response = session.get(url, params=parameters)
  data = json.loads(response.text)
  print(data)
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)