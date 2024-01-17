import requests
import os
import json
from datetime import datetime

# +  api endpoints:
#     [coingecko link](https://www.coingecko.com/api/documentation)

# + esempio price:
#     curl -X 'GET' \ 'https://api.coingecko.com/api/v3/coins/bitcoin/history?date=2017-12-31' \ -H 'accept: application/json'

path_conf_gecko = os.path.join(os.path.abspath(__file__)[:-len('prices.py')],'config', 'gecko.json')
with open(path_conf_gecko, 'r') as file:
    id = json.load(file)


def coins():
    url= 'https://api.coingecko.com/api/v3/coins/list?include_platform=true'
    headers = {'accept': 'application/json'}
    return requests.get(url, headers=headers).json()

def price(coin, date):
    assert type(coin)== str    
    if not coin in id:
        raise Exception('missing coin', coin)
    else:
        coin = id[coin]

    assert type(date)== str
    
   
    date = '-'.join(date.split('-')[::-1])
    
    url = f'https://api.coingecko.com/api/v3/coins/{coin}/history'
    params = {'date': date}
    headers = {'accept': 'application/json'}
    r = requests.get(url, params=params, headers=headers)
    
    if not r.status_code == 200:
        raise Exception(r.json())

    r = r.json()
    if 'error' in r:
        raise Exception(r)
    if 'market_data' in r:
        return r['market_data']['current_price']['eur']
    else:
        return None
        #raise Exception(r)

def dateFormat(date):
    date_format = "%Y-%m-%d"  # Specify the desired format
    try:
        # Attempt to parse the date string with the specified format
        parsed_date = datetime.strptime(date, date_format)
        #print("Date is in the specified format.")
    except ValueError:
        raise Exception("Date is not in the specified format.")
    return True


