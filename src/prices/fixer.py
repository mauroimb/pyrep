import requests
from datetime import datetime

api_key = 'INSERIRE LA PROPRIA API-KEY'

def retrieve(base, symbols,date):
    assert type(base)== str
    assert type(date) == str
    assert type(symbols) == list

    date_format = "%Y-%m-%d"  # Specify the desired format
    try:
        # Attempt to parse the date string with the specified format
        parsed_date = datetime.strptime(date, date_format)
        #print("Date is in the specified format.")
    except ValueError:
        raise Exception("Date is not in the specified format.")


    s = ''
    for c in symbols:
        s += c + ','
    s=s[:-1]
    # date format YYYY-MM-DD
    url = 'http://data.fixer.io/api/'+ date
    params = {
        'access_key': api_key,
        'base' : base,
        'symbols' :s
    }
    headers = {'accept': 'application/json'}

    r = requests.get(url, params=params, headers=headers)

    return r


def eur(date):
    j = retrieve('EUR', ['USD'], date).json()
    if j['success']:   return j['rates']['USD']
    else: raise Exception(j)

def usd(date):
    j = retrieve('USD', ['EUR'], date).json()
    if j['success']:   return j['rates']['USD']
    else: raise Exception(j)

