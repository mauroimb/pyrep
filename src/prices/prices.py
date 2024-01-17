import os
import json
import time

from src.tools import * 

import src.prices.gecko as gecko
import src.prices.fixer as fixer
from src.prices.db_prices import Db


path_conf_gecko = os.path.join(os.path.abspath(__file__)[:-len('prices.py')],'config', 'gecko.json')
path_conf_null = os.path.join(os.path.abspath(__file__)[:-len('prices.py')],'config', 'null.json')

with open(path_conf_gecko, 'r') as file:
    id = json.load(file)

with open(path_conf_null, 'r') as file:
    fixed = json.load(file)

class Price:
    def __init__(self, delay=5):
        self.db = Db('prices.db')
        self.delay = delay

    @property
    def dataframe(self):
        return self.db.dataframe
    
    @staticmethod
    def format(date):
        if type(date)==str:
            checkFormat(date, "%Y-%m-%d")
        elif type(date) == int:
            date = formatTime(date)[:10]
        return date
    
    def price(self, coin, date):
        #print('inside price', coin, date)
        assert type(coin) == str
        date = Price.format(date)

        if coin in ['USDT', 'BUSD', 'GUSD']:
            coin = 'USD'
        

        p = self.db.get(date, coin)
        
        if p is not None:
            return p
        else:
            if coin in fixed:
                return fixed[coin]
            elif coin in id:
                p = gecko.price(coin, date)
                print('online check', coin, date, p)
                time.sleep(self.delay)
                if (p is None):
                    p = 0
                self.db.set(date, coin, p)
                return p
            elif (coin == 'USD'):
                p =  1/fixer.eur(date) 
                time.sleep(self.delay)
                print('online check', coin, date, p)
                if (p>0):
                    self.db.set(date, coin, p)
                else:
                    raise Exception('usd price 0 ?')
                return p
            elif (coin == 'EUR'):
                return 1
            else:
                raise Exception('unavailable coin', coin)

