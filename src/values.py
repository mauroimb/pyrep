
from datetime import datetime
import sys
import os


import pandas as pd


from src.tools import * 
from src.database import Database
from src.accountant import Accountant
from src.prices.prices import Price




class Values:
    def __init__(self, db):
        assert isinstance(db, Database)
        self.path_prices = os.path.join(os.path.abspath(__file__)[:-len('src/values.py')],'data', 'prices.xlsx')
        self.prices = pd.read_excel(self.path_prices, index_col='timestamp')
        self.db = db
        self.act = Accountant(db)
        self._price = Price()

    def price(self, coin, date):
        return self._price.price(coin, date)
    
    @staticmethod
    def purge(shot, threshold = 0):
        return shot[(~((shot < threshold) | shot.isna())).any(axis=1)]

    @staticmethod
    def find01(date_string):
        date_obj = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        year = date_obj.year
        return str(year)+'-01-01'

    def values(self, snapshot_date, prices_date):
        checkFormat(snapshot_date, "%Y-%m-%d %H:%M:%S")
        checkFormat(prices_date, "%Y-%m-%d")
        if not prices_date in self.prices.index:
            raise Exception('prices not available')

        shot = self.act.shot(snapshot_date)
        l = list(shot.index)
        b = self.prices[l].loc[prices_date]
        values = shot.multiply(b, axis=0)
        values.loc['sum']=values.sum()
 
        return (shot, values, values.loc['sum', 'global'])
    
    def giacenza(self, date):
        prices_date = self.find01(date)
        # print('portfolio at: ', date)
        # print('prices at:    ', prices_date)
        return self.values(date, prices_date)

    def giacenza_range(self, start, stop):
        checkFormat(start,  "%Y-%m-%d" )
        checkFormat(stop,  "%Y-%m-%d" )

        date = pd.date_range(start, stop)
        df = pd.DataFrame(index=date)

        df.index = df.index.to_series()
        df['values'] = df.index.to_series().apply(str).apply(lambda x:self.giacenza(x)[2])

        return df
    

    



    



