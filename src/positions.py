import sys
sys.path.append('..')
sys.path.append('../..')

import pandas as pd


from src.tools import *
from src.database import Database

db1 = Database('db1.db')


class _Positions(pd.DataFrame):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self['cgain']=pd.to_numeric(self['cgain'].apply(lambda x: 0 if x == 'unknown' else x))
            self['close_price']=pd.to_numeric(self['close_price'].apply(lambda x: 0 if x == 'unknown' else x))

        @property
        def _constructor(self):
            return _Positions

        @property
        def coins(self):
            return self['coin'].unique().tolist()

        def coin(self, c):
            assert c in self.coins, 'wrong coin'
            return self[self['coin']==c]
        
        def opened_between(self, start, stop):
            return self.select('open_t', start, stop) 
        
        def closed_between(self, start, stop):
            return self.select('close_t', start, stop) 
        
        def select(self, openclose, start, stop):
            assert type(start) == str
            assert type(stop) == str
            if len(start) == 10: start += ' 00:00:00'
            if len(stop) == 10: stop += ' 00:00:00'
            checkFormat(start, '%Y-%m-%d %H:%M:%S')
            checkFormat(stop, '%Y-%m-%d %H:%M:%S')
            start = timeFormat(start)
            stop =timeFormat(stop)
            return self[((self[openclose]>= start) & (self[openclose] <= stop))]


        def closed_by(self,id):
            return self[self['close_id']==id]
        def opened_by(self,id):
            return self[self['open_id']==id]
        def active(self):
            return self[self['close_id'].isna()]
        
        def balance(self, coin):
            assert coin in self.coins, 'wrong coin'
            return self.coin(coin).active()['amount'].sum()

        
        @property
        def gain(self):
            return self['cgain'].sum()
        @property
        def tax(self):
            return (self.gain)*0.26
    


class Positions(_Positions):
        def __init__(self, db, name='positions'):
            super().__init__(pd.read_sql('SELECT * FROM '+name, db.conn))
            
            assert type(name) == str
            assert isinstance(db, Database)

            self['cgain']=pd.to_numeric(self['cgain'].apply(lambda x: 0 if x == 'unknown' else x))
            self['close_price']=pd.to_numeric(self['close_price'].apply(lambda x: 0 if x == 'unknown' else x))

            self._metadata = ['name', 'db','txs']
            self.name = name
            self.db=db
            self.txs = self.db.read('valorized')


        
        def show(self,i):
            posi = self.loc[i]
            print('closed ', posi['close_amount'], posi['coin'], 'at', formatTime(posi['close_t']), 'with gain:', posi['cgain'])
            idopen = self.loc[i, 'open_id']
            idclose = self.loc[i, 'close_id']
            txs = self.txs
            uno = txs[txs['id']==idopen]
            due = txs[txs['id']==idclose]
            return date(pd.concat([uno, due]))
             

