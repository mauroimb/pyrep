from datetime import datetime

import pandas as pd

from src.tools import *
 

class Accountant:
    def __init__(self, db):
        self.db = db
        self.balances = {}
        self.rounding_precision = 0.00000001
 
    def calc_global(self):
        return makeBalances(self.db.txs())

    def calc_balances(self):
        bal = {}
        dfs = self.db.transactions()
        for a in dfs:
            bal[a] = makeBalances(dfs[a])
        return bal

    def calculate(self, arg=None):
        if (arg == None):
            self.balances = self.calc_balances()
        if (arg == 'global' or arg == None):
            self.balances['global'] = self.calc_global()
        elif arg in self.db.list:
            self.balances[arg] = makeBalances(self.db.txs(arg))
        else:
            raise Exception('wrong arg '+str(arg))
        
    def insert(self):
        for rec in self.db.list:
            assert rec in self.balances, 'balance unavailable for this recipient: '+rec
            self.db.insert_balances(rec, self.balances[rec])
        self.db.insert_balances('global', self.balances['global'])
            
    
    def setup(self):
        self.calculate()
        self.insert()
        self.integrity()


    def integrity(self):
        # rifare questo test, controllare i bilanci
        if not bool(self.balances):
            self.calculate()
        act_balances = self.balances
        db_balances = self.db.balances()

        for table in self.db.list:
            act = act_balances[table]
            db = db_balances[table]
            col1 = act.columns
            col2 = db.columns
            assert set(col1) == set(col2)
            assert act[col2].equals(db)

        print('test OK')

    def purge(self,shot):
        return shot[(~((shot == 0) | shot.isna())).any(axis=1)].drop('recipients').drop('timestamp')

    def shot(self, data):
        # if not bool(self.balances):
        #     self.calculate()
        # balances = self.balances
        balances = self.db.balances()
        shot = {}
        for rec in balances:
            serie = balances[rec]['timestamp']
            index = find_index(serie, data)
            if (index == - 1):
                pass
            else:
                shot[rec] = balances[rec].iloc[index]
        s = pd.concat(shot, axis=1)
        assert self.test_shot(s) 
        #print('test passed!')
        return self.purge(s)

    def floor(self, x):
        if abs(x) < self.rounding_precision:
            return True

    def test_shot(self, shot):
        if not 'global' in shot:
            raise Exception('wrong shot, non global')
        col = shot.columns != 'global'

        df1 = shot.loc[:, col].drop(['timestamp', 'recipients']).sum(axis=1)
        df2 = shot['global'].drop(['timestamp', 'recipients'])

        test = (df1-df2).map(self.floor).all()

        assert test
        return test
