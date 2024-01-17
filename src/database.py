import os
from datetime import datetime
import json
 
import sqlite3

from src.tools import *



class Database:
    # table naming convention:
    # recipient_balances, recipient_txs, ...
    txs_column_list =  ['id', 'timestamp', 'type', 'info', 'in', 'in_iso', 'out', 'out_iso', 'fee', 'fee_iso']

    def __init__(self, file):
        self.path = os.path.join(os.path.abspath(__file__)[:-len('src/database.py')],'data', file)
        self.file = file
        self.conn = sqlite3.connect(self.path)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def insert_txs(self, name, df):
        # name = of the recipient
        assert type(name)==str
        df.to_sql(name+'_txs', self.conn, if_exists='replace', index=True)

    def insert_balances(self, name, df):
        # name = of the recipient
        assert type(name)==str
        df.to_sql(name+'_balances', self.conn, if_exists='replace', index=True)

    def save(self, name, df):
        # name = of the recipient
        assert type(name)==str
        df.to_sql(name, self.conn, if_exists='replace', index=True)

    def read(self, name):
        assert type(name) == str
        return pd.read_sql_query(
                    'SELECT * FROM "'+name+'"', self.conn)
    

    def delete(self, table_name):
        self.cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        self.conn.commit()

    def cancel(self, recipient):
        assert type(recipient) == str
        lista = self.list
        if not recipient in lista:
            raise Exception(recipient + ' not in database')
        self.cursor.execute(f'DROP TABLE IF EXISTS "{recipient}_txs"')
        self.cursor.execute(f'DROP TABLE IF EXISTS "{recipient}_balances"')
        self.conn.commit()
        lista = self.list
        test = recipient in lista
        assert not recipient in lista
        return True
    

    @property
    def list(self):
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()
        count = {}
        for table in tables:
            table = table[0].split('_')[0]
            if table != 'global':
                if table in count:
                    count[table] += 1
                else:
                    count[table] = 0

        for table in count:
            if count[table] != 1:
                raise Exception('wrong table '+str(table) +
                                ' (or maybe has no balance or txs)')

        return [table for table in count]
    
    @property
    def tables(self):
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()
        return tables
    
    def coins(self, dump=False):
        l = list(self.balances('global'))[2:]
        if dump==True:
            path=os.path.join(os.path.abspath(__file__)[:-len('src/database.py')],'data', 'coins.json')
            with open(path, "w") as file:
                json.dump(l, file)
        return l
            
    @staticmethod
    def between_days(df, start, stop):
        checkFormat(start,"%Y-%m-%d")
        checkFormat(stop,"%Y-%m-%d")
        start = timeFormat(start+' 00:00:00')
        stop = timeFormat(stop+' 23:59:59')
        mask = (df['timestamp']>start) & (df['timestamp']<stop)
        return df[mask]
    
    def txs(self, start=False, stop=False, name=False):
        if name!= False:
            assert type(name)==str
            all = pd.read_sql_query('SELECT * FROM "'+name+'_txs"', self.conn, index_col='id')
        else:
            frames = self.transactions()
            all =  concat(frames).rename_axis(['recipients', 'id']).swaplevel()
            all.sort_values(by='timestamp', inplace=True)

        if (not start) & (not stop):
            return all
        else:
            if (type(start) == str and type(stop) == str):
                return self.between_days(all, start, stop)
            else:
                raise Exception('missing start or stop value')
            
    def _transactions(self, name):
        assert type(name) == str

        df = pd.read_sql_query(
                    'SELECT * FROM "'+name+'_txs"', self.conn, index_col='id')
        df['in'] = pd.to_numeric(df['in'])
        df['out'] = pd.to_numeric(df['out'])
        df['fee'] = pd.to_numeric(df['fee'])
        return df

    def transactions(self, name=False):
        if name != False:
            return self._transactions(name)
        else:
            dfs = {}
            tables = self.list
            for table in tables:
                dfs[table] = self._transactions(table)
            return dfs
  
    def balances(self, recipient=False):
        if recipient != False:
            return pd.read_sql_query(
                    'SELECT * FROM "'+recipient+'_balances"', self.conn, index_col='id')
            
        else:
            dfs = {}
            tables = self.list
            for table in tables:
                dfs[table] = pd.read_sql_query(
                    'SELECT * FROM "'+table+'_balances"', self.conn, index_col='id')
                dfs[table] = dfs[table].applymap(pd.to_numeric)

            dfs['global'] = pd.read_sql_query(
                'SELECT * FROM "global_balances"', self.conn, index_col='id')
            dfs['global'] = dfs['global']

            return dfs
        
    def token(self, iso):
        assert type(iso)== str, 'wrong iso'
        assert iso in self.coins(), 'unavailable coin'
        txs = self.txs()
        mask = (txs['in_iso'] == iso) | (txs['out_iso']==iso)        
        return txs[mask].copy()
    
    def find_token(self, string):
        assert type(string) == str, 'wrong type'
        coins = self.coins()
        lista = []
        for c in coins:        
            if string in c:
                lista.append(c)
        return lista 

    
    def filter_iso(self, iso, txs):
        assert type(iso)== str, 'wrong iso'
        assert iso in self.coins(), 'unavailable coin'
        mask = (txs['in_iso'] == iso) | (txs['out_iso']==iso)        
        return txs[mask].copy()
    
    def filter_type(self, tipo, txs):
        assert type(tipo)== str, 'wrong type'
        assert tipo in ['ext', 'trade', 'other'], 'wrong type'
        mask = txs['type'] == tipo
        return txs[mask].copy()
    
    # operazioni sulla singola transazione
    def tx(self, id):
        r = self.txs().loc[id]
        return tx(r)
    
    def tx_modify(self, id, recipient, mapper):
        assert recipient in self.list, 'wrong recipient'
        assert isinstance(mapper, dict) and bool(mapper), 'wrong mapper'
        x = self.cursor.execute('SELECT * FROM "'+recipient+'_txs" WHERE id="'+str(id)+'"')
        entry = x.fetchall()
        assert len(entry) < 2, 'what kind of tx is this?'
        assert len(entry) != 0, 'missing tx'
        update_query = 'UPDATE "'+recipient+'_txs"  SET '
        for k in mapper:
            if k in ['id','info', 'type', 'in_iso', 'out_iso', 'fee_iso']:
                update_query += '"'+ str(k)+'"='
                update_query += '"'+str(mapper[k])+'",'
            elif k in ['timestamp', 'in','out','fee']:
                update_query += '"'+ str(k)+'"='
                update_query += str(mapper[k])+','

        update_query=update_query[:-1]
        update_query += ' WHERE id="'+id+'"'
        self.cursor.execute(update_query)
        self.conn.commit()

        if self.cursor.rowcount > 0:
            return 
        else:
            raise Exception('no rows updated')
        
    def tx_insert(self, tx):
        tx = Tx(tx)
        assert tx['recipient'] in self.list, 'wrong recipient'
        x = self.cursor.execute('SELECT * FROM "'+tx['recipient']+'_txs" WHERE id="'+str(tx['id'])+'"')
        entry = x.fetchall()
     
        assert len(entry) < 2, 'what kind of tx is this?'
        if len(entry) == 1:
            return self.tx_modify(tx['id'], tx['recipient'], tx)


        updatequery = 'INSERT INTO "'+ tx['recipient']+'_txs" ('
        col = ''
        values = ''
        for k,v in tx.items():
            if k in ['id', 'info', 'type', 'in_iso', 'out_iso', 'fee_iso']:
                col +='"'+ k+'",'
                values += '"'+str(v)+'",'
            elif k in ['timestamp', 'in','out','fee']:
                col +='"'+ k+'",'
                values += str(v) + ','
        col = col[:-1]
        values = values[:-1]
        updatequery = updatequery + col +') VALUES ('+values+')'
        self.cursor.execute(updatequery)
        self.conn.commit()

    def tx_cancel(self, id, recipient):
        assert type(id) in [str, int, float]
        assert recipient in self.list
        deletequery = 'DELETE FROM "'+ recipient+'_txs" WHERE id = "'+ id+'"'
        self.cursor.execute(deletequery)
        self.conn.commit()
