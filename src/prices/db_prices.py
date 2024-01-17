import os
import json

import sqlite3
import pandas as pd

from src.tools import * 
import src.prices.gecko as gecko
import src.prices.fixer as fixer

class Db:
    def __init__(self, file):
        self.file = file
        self.path = os.path.join(os.path.abspath(__file__)[:-len('src/prices/db_prices.py')],'data', file)
        self.path_coins = os.path.join(os.path.abspath(__file__)[:-len('src/prices/db_prices.py')],'data', 'coins.json')
        self.conn = sqlite3.connect(self.path)
        self.cursor = self.conn.cursor()
        self._dataframe = None

        with open(self.path_coins, 'r') as file:
            self.coins = json.load(file)


    def create(self):
        table_name='prices'
        column_names = self.coins
        def sanitize_column_name(name):
            return f'[{name}]'
        columns_definition = ', '.join(f'{sanitize_column_name(col)} TEXT' for col in column_names)
        create_table_query = f"CREATE TABLE {table_name} (timestamp INTEGER PRIMARY KEY, {columns_definition});"        
        self.cursor.execute(create_table_query)
        self.conn.commit()   


    @property
    def dataframe(self):
        self._dataframe = pd.read_sql_query('SELECT * FROM prices', self.conn).sort_values(by='timestamp').set_index('timestamp')  
        return self._dataframe
    
    def save(self):
        assert isinstance(self._dataframe,pd.DataFrame), 'missing dataframe'
        self.dataframe.to_sql('prices', self.conn, if_exists='replace', index=True)
    
    def close(self):
        self.cursor.close()
        self.conn.close()

    def date_input(self, date):
        if type(date)==str:
            if checkFormat(date, "%Y-%m-%d"):
                date = date + ' 00:00:00'
            elif checkFormat(date,'%Y-%m-%d %H:%M:%S'):
                date = date
            else: 
                raise Exception('wrong time format')  
            return timeFormat(date)
        elif type(date)==int:
            return date
        else:
            raise Exception('wrong date input')

    # set/has/get/del
    def set(self, date, coin, price):
        assert coin in self.coins, 'coin not in portfolio'
        assert type(price) == float or type(price)== int, 'wrong type price'
        assert price >= 0, 'impossibile price'
        date = self.date_input(date)
        select_query = f"SELECT * FROM prices WHERE timestamp = "+str(date)
        output =  self.cursor.execute(select_query).fetchall()
        assert len(output) < 2
        if len(output) == 0:
            self.cursor.execute("INSERT INTO prices (timestamp, "+coin+") VALUES (?,?);", (date, price)).fetchall()
            self.conn.commit()
        else:
            update_query = "UPDATE prices SET "+coin+"="+str(price)+" WHERE timestamp="+str(date)
            self.cursor.execute(update_query)
            self.conn.commit()

    def has(self, date):
        date = self.date_input(date)
        select_query = f"SELECT * FROM prices WHERE timestamp = "+str(date)
        lista =  self.cursor.execute(select_query).fetchall()
        return False if len(lista)==0 else True 

    
    def get(self, date, coin=False):
        date = self.date_input(date)

        if coin !=  False:
            assert coin in self.coins
            out =  self.cursor.execute('SELECT "'+coin+'" FROM prices where timestamp='+str(date)).fetchall()
                
            assert len(out)<2
            if len(out)==1:
                if pd.notna(out[0][0]):
                    return float(out[0][0])
                else:
                    return None
            else:
                return None
            #return float(out[0][0]) if len(out)==1 else None
        else:
            out =  self.cursor.execute("SELECT * FROM prices where timestamp="+str(date)).fetchall()
            elenco = self.cursor.description
            output = {}
            for i, j in enumerate(elenco):
                if i>0:
                    if pd.notna(out[0][i]):
                        output[j[0]]=out[0][i]
            return output

    
    def delete(self, date, coin=False):
        date = self.date_input(date)
        if coin != False:
            assert coin in self.coins, 'coin not in portfolio'
            if self.has(date):
                self.cursor.execute("UPDATE prices SET "+coin+"=NULL where timestamp="+str(date))
                self.conn.commit()
            else:
                return True
        else:
            query = "DELETE FROM prices WHERE timestamp="+str(date)
            self.cursor.execute(query)
            self.conn.commit()








