import os
import sys
sys.path.append('../..')

import numpy as np
import pandas as pd

from src.tools import *



def wallet_list():
    current_directory = os.getcwd()
    subdirectory_path = os.path.join(current_directory, 'data')
    return [f for f in os.listdir(subdirectory_path)]


def get_dfs(wallet):
    df = {}
    cd = os.getcwd()
    df['tx'] = pd.read_csv(f'{cd}/data/{wallet}/export-{wallet}.csv')
    df['token'] = pd.read_csv(
        f'{cd}/data/{wallet}/export-address-token-{wallet}.csv')
    df['internal'] = pd.read_csv(
        f'{cd}/data/{wallet}/export-internal-tx-{wallet}.csv')
    df['address'] = wallet
    return df


def incluso(DF, df):
    res = True
    for index, row in df.iterrows():
        h = row['Txhash']
        if not (DF['Txhash'] == h).any():
            return False
    return True


def inside(DF, hash):
    return (DF['Txhash'] == hash).any()


def transactions(dfs):

    # setting index to hash and renaming dataframes
    def ind(df):
        df['hash'] = df['Txhash']
        return df.set_index('hash')
    tx = ind(dfs['tx'])
    token = ind(dfs['token'])
    internal = ind(dfs['internal'])
    address = dfs['address'].lower()


    #creating the final dataframe
    columns = ['timestamp','id', 'type', 'info',
               'in', 'in_iso', 'out', 'out_iso', 'fee', 'fee_iso']
    df = pd.DataFrame(columns=columns)


    #cicle over transactions
    for index, row in tx.iterrows():
        (h, method, in_, out, status) = (row['Txhash'], row['Method'], row['Value_IN(ETH)'], row['Value_OUT(ETH)'], row['Status'])
        df.loc[h] =  {
            'timestamp': row['UnixTimestamp']*1000, 
            'id': h,
            'type': 'ext',
            'info':''
        }
    
        if row['From'].lower() == address:
            df.loc[h, 'fee']=row['TxnFee(ETH)']
            df.loc[h, 'fee_iso']='ETH'
            df.loc[h, 'info']=row['To'].lower()
        elif row['To'].lower() == address:
            df.loc[h, 'info']=row['From'].lower()
        else:
            print(index, row)
            raise Exception('what kind of tx is this?')


        if not pd.isna(status):
            df.loc[h, 'info']=status
        elif ((in_ != 0) & (out != 0)):
            raise Exception('in e out diversi da zero', str(row))
        elif (in_ != 0 ):
            df.loc[h, 'in'] = in_
            df.loc[h, 'in_iso'] = 'ETH'
            if h in internal.index: raise Exception('eh?')
            if h in token.index: raise Exception('eh?')
        elif (out != 0):
            df.loc[h, 'out'] = out
            df.loc[h, 'out_iso'] = 'ETH'
            if h in internal.index: raise Exception('eh?')
            if h in token.index: 
                if token.loc[h, 'From'] == address:
                    # out non può essere un token, se già è ETH
                    raise Exception('eh?')
                else:
                    df.loc[h, 'in'] = token.loc[h, 'TokenValue']
                    df.loc[h, 'in_iso'] = token.loc[h, 'TokenSymbol']
            #print(row)
        else:
            # print(method)
            if h in internal.index:
                #print(row)
                df.loc[h, 'type'] = 'ext'
                df.loc[h, 'in'] = internal.loc[h, 'Value_IN(ETH)']
                df.loc[h, 'in_iso'] = 'ETH'
                df.loc[h, 'out'] = internal.loc[h, 'Value_OUT(ETH)']
                df.loc[h, 'out_iso'] = 'ETH'    
            elif h in token.index:
                df.loc[h, 'type'] = 'ext'
                # print('###########################TOKEN')
                # print(token.loc[h])
                # print(address, token.loc[h, 'From'])
                # print(address == token.loc[h, 'From'])
                if (token.loc[h, 'From'] == address):
                # print('+++ FROM')
                    df.loc[h, 'info'] = token.loc[h, 'To']
                    df.loc[h, 'out'] = token.loc[h, 'TokenValue']
                    df.loc[h, 'out_iso'] = token.loc[h, 'TokenSymbol']
                elif (token.loc[h, 'To'] == address):
                    # print('+++ TO')
                    df.loc[h, 'info'] = token.loc[h, 'From']
                    df.loc[h, 'in'] = token.loc[h, 'TokenValue']
                    df.loc[h, 'in_iso'] = token.loc[h, 'TokenSymbol']
                else: 
                    print(row)
                    print(token.loc[h])
                    raise Exception('e alur?')
            else:
                df.loc[h, 'type']='other'  

    for index, row in token.iterrows():
        if index not in df.index:
            if row['From'] == address:
                raise Exception(row)
            
            df.loc[index] =  {
            'timestamp': row['UnixTimestamp']*1000,
            'id': index,
            'type': 'ext',
            'info':row['From'],
            'in':row['TokenValue'],
            'in_iso':row['TokenSymbol']
            }

    for h, row in internal.iterrows():
        if h not in df.index:
            print(row)
            raise Exception('la suddetta transazione è stata trascurata')

    df.sort_values(by='timestamp', inplace=True)
    df = makeStrict(df)

    def rep(x):
        if type(x)==str:
            return x.replace(',','')
        else:
            return x

    df['in'] = df['in'].apply(rep)
    df['out'] = df['out'].apply(rep)
    df['fee'] = df['fee'].apply(rep)

    df[['in', 'out', 'fee']]=df[['in', 'out', 'fee']].apply(pd.to_numeric)

    return arrange(df)
