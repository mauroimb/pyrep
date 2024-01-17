import sys
sys.path.append('../..')


from src.tools import *


timezone = 'Europe/Rome'


def transactions(file):

    tx = pd.read_csv(file)

    columns = ['timestamp', 'id', 'type', 'info',
               'in', 'in_iso', 'out', 'out_iso', 'fee', 'fee_iso']

    df = pd.DataFrame(columns=columns)
    df['id'] = tx['transaction_hash']
    df['timestamp'] = tx['timestamp'].apply(lambda x :timeFormat(x, input_timezone=timezone))
    df['type'] = 'ext'
    df['info'] = None

    for h, row in tx.iterrows():
        value = row['value']

        if value == 0:
            raise Exception('0 value tx ' + h)
        elif value > 0:
            df.loc[h, 'in'] = value
            df.loc[h, 'in_iso'] = 'BTC'
        elif value < 0:
            df.loc[h, 'out'] = -value-row['fee']
            df.loc[h, 'out_iso'] = 'BTC'
            df.loc[h, 'fee'] = row['fee']
            df.loc[h, 'fee_iso'] = 'BTC'

    df.sort_values(by='timestamp', inplace=True)
    df = makeStrict(df)

    return arrange(df)
