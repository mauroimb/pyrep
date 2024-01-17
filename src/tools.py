from datetime import datetime
import pytz

import pandas as pd


DECIMALS = 10


def ora():
    return datetime.now().time().strftime('%Y-%m-%d %H-%M-%S')

columns = ['id', 'timestamp', 'type', 'info',
               'in', 'in_iso', 'out', 'out_iso', 'fee', 'fee_iso']
 

def timeFormat(date_string, input_timezone='UTC', input_format='%Y-%m-%d %H:%M:%S'):
    # Convert the input string to a datetime object in the specified timezone
    input_datetime = datetime.strptime(date_string, input_format)
    input_datetime = pytz.timezone(input_timezone).localize(input_datetime)

    # Convert the datetime object to UTC
    utc_datetime = input_datetime.astimezone(pytz.utc)

    # Convert the UTC datetime to a timestamp in milliseconds
    timestamp_millis = int((utc_datetime - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds() * 1000)

    return timestamp_millis


def formatTime(unix, timezone='UTC'):
    unix = unix / 1000
    datetime_obj = datetime.fromtimestamp(unix, tz=pytz.timezone(timezone))
    return datetime_obj.strftime('%Y-%m-%d %H:%M:%S %Z')


def arrange(txs):
    txs.reset_index(inplace=True)

    columns = ['id', 'timestamp', 'type', 'info',
               'in', 'in_iso', 'out', 'out_iso', 'fee', 'fee_iso']
    # id = transaction id internal of the recipient
    # timestamp = unix in ms, int
    # type = 'ext'/'trade'/'other'/'FIRST'    le tx 'FIRST' servono ad aggiustare il bilancio
    #                                                        se non si possiedono le tx iniziali
    # info = information about the tx, like external wallet address, or similar
    # in, out, fee = float64
    # in_iso out_iso, fee_iso =  string representing the iso code of the currenc involved

    # check that all the columns are in place
    if not set(columns).issubset(txs.columns):
        diff = set(columns) - set(txs.columns)
        raise Exception('wrong columns \n'+str(txs.columns) +
                        '\n instead of \n'+str(columns) + '\n missing: '+str(diff))

    if not set(txs['type'].unique()).issubset(set(['ext', 'trade', 'other'])):
        raise Exception('wrong type' + str(txs['type'].unique()))

    # check that all the values are > 0
    has_negatives = (txs[['in', 'out', 'fee']] < 0).any().any()
    if has_negatives:
        raise Exception('DataFrame holds negative values: find them!')

    # verify that timestamps are  monotonically increasing
    mono = txs['timestamp'].is_monotonic_increasing
    if not mono:
        raise Exception('timestamps are not monotonic increasing')

    # verify that ids are unique
    if not txs['id'].is_unique:
        raise Exception('ids are not unique')

    # make 'id' the new index
    txs.set_index(['id'], inplace=True)

    # return the DataFrame only with the desired columns and in the right order
    return txs[columns[1:]]


def find_index(df, date):
    if (isinstance(date, str)):
        datetime_obj = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        timestamp = int(datetime_obj.timestamp())*1000
    if (isinstance(date, int)):
        timestamp = date
    index = df.searchsorted(timestamp, side='left')-1
   # print(timestamp, datetime.fromtimestamp(timestamp/1000), index)
    return index


def makeStrict(df):
    ti = 0

    if not df['timestamp'].is_monotonic_increasing:
        raise Exception('is not monotonic increasing')

    for index, row in df.iterrows():
        tii = row['timestamp']
        if (tii <= ti):
            ti += 1
            df.at[index, 'timestamp'] = ti
        else:
            ti = row['timestamp']
    return df


def concat(lista):
    all = pd.concat(lista, verify_integrity=True).sort_values(
            by=['timestamp'])
    return all


def makeBalances(txs):
    df = pd.DataFrame()
    df.index = txs.index

    currencies = set(txs['in_iso'].unique()).union(
        txs['out_iso'].unique(), txs['fee_iso'].unique())
    currencies = [x for x in currencies if pd.notna(x)]

    df[currencies] = 0

    for index, r in txs.iterrows():
        if (pd.notna(r['in'])):
            df.at[index, r['in_iso']] += r['in']
        if (pd.notna(r['out'])):
            df.at[index, r['out_iso']] -= r['out']
        if (pd.notna(r['fee'])):
            df.at[index, r['fee_iso']] -= r['fee']

    s = df.cumsum()
    s.insert(0, 'timestamp', txs['timestamp'], allow_duplicates=True)

    # return s
    return floor(s, DECIMALS)


def maxi(txs, balances, out_=False):
    date = pd.DataFrame()
    date.index = txs.index
    date['Date'] = txs['timestamp'].apply(formatTime)
    # date.set_index(['timestamp'], inplace=True)
    maxi = pd.concat([date, txs.drop(columns=['timestamp']),
                     balances.drop(columns=['timestamp'])], axis=1)
    if (out_):
        if out_ == True:
            maxi.to_excel('output.xlsx')
        else:
            maxi.to_excel(out_)
    return maxi



def floor(df, decimals):
    if not isinstance(decimals, int):
        raise Exception('wrong decimals number', str(decimals))

    delta = 1/10**decimals

    return df.applymap(lambda x: 0 if abs(x) < delta else x)

def checkFormat(date, date_format):
    #  # Specify the desired format
    #examples     '%Y-%m-%d %H:%M:%S'   "%Y-%m-%d" 
    try:
        # Attempt to parse the date string with the specified format
        parsed_date = datetime.strptime(date, date_format)
        #print("Date is in the specified format.")
    except ValueError:
        raise Exception("Date is not in the specified format.")
    return True

def date(df):
    assert isinstance(df, pd.DataFrame), 'wrong df'
    if 'timestamp' in df.columns:
        df['date'] = df['timestamp'].apply(formatTime)
    elif df.index.name == 'timestamp':
        df['date'] = df.index.map(formatTime)
    else:
        raise Exception('no timestamps')
    
    columns_order = ['date'] + [col for col in df.columns if col != 'date']

    return df[columns_order]

def purge(shot, threshold = 0):
    if isinstance(shot, pd.DataFrame):
        return shot[(~((shot < threshold) | shot.isna())).any(axis=1)]
    elif isinstance(shot, pd.Series):
        return shot[~((shot<=threshold) | shot.isna())]
    else:
        raise Exception('wrong shot type')
    
def _tx(df):
    assert isinstance(df, pd.DataFrame), 'wrong input'
   # if type(id) in [int, float, str]


def tx(df):
    assert isinstance(df, pd.DataFrame), 'wrong input'
    l = len(df)
    out = []
    if l == 0: return []
    for i, r in df.iterrows():
        out.append({
        'recipient': i,
        'timestamp':  r['timestamp'],
        'type':  r['type'],
        'info':  r['info'],
        'in':  r['in'],
        'in_iso':  r['in_iso'],
        'out':  r['out'],
        'out_iso':  r['out_iso'],
        'fee':  r['fee'],
        'fee_iso':  r['fee_iso']
    })
    return out




def txdate(df):
    assert isinstance(df, pd.DataFrame), 'wrong input'
    return formatTime(df['timestamp'].iloc[0])

def Tx(df):
        assert  isinstance(df, dict)
        sset = set(df.keys())
        assert sset.issubset( set(['recipient', *columns]))
        assert set(['id', 'timestamp','recipient', 'type']).issubset(sset)
        assert set(['in', 'in_iso']).issubset(sset) or set(['out', 'out_iso']).issubset(sset) or set(['fee', 'fee_iso']).issubset(sset) 
        assert type(df['id']) in [str, float, int, tuple]
        assert type(df['recipient'])== str
        assert type(df['timestamp'])== int
        assert type(df['info'])== str
        if 'in' in sset:
            assert type(df['in_iso'])== str
            assert type(df['in']) in [int, float]
        if 'out' in sset:
            assert type(df['out_iso'])== str
            assert type(df['out'])in [int, float]
        if 'fee' in sset:
            assert type(df['fee_iso'])== str
            assert type(df['fee'])in [int, float]
        obj = df 
        return obj

