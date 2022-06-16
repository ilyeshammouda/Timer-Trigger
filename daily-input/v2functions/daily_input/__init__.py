import datetime
import logging
import requests
import datetime
from fileinput import filename
import pandas as pd
import sqlalchemy
from sqlalchemy.engine import URL
SERVER = 'como-trading.database.windows.net'
DATABASE = 'como-risk'
USERNAME = 'como-admin'
PASSWORD = 'CRDpls-2041!'
DRIVER= '{ODBC Driver 17 for SQL Server}'


import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    main_deb()

def load_historical(instrument, start_date, end_date):
    """ get historical prices end of day 24:00 UTC  """

    start_time = start_date.timestamp()
    end_time = end_date.timestamp()

    ftx_address = 'https://ftx.com/api/markets/'
    data_format = '/candles?'
    resolution = 'resolution=86400'
    start =  '&start_time=' + str(start_time)
    end = '&end_time=' + str(end_time)
    request_string = ftx_address + instrument + data_format + resolution + start + end + ''

    historical = requests.get(request_string).json()
    #print('11111' , historical)
    if not historical['success']:
        print(historical['error'])
        return pd.DataFrame()
    else:
        historical = pd.DataFrame(historical['result'])
        #print('---------------------------------' , historical)
        historical.drop(['open'], axis = 1, inplace=True)
        historical.drop(['high'], axis = 1, inplace=True)
        historical.drop(['low'], axis = 1, inplace=True)
        historical.drop(['volume'], axis = 1, inplace=True)
        historical.drop(['startTime'], axis = 1, inplace=True)
        historical['Date'] =  pd.to_datetime(historical['time'].astype(int),unit='ms')
        historical['Instrument_Id'] = instrument
        historical.rename({'close': 'Price'}, axis=1, inplace=True)
        historical.drop(['time'], axis = 1, inplace=True)
    return historical

def insert_pricers_in_table_alchemy(price_table):
    """ inserts in database """
    connection_string = 'DRIVER='+DRIVER+';SERVER=tcp:'+SERVER+';PORT=1433;DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+ PASSWORD
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sqlalchemy.create_engine(connection_url)
    price_table.to_sql('daily_input1', con=engine,index=False, if_exists='append')


    # if mytimer.past_due:
    #     logging.info('The timer is past due!')
# Press the green button in the gutter to run the script.
def main_deb():
    # entry point, end_date should/will not be in the result
    today = datetime.datetime.today()
    start_date = datetime.datetime.now() - datetime.timedelta(days=1)
    end_date = datetime.datetime(today.year,today.month,today.day,today.hour,today.minute) + datetime.timedelta(days=1)
    # start_date =  datetime.datetime(today.year,today.month,today.day,today.hour,today.minute) - datetime.timedelta(minutes=20)
    # end_date = start_date + datetime.timedelta(minutes=20)
    print(start_date , end_date)
    instruments = ['BTC/USD','ETH/USD']
    #instruments = ['ETH/USD']
    for instrument in instruments:
        prices = load_historical(instrument, start_date,end_date)
        print(prices)
        insert_pricers_in_table_alchemy(prices)