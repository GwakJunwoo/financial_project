import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pandas_datareader as pdr
import FinanceDataReader as fdr
import yfinance as yf
import investpy
import pykrx
import dart_fss as dart
from datetime import datetime
from openpyxl import Workbook, load_workbook
from typing import Optional, Dict, List, Tuple, Union
import pymysql.cursors
import time
from tqdm import tqdm


class Mydb:
    def __init__(self, host, user, db, password):
        self.conn = pymysql.connect(host=host, user=user, db=db, password=password)
        self.cursor = self.conn.cursor()

    def insert(self, table, value_list: Union[List, Tuple], column_list: Optional[Union[List, Tuple]] = None):
        try:
            if column_list is None:
                sql = "INSERT INTO {table} VALUES (" + ', '.join(value_list) + ")"
            else:
                sql = "INSERT INTO {table} (".format(table=table) + ', '.join(column_list) + ") VALUES (" \
                      + ', '.join(value_list) + ")"
            self.cursor.execute(sql)
            self.conn.commit()

        except Exception as e:
            print(e)

    def delete(self, table, condition):
        try:
            sql = "DELETE FROM {table} WHERE ".format(table=table) + condition
            self.cursor.execute(sql)
            self.conn.commit()

        except Exception as e:
            print(e)

    def query(self, query):
        try:
            self.cursor.execute(query)
            self.conn.commit()

        except Exception as e:
            print(e)

    def view(self, table, condition):
        try:
            sql = "SELECT * FROM {table} ".format(table=table) + "WHERE " + condition
            df = pd.read_sql(sql, self.conn)
            print(df)

        except Exception as e:
            print(e)

    def get_data(self, table, condition):
        try:
            sql = "SELECT * FROM {table} ".format(table=table) + "WHERE " + condition
            df = pd.read_sql(sql, self.conn)
            return df

        except Exception as e:
            print(e)

    def table_status(self):
        try:
            sql = "SHOW TABLE STATUS"
            return pd.read_sql(sql, self.conn)

        except Exception as e:
            print(e)

    def columns_describe(self, table):
        try:
            sql = "DESCRIBE " + table
            return pd.read_sql(sql, self.conn)

        except Exception as e:
            print(e)


class Crawler:
    def __init__(self, database: Mydb, data_vendor=None):
        self.tickers = None
        self.ticker_index = None

        self.VENDOR = data_vendor
        self.VENDOR_URL = ""
        self.VENDOR_ID: int
        self.source = None

        self.db = database

        if self.VENDOR == "Yahoo":
            self.VENDOR_URL = "https://finance.yahoo.com/"
            self.VENDOR_ID = 1

        elif self.VENDOR == "Investing":
            self.VENDOR_URL = "https://kr.investing.com/"
            self.VENDOR_ID = 3

        elif self.VENDOR == "Dart":
            self.VENDOR_URL = "https://dart.fss.or.kr/"
            self.VENDOR_ID = 5

        elif self.VENDOR == "FinanceDataReader":
            self.VENDOR_URL = "https://github.com/FinanceData/FinanceDataReader"
            self.VENDOR_ID = 2

        elif self.VENDOR == "pykrx":
            self.VENDOR_URL = "https://github.com/sharebook-kr/pykrx"
            self.VENDOR_ID = 4

        else:
            print('Unconfirmed source! Please check your data vendor!')

    def set_tickers(self, ticker_list: Optional[List] = None):
        if ticker_list is None:
            all_tickers = pd.read_sql("SELECT ticker, id FROM security", self.db.conn)
            self.ticker_index = dict(all_tickers.to_dict('split')['data'])
            self.tickers = list(self.ticker_index.keys())

        else:
            self.tickers = ticker_list

    def download_stock_data_chunk(self, start_idx, end_idx, tickers, start_date=None):
        ms_tickers = []

        for ticker in tickers[start_idx:end_idx]:
            if self.VENDOR_ID == 1:
                df = pdr.get_data_yahoo(ticker, start=start_date)

                if df.empty:
                    print(f"df is empty for {ticker}")
                    ms_tickers.append(ticker)
                    time.sleep(3)
                    continue

                for row in df.itertuples():
                    values = [self.VENDOR_ID, self.ticker_index[ticker]] + list(row)
                    self.db.insert('daily_price', values, ('data_vendor_id', 'ticker_id', 'price_date',
                                                           'open_price', 'high_price', 'low_price',
                                                           'close_price', 'adj_close_price', 'volume'))

            elif self.VENDOR_ID == 2:
                df = fdr.DataReader(ticker, start_date)

                if df.empty:
                    print(f"df is empty for {ticker}")
                    ms_tickers.append(ticker)
                    time.sleep(3)
                    continue

                for row in df.itertuples():
                    values = [self.VENDOR_ID, self.ticker_index[ticker]] + list(row)
                    self.db.insert('daily_price', values[:8], ('data_vendor_id', 'ticker_id', 'price_date',
                                                               'open_price', 'high_price', 'low_price',
                                                               'close_price', 'volume'))

            # TODO
            elif self.VENDOR_ID == 3:
                # Investing.com
                continue

            # TODO
            elif self.VENDOR_ID == 4:
                # pykrx
                continue

        return ms_tickers

    def download_stock_data(self, tickers, chunk_size=100, start_date=None):
        n_chunks = -(-len(tickers) // chunk_size)

        ms_tickers = []
        for i in tqdm(range(0, n_chunks, chunk_size)):
            ms_from_chunk = self.download_stock_data_chunk(i, i + chunk_size, tickers, start_date)
            ms_tickers.append(ms_from_chunk)

            if len(ms_from_chunk) > 40:
                time.sleep(120)

            else:
                time.sleep(10)

        return ms_tickers

    # TODO: check
    def update_prices(self):
        # Get present tickers
        present_ticker_ids = pd.read_sql("SELECT DISTINCT ticker_id FROM daily_price", self.db.conn)
        index_ticker = {v: k for k, v in self.ticker_index.items()}
        present_tickers = [index_ticker[i] for i in list(present_ticker_ids['ticker_id'])]

        # Get last date
        sql = "SELECT price_date FROM daily_price WHERE ticker_id=1"
        dates = pd.read_sql(sql, self.db.conn)
        last_date = dates.iloc[-1, 0]
        self.download_stock_data(present_tickers, start_date=last_date)
