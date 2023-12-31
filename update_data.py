import postgres_connect as postgres
from datetime import timedelta, time, datetime
import datetime
import pandas as pd
import os
import csv
import numpy as np
import requests
from bs4 import BeautifulSoup
import pygsheets
from telegram import Bot

def difine_previous_trading_date(date_check):
    while True:
        date_check = date_check - datetime.timedelta(days=1)
        weekday = date_check.weekday()
        query_get_date_note_trade = 'select * from portfolio_datenottrading '
        df_date = postgres.read_sql_to_df(1, query_get_date_note_trade)
        check_in_dates = date_check in df_date["date"].values
        if not check_in_dates and weekday not in (5, 6):
            return date_check

def sector_data_import():
    # Đường dẫn tới thư mục chứa các file CSV
    folder_path = "C:\\ExportData\\Sector"
    # Tạo một danh sách để lưu trữ tất cả các DataFrame từ các file CSV
    dfs = []
    # Duyệt qua tất cả các file trong thư mục
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            # Loại bỏ phần ".csv" ở cuối và sử dụng tên file làm cột 'name'
            name = os.path.splitext(filename)[0]
            file_path = os.path.join(folder_path, filename)
            # Đọc file CSV, bỏ dòng đầu tiên
            df = pd.read_csv(file_path, header=0, index_col=False)
            df['ticker'] = name
            dfs.append(df)
    # Nối tất cả các DataFrame thành một DataFrame tổng
    sector_df = pd.concat(dfs, ignore_index=True)
    sector_df['date'] = pd.to_datetime(sector_df['date'], format='%d-%b-%y').dt.strftime('%Y-%m-%d')
    sector_df['date_time'] = pd.to_datetime(sector_df['date']) + pd.Timedelta(hours=14, minutes=45)
    sector_df['id'] = sector_df.index
    sector_df.to_sql('portfolio_sectorprice', postgres.engine(1), if_exists='replace', index=False)
    print('Tai data nganh xong')


def stock_price_adjusted_data_import():
    # Đường dẫn tới thư mục chứa các file CSV
    folder_path = "C:\\ExportData\\Stock"
    # Tạo một danh sách để lưu trữ tất cả các DataFrame từ các file CSV
    dfs = []
    # Duyệt qua tất cả các file trong thư mục
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            # Loại bỏ phần ".csv" ở cuối và sử dụng tên file làm cột 'name'
            name = os.path.splitext(filename)[0]
            file_path = os.path.join(folder_path, filename)
            # Đọc file CSV, bỏ dòng đầu tiên
            df = pd.read_csv(file_path, header=0, index_col=False)
            df['ticker'] = name
            dfs.append(df)
    # Nối tất cả các DataFrame thành một DataFrame tổng
    stock_df = pd.concat(dfs, ignore_index=True)
    stock_df['date'] = pd.to_datetime(stock_df['date'], format='%d-%b-%y').dt.strftime('%Y-%m-%d')
    stock_df = stock_df.sort_values('date')
    stock_df['date_time'] = pd.to_datetime(stock_df['date']) + pd.Timedelta(hours=14, minutes=45)
    stock_df = stock_df.reset_index(drop=True)
    # dòng này sẽ gây lỗi với Django, không migrate được data
    stock_df.to_sql('portfolio_stockprice', postgres.engine(1), if_exists='replace', index=False)
    add_id_column_query = f"ALTER TABLE public.portfolio_stockprice ADD id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY;"
    postgres.execute_query(1,add_id_column_query)
    print('Tai data chứng khoán xong')

def stock_price_filter_adjusted_data_import(num_date):
    date = datetime.datetime.now()
    start_date = date - datetime.timedelta(days = num_date)
    # Đường dẫn tới thư mục chứa các file CSV
    folder_path = "C:\\ExportData\\Stock"
    # Tạo một danh sách để lưu trữ tất cả các DataFrame từ các file CSV
    dfs = []
    # Duyệt qua tất cả các file trong thư mục
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            # Loại bỏ phần ".csv" ở cuối và sử dụng tên file làm cột 'name'
            name = os.path.splitext(filename)[0]
            file_path = os.path.join(folder_path, filename)
            # Đọc file CSV, bỏ dòng đầu tiên
            df = pd.read_csv(file_path, header=0, index_col=False)
            df['ticker'] = name
            dfs.append(df)
    # Nối tất cả các DataFrame thành một DataFrame tổng
    stock_df = pd.concat(dfs, ignore_index=True)
    stock_df['date'] = pd.to_datetime(pd.to_datetime(stock_df['date'], format='%d-%b-%y').dt.strftime('%Y-%m-%d'))
    stock_df['date_time'] = stock_df['date'] + pd.Timedelta(hours=14, minutes=45)
    limited_stock_df = stock_df[stock_df['date']>=start_date]
    limited_stock_df = limited_stock_df.sort_values('date')
    limited_stock_df = limited_stock_df.reset_index(drop=True)
    # dòng này sẽ gây lỗi với Django, không migrate được data
    limited_stock_df.to_sql('portfolio_stockpricefilter', postgres.engine(1), if_exists='replace', index=False)
    add_id_column_query = f"ALTER TABLE portfolio_stockpricefilter ADD id int8 NOT NULL GENERATED BY DEFAULT AS IDENTITY;"
    postgres.execute_query(1,add_id_column_query)
    print('Tai data chứng khoán xong')
    return limited_stock_df






def cal_ishare_fund(df, start_date ='2000-01-01'):
    origin_inav = 10000
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date() 
    df['date'] = pd.to_datetime(df['date']).dt.date
    df =df[df['date']>=start_date]
    # thêm tiền dư trong 2 TK madam và A Thanh vào cash in
    for index, row in df.iterrows():
        if row['date'] == start_date:
            df.at[index, 'cashin_out'] = row['nav']
    df = df.sort_values('date').reset_index(drop=True)
    df['change_share'] = 0.0 
    df['price_share'] = 0.0 
    df['number_share'] = 0.0 
    for index, row in df.iterrows():
        if index == 0:
            df.at[index, 'price_share'] = origin_inav
            df.at[index, 'number_share'] = round(row['cashin_out'] / origin_inav)
        else:
            if row['cashin_out'] == 0:
                number_share = df.at[index - 1, 'number_share']
            else:
                change_share = round(row['cashin_out'] / df.at[index - 1, 'price_share'])
                number_share = df.at[index - 1, 'number_share'] + change_share
                df.at[index, 'change_share'] = change_share
            df.at[index, 'number_share'] = number_share
            df.at[index, 'price_share'] = round(row['nav'] / number_share, 2)
    df['ROI'] = ((df['price_share'] - df.at[0, 'price_share'])) / df.at[0, 'price_share'] * 100
    df['profit'] = df['nav']- df['cashin_out'].cumsum()
    return df

def compare_market_index(df, start_date ='2000-01-01'):
    df = cal_ishare_fund(df,start_date)
    new_df = df[['date', 'ROI']]
    new_df['ticker'] = 'my_fund'
    min_date = df['date'].min()
    max_date = df['date'].max()
    query_get_df_sector = f"select date, ticker, close from portfolio_sectorprice where date >= '{min_date}' and date <= '{max_date}' "
    df_data = postgres.read_sql_to_df(1,query_get_df_sector)
    df_VNINDEX = df_data[(df_data['ticker']=='VNINDEX')].sort_values('date').reset_index(drop=True)
    df_VNINDEX['ROI'] = ((df_VNINDEX['close'] - df_VNINDEX.at[0, 'close'])) / df_VNINDEX.at[0, 'close'] * 100
    df_VNINDEX = df_VNINDEX.drop(columns =['close'])
    df_VN30 = df_data[(df_data['ticker']=='VN30')].sort_values('date').reset_index(drop=True)
    df_VN30['ROI'] = ((df_VN30['close'] - df_VN30.at[0, 'close'])) / df_VN30.at[0, 'close'] * 100
    df_VN30 = df_VN30.drop(columns =['close'])
    result = pd.concat([new_df, df_VN30, df_VNINDEX], ignore_index=True)
    result['date'] =pd.to_datetime(result['date'])
    return df, result
    

# df_cash =pd.read_csv('Cash_in_out.csv')
# df_cash['CashOut'] = pd.to_numeric(df_cash['CashOut'], errors='coerce')
# df_cash['CashIn'] = pd.to_numeric(df_cash['CashIn'], errors='coerce')
# df_cash.fillna(0, inplace=True)
# # Thực hiện phép tính trên các số
# df_cash['Cash Out/In'] = df_cash['CashIn'] - df_cash['CashOut']
# df_cash =df_cash[['Date', 'AccountID', 'Cash Out/In']]

# df_nav =pd.read_csv('NAV.csv')
# merged_df = pd.merge(df_nav, df_cash, left_on=['date', 'AccountID'], right_on=['Date', 'AccountID'], how='left')
# result_df = merged_df[['date', 'AccountID', 'NAV', 'Cash Out/In']]
# result_df.columns = ['date', 'account', 'nav', 'cashin_out']
# result_df.fillna(0, inplace=True)
# result_df.to_sql('tbthpnavcashinouthsc', postgres.engine(0), if_exists='replace', index=False)   

# for index, row in df_cash.iterrows():
#     if row['MovementType'] == 'D':
#         df_cash.at[index, 'cashin_out'] = row['cashin_out']
#     else:
#         df_cash.at[index, 'cashin_out'] = row['cashin_out']*-1



