from update_data import *


def filter_discount_stock(date):
    today = datetime.datetime.now().date()
    date_str =str(datetime.datetime.now())
    query_get_data = f"select ticker,date, close, volume from portfolio_stockpricefilter where date >= '{date}';"
    df_stock = postgres.read_sql_to_df(1,query_get_data)
    df_stock['date'] = df_stock['date'].dt.date
    max_close_values = df_stock.groupby('ticker')['close'].max()
    df_stock_lated = df_stock[(df_stock['date']==today)&(df_stock['volume']>100000)]
    df_stock_lated['max'] = df_stock_lated['ticker'].map(max_close_values)
    df_stock_lated['percent_discount'] = round((df_stock_lated['close']/df_stock_lated['max']-1)*100,2)
    query_get_sector = 'select * from tbliststockbysectoricb;'
    df_sector = postgres.read_sql_to_df(0,query_get_sector)
    df_final = pd.merge(df_stock_lated, df_sector, left_on='ticker', right_on='stock')
    df_final = df_final.sort_values(by=['sector', 'percent_discount'], ascending=[True, False]).reset_index(drop =True)
    df_final =df_final.drop(columns=['stock','date','volume'])
    df_final.to_csv('filter result.csv', index=False)
    gc = pygsheets.authorize(service_file = "C:\\Users\\Huy Tin\\workspace\\vervproject\\thptrading\\thptrading.json") 
    sheet = gc.open('data') 
    sheet_ranking_discount_price=sheet[1]
    sheet_ranking_discount_price.set_dataframe(df_final,(1,1))
    sheet_ranking_discount_price.update_value('I1',date_str)
    print('đẫ update bảng discount')


