from update_data import *


def status_sent_ms(number):
    if number >0:
        status = "Tăng"
    elif number ==0:
        status = "Không đổi"
    else:
        status = "Giảm"
    return status

def get_market_price_port_mrChi():
    gc = pygsheets.authorize(service_file = "C:\\Users\\Huy Tin\\workspace\\vervproject\\thptrading\\thptrading.json") 
    sheet = gc.open('data') 
    sheet_update_price=sheet[0]
    df = sheet_update_price.get_as_df()
    list_stock = df['ticker'].to_list()
    date_check = datetime.datetime.now().date()
    previous_date = difine_previous_trading_date(date_check)
    query_get_price= f"select ticker, date, close,volume from portfolio_stockpricefilter where date >= '{previous_date.strftime('%Y-%m-%d')}' and ticker in {tuple(list_stock)}"
    df_math_price = postgres.read_sql_to_df(1,query_get_price)
    grouped = df_math_price.groupby('ticker') 
    df_math_price['price']= df_math_price['close']
    df_math_price['close_change'] = grouped['close'].diff()
    df_math_price['volume_change'] = grouped['volume'].diff()
    # Tính phần trăm thay đổi cho cột 'close' và 'volume'
    df_math_price['close_pct_change'] = (df_math_price['close_change'] / df_math_price.groupby('ticker')['close'].shift(1)) * 100
    df_math_price['volume_pct_change'] = (df_math_price['volume_change'] / df_math_price.groupby('ticker')['volume'].shift(1)) * 100
    df_final = df_math_price.drop(columns=['close'])
    df_final = df_final[df_final['date'].dt.date==date_check]
    sheet_update_price.set_dataframe(df_final,(1,1))
    result_message = ""
    for index, row in df_final.iterrows():
        stock = row['ticker']
        close_pct_change = row['close_pct_change']
        volume_pct_change = row['volume_pct_change']
        price = row['price']
        status_price = status_sent_ms(close_pct_change)
        status_volume = status_sent_ms(volume_pct_change)
        message = f"{stock}: {status_price} {close_pct_change:.2f}% với giá {price:.2f}; KLGD {status_volume} {volume_pct_change:.2f}%"
        result_message += message + "\n" 
    bot = Bot(token='5881451311:AAEJYKo0ttHU0_Ztv3oGuf-rfFrGgajjtEk')
    bot.send_message(
                    chat_id='-870288807', #room Thạch
                    text=result_message)
    print('đã gửi báo cáo Mr Chi xong')
    return 
    
def report_inav_mr_chi(data_file):
    df =pd.read_csv(data_file)
    data = compare_market_index(df)
    data[0].to_sql('tbreportinavmrchi', postgres.engine(0), if_exists='replace', index=False)   
    data[1].to_sql('tbcompareeffectivewithindex', postgres.engine(0), if_exists='replace', index=False)  



query_get_price= f"select ticker, date, close,volume from portfolio_sectorprice where ticker = 'VNINDEX' "
df_math_price = postgres.read_sql_to_df(1,query_get_price)
df_math_price['date'] = pd.to_datetime(df_math_price['date'])
december_data = df_math_price[df_math_price['date'].dt.month == 12]

result = december_data.groupby([december_data['date'].dt.year]).agg({'date': ['min', 'max']}).reset_index()
result.columns = ['nam', 'ngay_nho_nhat', 'ngay_lon_nhat']
df_melted = pd.melt(result, id_vars=['nam'], value_vars=['ngay_nho_nhat', 'ngay_lon_nhat'], var_name='ngay_type', value_name='ngay')
result = pd.merge(df_melted, df_math_price[['date', 'close']], left_on='ngay', right_on=df_math_price['date'])
result= result[['nam', 'date', 'close']].sort_values('date')
result['percent_change'] = result.groupby('nam')['close'].pct_change() * 100
df_final = result.dropna(subset=['percent_change'])
df = df_final[['nam','percent_change']]
df.to_csv('thong ke noel.csv')



