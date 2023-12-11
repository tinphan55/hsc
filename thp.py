from update_data import *


def get_number_stock_listed(stock):
    linkbase= 'https://www.cophieu68.vn/quote/profile.php?id=' + stock 
    r =requests.get(linkbase)
    soup = BeautifulSoup(r.text,'html.parser')
    td_tags = soup.find_all('td')
    desired_label = "KL niÃªm yáº¿t"
    desired_value = None
    for i in range(len(td_tags) - 1):
    # Kiểm tra nếu nội dung của td_tags[i] phù hợp với desired_label
        if td_tags[i].text.strip() == desired_label:
            # Lấy giá trị tương ứng từ td_tags[i + 1]
            desired_value = td_tags[i + 1].text.strip()
            break
    return int(desired_value.replace(',', ''))

def define_date_stock_on_account(time_matched):
    query_get_date_note_trade = 'select * from portfolio_datenottrading '
    df_date = postgres.read_sql_to_df(1, query_get_date_note_trade)
    t = 0
    while t != 2:  # Sửa điều kiện thành t != 2
        time_matched = time_matched + timedelta(days=1)
        weekday = time_matched.weekday() 
        check_in_dates = time_matched.date() in df_date["date"].values
        if check_in_dates or weekday == 5 or weekday == 6:
            pass
        else:
            t += 1
    return time_matched

def open_file(name_file):
    data_list = []
    stock_list = []
    stock_set = set()
    path1 = r'C:\Users\Huy Tin\Downloads'
    path2 = r'C:\Users\Huy Tin\Documents'
    valid_paths = [path1, path2]
    for path in valid_paths:
        full_path = path + '\\' + name_file + '.csv'
        try:
            with open(full_path, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    date = datetime.datetime.strptime(name_file, '%d.%m.%Y')
                    row['DATE'] = date
                    row['date_receive'] = define_date_stock_on_account(date)
                    for field in ['EXECUTED PRICE', 'EXECUTED QTY', 'CONSIDERATION']:
                        row[field] = row[field].replace(',', '')  # Loại bỏ dấu phẩy ngăn cách phần ngàn
                    stock = row['STOCK']
                    if stock not in stock_set:
                        stock_list.append(stock)
                        stock_set.add(stock)
                    data_list.append(tuple(row.values()))
            return data_list, stock_list
        except FileNotFoundError:
            pass  # Bỏ qua nếu không tìm thấy tệp, tiếp tục kiểm tra các đường dẫn khác
    print("Tệp không tồn tại.")
    return None

def open_file_pandas(name_file):
    stock_set = set()
    path1 = r'C:\Users\Huy Tin\Downloads'
    path2 = r'C:\Users\Huy Tin\Documents'
    valid_paths = [path1, path2]
    df = None  # Khởi tạo df ban đầu
    for path in valid_paths:
        full_path = path + '\\' + name_file + '.csv'
        try:
            df = pd.read_csv(full_path, encoding='utf-8')
            date = datetime.datetime.strptime(name_file, '%d.%m.%Y')
            df['DATE'] = date
            df['date_receive'] = define_date_stock_on_account(date)
            for field in ['EXECUTED PRICE', 'EXECUTED QTY', 'CONSIDERATION']:
                df[field] = df[field].str.replace(',', '')  # Loại bỏ dấu phẩy ngăn cách phần ngàn
            stock_set.update(df['STOCK'].unique())  # Thêm danh sách các mã cổ phiếu vào set
        except FileNotFoundError:
            pass  # Bỏ qua nếu không tìm thấy tệp, tiếp tục kiểm tra các đường dẫn khác
    if df is not None:  # Kiểm tra sự tồn tại của df
        return df, stock_set
    print("Tệp không tồn tại.")
    return None





def return_df_remaining_qty(stock_symbol, df):
    stock_df = df[(df['STOCK'] == stock_symbol)].copy()
    buy_df = stock_df[stock_df['B/S'] == 'B'].copy()
    buy_df['remaining_qty'] = buy_df['EXECUTED QTY']
    sell_df = stock_df[stock_df['B/S'] == 'S'].sort_values(by=['DATE', 'id'], ascending=[True, False])
    executed_buy_index = []  # Chứa index của các lệnh mua đã thực hiện
    total_buy = buy_df['EXECUTED QTY'].sum()
    total_sell = sell_df['EXECUTED QTY'].sum()
    remain = total_buy - total_sell
    if remain>0:
        for index, row in sell_df.iterrows():
            sell_quantity = row['EXECUTED QTY']
            sell_date = row['DATE']
            valid_buy_df = buy_df[(buy_df['date_receive'] < sell_date) & (buy_df.index.isin(executed_buy_index) == False)]
            valid_buy_df = valid_buy_df.sort_values(by=['EXECUTED PRICE', 'DATE', 'id'], ascending=[True, True, False])
            total_valid_quantity = valid_buy_df['EXECUTED QTY'].sum()
            if total_valid_quantity >= sell_quantity:
                remaining_qty_to_subtract = sell_quantity  # Số lượng cổ phiếu cần trừ
                for idx, row in valid_buy_df.iterrows():
                    if remaining_qty_to_subtract <= row['remaining_qty']:
                        buy_df.at[idx, 'remaining_qty'] -= remaining_qty_to_subtract
                        # executed_buy_index.append(idx)
                        break
                    else:
                        remaining_qty_to_subtract -= row['remaining_qty']
                        buy_df.at[idx, 'remaining_qty'] = 0
                        executed_buy_index.append(idx)
        buy_df['remaining_value'] = buy_df['remaining_qty']*buy_df['EXECUTED PRICE']
    elif total_buy == total_sell:
        buy_df = pd.DataFrame()
    else:
        print (f"bug data với cổ phiếu {stock_symbol}")
    return buy_df, remain






def calculate_avg_price(stock_symbol, df):
    info = return_df_remaining_qty(stock_symbol, df)
    df_result = info[0]
    remain = info[1]
    if len(df_result)>0:
        total_qty = df_result['remaining_qty'].sum()
        total_value = df_result['remaining_value'].sum()
        avg_price = round(total_value/total_qty,0)
        if total_qty !=remain:
            print(f"Không khớp số lượng còn lại trong tính toán trung bính giá {stock_symbol}")
    else:
        avg_price = 0
        total_qty =0
    return avg_price, remain



def stock_on_account(df):
    df = df.drop(columns=["id","date_receive", "remaining_qty","remaining_value","CONSIDERATION","EXECUTED PRICE"])
    grouped = df.groupby(['CLIENT', 'STOCK', 'B/S'])['EXECUTED QTY'].sum().reset_index()
    df_b = df[df['B/S'] == 'B']
    df_s = df[df['B/S'] == 'S']
    sum_b = df_b.groupby(['CLIENT', 'STOCK'])['EXECUTED QTY'].sum().reset_index()
    sum_s = df_s.groupby(['CLIENT', 'STOCK'])['EXECUTED QTY'].sum().reset_index()
    # Merge the two DataFrames on 'CLIENT' và 'STOCK'
    result = pd.merge(sum_b, sum_s, on=['CLIENT', 'STOCK'], suffixes=('_B', '_S'), how='outer')
    result['EXECUTED QTY_S'].fillna(0, inplace=True)
    result['sellable'] =  result['EXECUTED QTY_B'] -  result['EXECUTED QTY_S']
    result = result.loc[result['sellable'] != 0]
    result.rename(columns={'CLIENT': 'account', 'STOCK': 'stocksymbol','EXECUTED QTY_B': 'total_buy', 'EXECUTED QTY_S': 'total_sell'}, inplace=True)
    return result



# # Chuyển đổi cột "DATE" thành kiểu dữ liệu ngày tháng
# df["DATE"] = pd.to_datetime(df["DATE"])
# # Tạo một DataFrame tạm thời với các giá trị DATE không trùng lặp
# unique_dates = df.drop_duplicates(subset="DATE")
# # Áp dụng hàm define_date_stock_on_account cho DataFrame tạm thời
# unique_dates["date_receive"] = unique_dates["DATE"].apply(define_date_stock_on_account)
# # Sử dụng merge để sao chép kết quả cho tất cả các bản ghi có cùng DATE
# df = df.merge(unique_dates[["DATE", "date_receive"]], on="DATE", how="left")
# # Chọn cột "date_receive" từ kết quả của merge và đặt lại tên thành "date_receive" (ghi đè lên cột ban đầu)
# df["date_receive"] = df["date_receive_y"]
# df = df.drop(columns=["date_receive_x", "date_receive_y"])

# # Lặp qua từng hàng trong buy_df và cập nhật dữ liệu vào bảng tbthptrasationhistory
# for index, row in df.iterrows():
#     condition = f"id = {row['id']}"
#     row_data = {
#         "date_receive": row["date_receive"],
#     }
#     update_query = f"UPDATE tbthptrasationhistory SET date_receive='{row['date_receive']}' WHERE {condition}"
#     postgres.execute_query(0,update_query)


def update_nav_history(df_account,df_port, date):
        df_result = pd.merge(df_account, df_port[['stocksymbol', 'avgprice', 'matchprice']], on=['stocksymbol'], how='outer')
        df_result['stock_value'] = df_result['matchprice']*df_result['sellable']
        df_market_value = df_result.groupby(['account'])['stock_value'].sum().reset_index()
        query_get_cash = 'select * from tbthpmargin;'
        df_cash = postgres.read_sql_to_df(0,query_get_cash)
        df_total_account = pd.merge(df_market_value, df_cash[['account', 'cashbalance']], on=['account'], how='outer')
        df_total_account['nav'] = df_total_account['stock_value'] +df_total_account['cashbalance'] 
        df_total_account['date'] = date
        query_get_check= f"select * from tbnavhistoryaccount where date = '{date}' "
        df_check = postgres.read_sql_to_df(0,query_get_check)
        for index, row in df_total_account.iterrows():
            date = row["date"]
            account = row["account"]
            stock_value = row["stock_value"]
            cashbalance = row["cashbalance"]
            nav = row["nav"]
            if len(df_check) >0:
                condition = f"date = '{date}' and account='{account}'"
                update_query = f"UPDATE tbnavhistoryaccount SET stock_value={stock_value}, cashbalance={cashbalance}, nav={nav} WHERE {condition}"
                postgres.execute_query(0,update_query)
            else:
                insert_query = f"INSERT INTO tbnavhistoryaccount (date, account,stock_value, cashbalance, nav) VALUES ('{date}', '{account}', {stock_value}, {cashbalance}, {nav})"
                postgres.execute_query(0, insert_query)

def update_market_price_port ():
    date = datetime.datetime.now().date()
    query_get_port = 'select * from tbthpopenportsummary'
    df_port = postgres.read_sql_to_df(0,query_get_port)
    if len(df_port)>0:
        query_get_price= f"select * from portfolio_stockpricefilter where date = '{date.strftime('%Y-%m-%d')}' "
        df_math_price = postgres.read_sql_to_df(1,query_get_price)
        ticker_to_close = df_math_price.set_index('ticker')['close'].to_dict()
        # Cập nhật cột matchprice của df_port bằng cột close từ df_math_price
        df_port['matchprice'] = df_port['stocksymbol'].map(ticker_to_close)*1000
        query_get_divident = f"select * from tbthpcashdivident where date >'2022-07-22' "
        df_divident = postgres.read_sql_to_df(0,query_get_divident) 
        df_divident = df_divident.groupby('stocksymbol').sum(numeric_only=True).reset_index()
        df_port = pd.merge(df_port, df_divident[['stocksymbol', 'value','ratio']], on=['stocksymbol'], how='left')
        df_port = df_port.loc[df_port['stocksymbol'] != 'YEG']
        df_port.fillna({'value': 0, 'ratio': 0}, inplace=True)
        df_port['market_value'] = df_port['matchprice'] *df_port['sellable']
        df_port['profit'] = (df_port['matchprice'] -df_port['avgprice'])*df_port['sellable'] + df_port['value']
        df_port['profitratio'] = (df_port['matchprice']-df_port['avgprice']+ df_port['ratio']*10000)/df_port['avgprice'] -0.08/100*2 -0.1/100
        df_port['date']= date
        df_port['stock_listed'] = df_port['stocksymbol'].apply(get_number_stock_listed)
        df_port['ratio_hold'] = round(df_port['sellable']/df_port['stock_listed']*100,2)
        df_port = df_port[df_port['sellable'] > 0]
        for index, row in df_port.iterrows():
            condition = f"stocksymbol = '{row['stocksymbol']}'"
            update_query = f"UPDATE tbthpopenportsummary SET matchprice={row['matchprice']}, profit={row['profit']},profitratio={row['profitratio']},date='{row['date']}',market_value = {row['market_value']},stock_listed = {row['stock_listed']},ratio_hold = {row['ratio_hold']}, divident ={round(float(row['value']),2)}  WHERE {condition}"
            postgres.execute_query(0,update_query)
        return df_port
    else:
        print('Danh mục không có cổ phiếu nào')

def update_profit_deal(df_all_trade,df_port, date):
    df_all_trade = df_all_trade.drop(columns=["CLIENT","id","date_receive", "remaining_qty","remaining_value","DATE"])
    df_all_trade = df_all_trade.loc[(df_all_trade['STOCK'] != 'YEG')]
    grouped = df_all_trade.groupby(['STOCK', 'B/S']).agg({'CONSIDERATION': 'sum', 'EXECUTED QTY': 'sum'}).reset_index()
    pivot_df = grouped.pivot_table(index='STOCK', columns='B/S', values=['CONSIDERATION', 'EXECUTED QTY'], fill_value=0)
    pivot_df['profit'] = pivot_df[('CONSIDERATION', 'S')] - pivot_df[('CONSIDERATION', 'B')]
    pivot_df['remain_qty'] = pivot_df[('EXECUTED QTY', 'B')] - pivot_df[('EXECUTED QTY', 'S')]
    pivot_df['fee'] = (pivot_df[('CONSIDERATION', 'S')] + pivot_df[('CONSIDERATION', 'B')])*0.08/100 + pivot_df[('CONSIDERATION', 'S')]*0.1/100
    pivot_df.reset_index(inplace=True)
    pivot_df.rename(columns={'STOCK': 'stocksymbol'}, inplace=True)
    pivot_df.columns = [' '.join(col).strip() for col in pivot_df.columns.values]
    df_result = pd.merge(pivot_df, df_port[['stocksymbol', 'matchprice']], on=['stocksymbol'], how='outer')
    query_get_divident = 'select stocksymbol, sum(value) as divident from tbthpcashdivident group by stocksymbol'
    df_divident = postgres.read_sql_to_df(0,query_get_divident)
    df_total = pd.merge(df_result, df_divident[['stocksymbol', 'divident']], on=['stocksymbol'], how='outer')
    # df_total.rename(columns={'value': 'divident'}, inplace=True)
    df_total.fillna({'divident': 0, 'matchprice': 0}, inplace=True)
    df_total['market_value'] = df_total['remain_qty']*df_total['matchprice'] 
    df_total['net_profit']= df_total['market_value']+df_total['divident'] +df_total['profit'] -df_total['fee']
    # df_result = df_result.drop(columns='stocksymbol')
    df_total = df_total.drop(columns=["CONSIDERATION B","CONSIDERATION S","EXECUTED QTY B", "EXECUTED QTY S"])
    df_total['date']=date
    df_total.to_sql('tbthpdealprofit', postgres.engine(0), if_exists='replace', index=False) 
    return df_total



    


def update_tbthpopenportsummary(name_file=None):
    query_get_port = 'select * from tbthpopenportsummary'
    df_port = postgres.read_sql_to_df(0,query_get_port)
    query_get_all_trade = 'select * from tbthptrasationhistory'
    df_all_trade = postgres.read_sql_to_df(0,query_get_all_trade)
    if name_file:
        file = open_file_pandas(name_file)
        date = datetime.datetime.strptime(name_file, '%d.%m.%Y')
        df_transation = file[0]
        stock_update =  list(file[1])
        max_id = df_all_trade['id'].max()
        new_ids = pd.Series(range(max_id + 1, max_id + 1 + len(df_transation)))
        df_transation['id'] = new_ids
        df_transation.to_sql('tbthptrasationhistory', postgres.engine(0), if_exists='append', index=False)
        print('Đã cập nhật giao dịch mới')
        df_all_trade = postgres.read_sql_to_df(0,query_get_all_trade)
        if len(df_port) >0:
            for stock_symbol in stock_update:
                row_info = calculate_avg_price(stock_symbol, df_all_trade)
                avgprice =round(row_info[0],0)
                sellable= row_info[1]
                matching_rows = df_port[df_port['stocksymbol'] == stock_symbol]
                # Nếu tìm thấy dòng tương ứng
                if not matching_rows.empty:
                    condition = f"stocksymbol = '{stock_symbol}'"
                    update_query = f"UPDATE tbthpopenportsummary SET sellable={sellable}, avgprice={avgprice} WHERE {condition}"
                    postgres.execute_query(0,update_query)
                    print (f"Cập nhật giá vốn thành công cho {stock_symbol}")
                else:
                    insert_query = f"INSERT INTO tbthpopenportsummary (sellable, avgprice, stocksymbol) VALUES ({sellable}, {avgprice}, '{stock_symbol}')"
                    postgres.execute_query(0,insert_query)     
                    print (f"Tạo mới giá vốn thành công cho {stock_symbol}")
        else:
            stock_check = list(set(df_all_trade['STOCK']))
            if stock_check:
                for stock_symbol in stock_check:
                        print(f"Tính giá vốn cho {stock_symbol}")
                        row_info = calculate_avg_price(stock_symbol, df_all_trade)
                        if row_info[1]>0:
                            new_row = { 'stocksymbol':stock_symbol,
                                            'sellable': row_info[1],
                                            'avgprice' : round(row_info[0],0)
                                            }
                            new_row_df = pd.DataFrame([new_row])
                            df_port = pd.concat([df_port, new_row_df], ignore_index=True)
                            df_port.to_sql('tbthpopenportsummary', postgres.engine(0), if_exists='append', index=False)
    else:
        date =  datetime.datetime.now().date()
    df_port = postgres.read_sql_to_df(0,query_get_port) 
    df_port = df_port.loc[(df_port['sellable'] != 0) & (df_port['avgprice'] != 0)]
    df_port.to_sql('tbthpopenportsummary', postgres.engine(0), if_exists='replace', index=False)   
    df_port = update_market_price_port()
    print('Đã cập nhật giá thị trường cho bảng tbthpopenportsummary ')
    df_account = stock_on_account(df_all_trade)
    df_account['date'] = date
    df_account.to_sql('tbthpstockaccount', postgres.engine(0), if_exists='replace', index=False)
    print('Đã cập nhật bảng tbthpstockaccount')
    update_nav_history(df_account,df_port,date)
    print('Đã cập nhật bảng tbnavhistoryaccount')
    df_deal = update_profit_deal(df_all_trade,df_port, date)
    print('Đã cập nhật bảng tbthpdealprofit')
    query_get_margin_fee = 'select sum(value) from tbthpmarginfee'
    margin_fee = float(postgres.query_data(0,query_get_margin_fee)[0][0])
    total_profit = df_deal['net_profit'].sum() -margin_fee
    query_get_df_profit_date = 'select * from tbthprofitdate'
    df_get_df_profit_date = postgres.read_sql_to_df(0,query_get_df_profit_date)
    date_timestamp = pd.Timestamp(datetime.datetime(date.year, date.month, date.day))
    matching_rows = df_get_df_profit_date[(df_get_df_profit_date['date'] == date_timestamp)]
    if not matching_rows.empty:
                condition = f"date = '{date}'"
                update_query = f"UPDATE tbthprofitdate SET value={total_profit} WHERE {condition}"
                postgres.execute_query(0,update_query)
    else:
                insert_query = f"INSERT INTO tbthprofitdate (date,value) VALUES ('{date}', {total_profit})"
                postgres.execute_query(0, insert_query)
    print('Đã cập nhật bảng tbthprofitdate') 
    


def report_inav(start_date ='2021-05-20'):
    #Ngày 20/05/2021 bắt đầu giao dịch đợt 2
    # Số yeg sở hữu tại ngày 20/05/2021 của tk c UP là 6,758,410
    query_get_data = f"select * from {'tbthpnavcashinoutHSC'}"
    df = postgres.read_sql_to_df(0,query_get_data) 
    merger_cash_df = df.groupby(['date', 'account'], as_index=False).agg({'nav': 'mean', 'cashin_out': 'sum'})
    result_df = merger_cash_df.groupby('date', as_index=False).agg({'nav': 'sum', 'cashin_out': 'sum'})
    data = compare_market_index(result_df, start_date)
    data[0].to_sql('tbthpreportinav', postgres.engine(0), if_exists='replace', index=False)   
    data[1].to_sql('tbthpcompareeffectivewithindex', postgres.engine(0), if_exists='replace', index=False) 








