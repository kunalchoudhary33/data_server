import time
import json
import pandas as pd
from kiteconnect import KiteConnect
from datetime import datetime, timedelta

CRED_CONFIG_PATH = "../config/cred.json"

class Stock():
    def __init__(self):
        with open(CRED_CONFIG_PATH, 'r') as f:
            cred_config = json.load(f)
        api_key = cred_config['key'] 
        access_token = cred_config['access_tkn']

        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token=access_token)
        self.path = "../../data/stock_instrument.csv"
        self.selected_stocks = "../../data/21_march.csv"

        self.data = pd.DataFrame()



    def camarilla_pivot_points(self, high, low, close):
        pivot_points = {}
        range = high - low
        pivot_points['PP'] = (close + high + low) / 3
        pivot_points['R1'] = close + (range * 1.1 / 12)
        pivot_points['R2'] = close + (range * 1.1 / 6)
        pivot_points['R3'] = close + (range * 1.1 / 4)
        pivot_points['R4'] = close + (range * 1.1 / 2)
        pivot_points['S1'] = close - (range * 1.1 / 12)
        pivot_points['S2'] = close - (range * 1.1 / 6)
        pivot_points['S3'] = close - (range * 1.1 / 4)
        pivot_points['S4'] = close - (range * 1.1 / 2)
        return pivot_points
    

    def get_stocks_pp(self):
        stockname = []
        stockr3 = []
        stockr4 = []
        stocks4 = []
        prev_vol = []
        name = []
        inst_token = []

        inst_data = pd.read_csv(self.path)
        sel_stocks_data = pd.read_csv(self.selected_stocks)
        sel_stocks_symbol_ls = sel_stocks_data['symbol'].to_list()
        for stock in sel_stocks_symbol_ls:
            filtered_df = inst_data[inst_data['symbol'] == stock]
            token = filtered_df.iloc[0]['instrument_token']
            name.append(stock)
            inst_token.append(token)

        
        instrument_data = pd.DataFrame()
        instrument_data['name'] = name
        instrument_data['instrument_token'] = inst_token

        for _,rows in instrument_data.iterrows():
            tradingsymbol = "NSE:"+str(rows['name'])
            instrument_token = rows['instrument_token']
            pre_day_data = self.kite.historical_data(instrument_token, (datetime.now() - timedelta(days=5)).date(),
                                          (datetime.now() - timedelta(days=1)).date(), "day", oi=True)
            time.sleep(0.5)
            prev_high = pre_day_data[-1]['high']
            prev_low = pre_day_data[-1]['low']
            prev_close = pre_day_data[-1]['close']
            prev_volume = pre_day_data[-1]['volume']
            pivot_point = self.camarilla_pivot_points(prev_high, prev_low, prev_close)
            stockname.append(tradingsymbol)
            stockr3.append(pivot_point['R3'])
            stockr4.append(pivot_point['R4'])
            stocks4.append(pivot_point['S4'])
            prev_vol.append(prev_volume)
        self.data['name'] = stockname
        self.data['r3'] = stockr3
        self.data['r4'] = stockr4
        self.data['s4'] = stocks4
        self.data['vol'] = prev_vol
        return self.data
    

    def main(self):
        local_data = self.data
        while True:
            for _,rows in local_data.iterrows():
                tradingsymbol = rows['name']
                s4 = rows['s4']
                time.sleep(0.25)
                last_trade_price = (self.kite.quote(tradingsymbol)[tradingsymbol]['last_price'])
                if(int(last_trade_price) < s4):
                    print(tradingsymbol.split(":")[-1]+"              :  ")
                    print(" ")

obj = Stock()
print("Data loaded started ")
data = obj.get_stocks_pp()
print("Data loaded successfully !! ")
print("################################################################")
obj.main()


# print(data.head(60))
# data = pd.DataFrame()
# a = ['a', 'b', 'c', 'd']
# b = [1,2,3,4]
# data['a'] = a
# data['b'] = b
# print(data.head())
# condition = (data['a'] == 'b')
# data = data[~condition]
# print(data.head())
