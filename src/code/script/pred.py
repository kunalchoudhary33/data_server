import xgboost as xgb
import pandas as pd
import joblib
import logging
import json
import time
import warnings
from kiteconnect import KiteConnect
from datetime import datetime, timedelta
from database import Database
from sklearn.preprocessing import MinMaxScaler

db = Database()
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG)
CONFIG_PATH = "../config/config.json"
CRED_CONFIG_PATH = "../config/cred.json"
warnings.filterwarnings('ignore')

class Pred():
    def __init__(self):
        with open(CRED_CONFIG_PATH, 'r') as f:
            cred_config = json.load(f)
        api_key = cred_config['key']
        access_token = cred_config['access_tkn']

        self.instrument_token = "256265"

        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token=access_token)

        self.model = xgb.Booster()
        self.model.load_model('../../model/xgb_model_nbn.model')

        scale_df = pd.read_csv("../..//data/master_data.csv")
        scale_df = scale_df.drop(['date', 'target','label'], axis=1)

        scaler = MinMaxScaler()
        self.scaler = scaler.fit(scale_df)

        # self.scaler = joblib.load("../../model/train_scaler_file.joblib") 


    def main(self, curr_pcr):

        
        pre_day_data = pd.DataFrame(self.kite.historical_data(self.instrument_token, (datetime.now() - timedelta(days=5)).date(),
                                          (datetime.now() - timedelta(days=1)).date(), "day"))
        prev_high = pre_day_data['high'].to_list()[-1]
        prev_low = pre_day_data['low'].to_list()[-1]
        prev_close = pre_day_data['close'].to_list()[-1]


        curr_date = datetime.now()
        hour = curr_date.hour
        minute = curr_date.minute
        second = curr_date.second
        date = curr_date.date()

        if(hour >= 0 and minute >=0):
            time.sleep(2.5)
            pcr = curr_pcr
            start_date = str(date)+" 09:15:00"
            end_date = str(date)+" 09:17:00"
            curr_df= pd.DataFrame(self.kite.historical_data(self.instrument_token, from_date = start_date, to_date = end_date, 
                                            interval = "minute"))
            open_ls = curr_df['open'].to_list()
            close_ls = curr_df['close'].to_list()
            cnd = (close_ls[0] - open_ls[0]) + (close_ls[1] - open_ls[1])

            curr_price = self.kite.quote(self.instrument_token)[self.instrument_token]["last_price"]
            open_price = open_ls[0]
            eth = float(curr_price) - float(prev_high)
            etl = float(curr_price) - float(prev_low)
            etc = float(curr_price) - float(prev_close)
            oth = float(open_price) - float(prev_high)
            otl = float(open_price) - float(prev_low)
            otc = float(open_price) - float(prev_close)
          
       
            features = [[round(pcr), round(cnd), round(eth), round(etl), round(etc), round(oth), round(otl), round(otc)]]
            scaled_data=self.scaler.transform(features)
            test_data = xgb.DMatrix(scaled_data)
            predictions = self.model.predict(test_data)

            if(predictions > 0.50):
                return ("Sell", predictions[0])
            else:
                return ("Buy", 1 - predictions[0])

        else:
            return ("Null", "Null")

# prd = Pred()
# pcr = -359700.0
# print(prd.main(pcr))



        