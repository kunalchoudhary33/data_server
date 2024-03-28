import os
import copy
import pandas as pd
import numpy as np
from kiteconnect import KiteConnect
import time
import dateutil.parser
import threading
import sys
from datetime import datetime, timedelta
import logging
import json

from database import Database
from pred import Pred

db = Database()
pred_obj = Pred()
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG)
CRED_CONFIG_PATH = "../config/cred.json"


with open(CRED_CONFIG_PATH, 'r') as f:
    cred_config = json.load(f)
api_key = cred_config['key'] 
access_token = cred_config['access_tkn']

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token=access_token)

try:
    kite.margins()
except:
    print("Login Failed!!!!")
    sys.exit()


exchange = None
while True:
    if exchange is None:
        try:
            exchange = pd.DataFrame(kite.instruments("NFO"))
            exchange = exchange[exchange["segment"] == "NFO-OPT"]
            break
        except:
            print("Exchange Download Error...")
            time.sleep(10)

df = pd.DataFrame({"FNO Symbol": list(exchange["name"].unique())})
df = df.set_index("FNO Symbol", drop=True)

pre_oc_symbol = pre_oc_expiry = ""
expiries_list = []
instrument_dict = {}
prev_day_oi = {}
stop_thread = False

def get_oi(data):
    global prev_day_oi, kite, stop_thread
    for symbol, v in data.items():
        if stop_thread:
            break
        while True:
            try:
                prev_day_oi[symbol]
                break
            except:
                try:
                    pre_day_data = kite.historical_data(v["token"], (datetime.now() - timedelta(days=5)).date(),
                                          (datetime.now() - timedelta(days=1)).date(), "day", oi=True)
                    try:
                        prev_day_oi[symbol] = pre_day_data[-1]["oi"]
                    except:
                        prev_day_oi[symbol] = 0
                    break
                except Exception as e:
                    time.sleep(0.5)


def formatINR(number):
    number = float(number)
    number = round(number,2)
    is_negative = number < 0
    number = abs(number)
    s, *d = str(number).partition(".")
    r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
    value = "".join([r] + d)
    if is_negative:
       value = '-' + value
    return value

print("#################  Streaming started. Data will render once received from exchange.  #########################")
prev_ce_value = 0
prev_pe_value = 0
while True:

    oc_symbol = "NIFTY"
    oc_expiry = "04-04-2024"
    oc_expiry = datetime.strptime(oc_expiry, "%d-%m-%Y")
    if pre_oc_symbol != oc_symbol or pre_oc_expiry != oc_expiry:

        instrument_dict = {}
        stop_thread = True
        time.sleep(2)
        if pre_oc_symbol != oc_symbol:

            expiries_list = []
        pre_oc_symbol = oc_symbol
        pre_oc_expiry = oc_expiry

    if oc_symbol is not None:
        try:
            if not expiries_list:
                df = copy.deepcopy(exchange)
                df = df[df["name"] == oc_symbol]
                expiries_list = sorted(list(df["expiry"].unique()))
                df = pd.DataFrame({"Expiry Date": expiries_list})
                df = df.set_index("Expiry Date", drop=True)

            if not instrument_dict and oc_expiry is not None:
                df = copy.deepcopy(exchange)
                df = df[df["name"] == oc_symbol]
                df = df[df["expiry"] == oc_expiry.date()]
                lot_size = list(df["lot_size"])[0]
                for i in df.index:
                    instrument_dict[f'NFO:{df["tradingsymbol"][i]}'] = {"strikePrice": float(df["strike"][i]),
                                                                        "instrumentType": df["instrument_type"][i],
                                                                        "token": df["instrument_token"][i]}
                stop_thread = False
                thread = threading.Thread(target=get_oi, args=(instrument_dict,))
                thread.start()
            option_data = {}
            instrument_for_ltp = "NSE:NIFTY 50" if oc_symbol == "NIFTY" else (
                                "NSE:NIFTY BANK" if oc_symbol == "BANKNIFTY" else ("NSE:NIFTY FIN SERVICE" if oc_symbol == "FINNIFTY" else f"NSE:{oc_symbol}"))
            underlying_price = kite.quote(instrument_for_ltp)[instrument_for_ltp]["last_price"]
            for symbol, values in kite.quote(list(instrument_dict.keys())).items():
                try:
                    try:
                        option_data[symbol]
                    except:
                        option_data[symbol] = {}
                    option_data[symbol]["strikePrice"] = instrument_dict[symbol]["strikePrice"]
                    option_data[symbol]["instrumentType"] = instrument_dict[symbol]["instrumentType"]
                    option_data[symbol]["lastPrice"] = values["last_price"]
                    option_data[symbol]["totalTradedVolume"] = values["volume"]
                    option_data[symbol]["openInterest"] = int(values["oi"]/lot_size)
                    option_data[symbol]["change"] = values["last_price"] - values["ohlc"]["close"] if values["last_price"] != 0 else 0
                    try:
                        option_data[symbol]["changeinOpenInterest"] = int((values["oi"] - prev_day_oi[symbol])) #/lot_size)
                    except:
                        option_data[symbol]["changeinOpenInterest"] = None

                except Exception as e:
                    pass

            df = pd.DataFrame(option_data).transpose()
            ce_df = df[df["instrumentType"] == "CE"]
            ce_df = ce_df[["totalTradedVolume", "change", "lastPrice", "changeinOpenInterest", "openInterest", "strikePrice"]]
            ce_df = ce_df.rename(columns={"openInterest": "CE OI", "changeinOpenInterest": "CE Change in OI",
                                          "lastPrice": "CE LTP", "change": "CE LTP Change", "totalTradedVolume": "CE Volume"})
            ce_df.index = ce_df["strikePrice"]
            ce_df = ce_df.drop(["strikePrice"], axis=1)
            ce_df["Strike"] = ce_df.index
            pe_df = df[df["instrumentType"] == "PE"]
            pe_df = pe_df[["strikePrice", "openInterest", "changeinOpenInterest",  "lastPrice", "change", "totalTradedVolume"]]
            pe_df = pe_df.rename(columns={"openInterest": "PE OI", "changeinOpenInterest": "PE Change in OI",
                                          "lastPrice": "PE LTP", "change": "PE LTP Change", "totalTradedVolume": "PE Volume"})
            pe_df.index = pe_df["strikePrice"]
            pe_df = pe_df.drop("strikePrice", axis=1)
            df = pd.concat([ce_df, pe_df], axis=1).sort_index()
            df = df.replace(np.nan, 0)
            df["Strike"] = df.index
            df.index = [np.nan] * len(df)

            atm_strike = round(underlying_price / 50) * 50
            strike_prices = [atm_strike-250, atm_strike-200, atm_strike-150, atm_strike-100,  atm_strike-50, atm_strike, atm_strike+50, atm_strike+100, atm_strike+150, atm_strike+200, atm_strike+250]
            selected_rows = df[df['Strike'].isin(strike_prices)]
            df_new = pd.DataFrame(selected_rows)

            # print(df_new.head())
            sum_of_chng_oi_ce = df_new["CE Change in OI"].sum()
            sum_of_chng_oi_pe = df_new["PE Change in OI"].sum()
            if(prev_ce_value != sum_of_chng_oi_ce or prev_pe_value != sum_of_chng_oi_pe):
                prev_ce_value = sum_of_chng_oi_ce
                prev_pe_value = sum_of_chng_oi_pe
                local_df = pd.DataFrame()
                # local_data = {'CE Sellers' : [formatINR(sum_of_chng_oi_ce)],'Diff' : [formatINR(sum_of_chng_oi_pe - sum_of_chng_oi_ce)] ,'PE Selleres' :  [formatINR(sum_of_chng_oi_pe)]}
                # local_df = pd.DataFrame(local_data)

                curr_pcr = sum_of_chng_oi_pe - sum_of_chng_oi_ce
                db.insert_data(datetime.now(),curr_pcr)

                ldata = pd.DataFrame()
                ldata['CE Change in OI'] = df_new['CE Change in OI']
                ldata['Strike'] = df_new['Strike']
                ldata['PE Change in OI'] = df_new['PE Change in OI']
                # print(ldata.head())
                curr_date = datetime.now()
                fmt_date = curr_date.strftime('%Y-%m-%d %H:%M:%S')
                hour = curr_date.hour
                minute = curr_date.minute

                if(hour >= 9 and minute >=15):
                # if(hour >= 0 and minute >=0):
                    pred, conf = pred_obj.main(curr_pcr)
                    local_data = {'Date' : [fmt_date],'Pcr' : [formatINR(sum_of_chng_oi_pe - sum_of_chng_oi_ce)] ,'Pred' :  [pred], 'Conf' :  [conf]}
                    local_df = pd.DataFrame(local_data)
                    print(local_df.head())
                else:
                    local_data = {'Date' : [fmt_date],'Pcr' : [formatINR(sum_of_chng_oi_pe - sum_of_chng_oi_ce)] ,'Pred' :  [pred], 'Conf' :  [conf]}
                    local_df = pd.DataFrame(local_data)
                    print(local_df.head())

                print(" ")  
            time.sleep(5)

        except Exception as e:
            pass
