from kiteconnect import KiteConnect
import json
import logging

logging.basicConfig(level=logging.DEBUG)
CRED_CONFIG_PATH = "../config/cred.json"

class Access_Token():

    def __init__(self):
        with open(CRED_CONFIG_PATH, 'r') as f:
            cred_config = json.load(f)
        self.key = cred_config['key'] 
        self.secret = cred_config['secret'] 
        self.kite = KiteConnect(api_key=self.key)
        logging.info("Login here : "+self.kite.login_url())
        self.req_token = input("Enter request token here : ")
        self.gen_ssn = self.kite.generate_session(request_token=self.req_token, api_secret=self.secret)
        self.acc_tkn = self.gen_ssn['access_token']
        logging.info(self.acc_tkn)


    def write_json(self):
        dict_cred = {
            'key' : self.key,
            'secret' : self.secret,
            'access_tkn' : self.acc_tkn
        }
        cred_object = json.dumps(dict_cred, indent = 3)
        with open(CRED_CONFIG_PATH, "w") as outfile:
            outfile.write(cred_object)


def main():
    access_token = Access_Token()
    access_token.write_json()


if __name__=="__main__":
    main()



# https://kite.zerodha.com/connect/login?api_key=czwkcv66rtnvliol&v=3
    

