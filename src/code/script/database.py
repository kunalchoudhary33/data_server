import sqlite3 
import pandas as pd
from datetime import datetime

class Database():

    def __init__(self):
        self.conn = sqlite3.connect('oi_data.db') 
        self.cursor = self.conn.cursor()
        # self.create_table()


    def create_table(self):
        table ="""CREATE TABLE OIDATA(DATE TIMESTAMP, PCR NUMBER);"""
        self.cursor.execute(table) 
        self.conn.commit()
    
    

    def insert_data(self, date, oidata):
        try:
            dt = datetime.now()
            date = dt.strftime('%Y-%m-%d %H:%M:%S')
            self.cursor.execute("INSERT INTO OIDATA (DATE, PCR) VALUES (?,?)", (date,str(oidata)))
            self.conn.commit()
            # self.display_data()
        except Exception as ex:
            print(ex)
            # print(traceback.print_exc())

    def get_pcr(self):
        data=self.cursor.execute('''SELECT * FROM OIDATA''') 
        data = self.cursor.fetchall()
        columns = [column[0] for column in self.cursor.description]
        df = pd.DataFrame(data, columns=columns)
        pcr_ls = df['PCR'].to_list()
        return pcr_ls[-1]

    def display_data(self):
        data=self.cursor.execute('''SELECT * FROM OIDATA''') 
        for row in data: 
            print(row) 
        
        # self.conn.close()


# db = Database()
# print(db.get_pcr())
# db.display_data()