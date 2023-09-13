import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import sqlite3
class individualPlazaInfo:
    def __init__(self, dbname):
        self.dbname = dbname
        self.extracted = False

    def fetchTableFromURL(self):
        #importing the data from  ('https://tis.nhai.gov.in/TollInformation.aspx?TollPlazaID={TollPlazaNUmber}')
        url = 'https://tis.nhai.gov.in/TollInformation.aspx?TollPlazaID=5634'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        #Here the information we need can be obtained from class = "PA15"
        self.plaza_name = soup.find(class_="PA15").find_all('p')[0].find('lable').text
        #Fetch the table 
        toll_info_table = soup.find_all('table', class_='tollinfotbl')
        toll_info_table_str = str(toll_info_table)
        #Splitting the string of the toll_info_table into two tables. 1. for fee information 2. For Other information such as cost of production etc
        self.fee_info = pd.read_html(toll_info_table_str)[0].dropna(axis = 0, how = "all")
        self.fee_info.reset_index(inplace=True)
        self.misc_info = pd.read_html(toll_info_table_str)[1].set_index(0)
        if not (self.fee_info.empty and self.misc_info.empty):
            self.extracted = True
    
    def mergeTwoTables(self):
        for row in list(self.misc_info.index.values):
            self.fee_info[row] = self.misc_info.loc[row][1]

    def cleanDataFrame(self):
        #To extract the name from plazaname to help merge with the summary table
        pattern1 = r'^(.*?)\s*\(.+?\)\s*(?:\(.*\))*$'
        self.fee_info['toll_plaza_name'] = re.match(pattern1, self.plaza_name).group(1)
        #Help extract the operation type such as public funded or private BOT toll
        pattern2 = r'\(([^)]+)\)'
        self.fee_info['operation_type'] = re.findall(pattern2, self.plaza_name)[0]
    
    def saveFeeInfoToSQL(self):
        conn = sqlite3.connect(self.dbname+'.db')
        #if the table already exists then append the new information to the existing database
        self.fee_info.to_sql(self.dbname, conn, if_exists='append', index = False)
        conn.close()
    
    def runPlazaInfoPipeline(self):
        self.fetchTableFromURL()
        if self.extracted:
            self.mergeTwoTables()
            self.cleanDataFrame()
            self.saveFeeInfoToSQL()
        else:
            print("Error! the table was not extracted successfully")
