import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import sqlite3
class individualPlazaInfo:
    def __init__(self, revenueDbname, feeDbname, plazaId, verbose = True):
        self.revenueDbname = revenueDbname
        self.feeDbname = feeDbname
        self.plazaId = plazaId
        self.extracted = False
        self.verbose = verbose

    def fetchTableFromURL(self):
        #importing the data from  ('https://tis.nhai.gov.in/TollInformation.aspx?TollPlazaID={TollPlazaNUmber}')
        url = f'https://tis.nhai.gov.in/TollInformation.aspx?TollPlazaID={self.plazaId}'
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
        df_info = pd.read_html(toll_info_table_str)[1].set_index(0)
        self.df_info = df_info.T
        if not (self.fee_info.empty and self.misc_info.empty):
            self.extracted = True

    def cleanDataFrame(self):
        pattern = r'(\d{2}-[A-Za-z]+-\d{4})'
        self.df_info['Date of fee notification'] = self.df_info['Date of fee notification'].str.extract(pattern)
        pattern2 = r'(\d+)'
        to_correct_list = ['Cumulative Toll Revenue (in Rs. Cr.)', 'Target Traffic (PCU/day)', 'Traffic (PCU/day)', 'Fee Rule']
        for col in to_correct_list:
            self.df_info[col] = self.df_info[col].str.extract(pattern2)
        self.df_info['contactPhone'] = self.df_info['Name / Contact Details of Incharge'].str.extract(pattern2)
        pattern3 = r'(^\s*(.*?)\s*/)'
        self.df_info['contactName'] = self.df_info['Name / Contact Details of Incharge'].str.extract(pattern3)[1]
        self.df_info.drop('Name / Contact Details of Incharge', axis = 1,inplace =True)

    def insertIntoDataFrame(self):
        #Help extract the operation type such as public funded or private BOT toll
        pattern = r'\(([^)]+)\)*'
        text = re.findall(pattern, self.plaza_name)[-1]
        self.df_info['operation_type'] = text

        #To extract the name from plazaname to help merge with the summary table
        #remove this text from plazanama string to isolate the name of the plaza
        self.plaza_name = self.plaza_name.replace(text,'')
        pattern2 = r'([^(]+?)\s*\([^)]*\)'
        new_plaza_name = re.search(pattern2, self.plaza_name).group(1).strip()
        self.plaza_name = self.plaza_name.replace(new_plaza_name,'')
        pattern3 = r'\(\w+\)'
        new_plaza_name2 = re.findall(pattern3, self.plaza_name)
        if new_plaza_name2:
            new_plaza_name+= ' '+new_plaza_name2[0]
        self.plaza_name = new_plaza_name
        self.df_info['TollPlazaName'] = self.plaza_name
        self.df_info['TollPlazaNum'] = self.plazaId
        self.fee_info['TollPlazaName'] = self.plaza_name
        self.fee_info['TollPlazaNum'] = self.plazaId
        #self.fee_info.set_index('TollPlazaName', inplace = True)
    
    def saveDFToSQL(self, df, dbname):
        conn = sqlite3.connect('TollPlazaProject.db')
        #if the table already exists then append the new information to the existing database
        df.to_sql(dbname, conn, if_exists='append', index = False)
        conn.commit()
        conn.close()

    def runPlazaInfoPipeline(self):
        self.fetchTableFromURL()
        if self.extracted:
            self.cleanDataFrame()
            self.insertIntoDataFrame()
            self.saveDFToSQL(self.fee_info, self.feeDbname)
            self.saveDFToSQL(self.df_info, self.revenueDbname)
            if self.verbose:
                print(f"Successfully ran the pipeline for toll plaza id {self.plazaId}")
        else:
            print(f"Error! the table was not extracted successfully for {self.plazaId}")

if __name__ == '__main__':
    obj = individualPlazaInfo('rev_toll_info','fee_info', 430)
    obj.runPlazaInfoPipeline()