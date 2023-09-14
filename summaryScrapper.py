#Method to scrap the Toll Information service
#using requests module to connect to the url to get the information
import requests
import pandas as pd
import re
import sqlite3

class NhaiSummaryScrapper:

    def __init__(self, dbname):
        self.dbname = dbname
        self.response = ''
        self.df = pd.DataFrame
        self.isDFCreated = False

    def fetchTableFromURL(self):
        #get the curl url command for the table that will be rendered during loading into the python format.
        cookies = {
        'ASP.NET_SessionId': '0oejx02ndkjatdzavvnezq0m',
        }

        headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json; charset=UTF-8',
        # 'Cookie': 'ASP.NET_SessionId=0oejx02ndkjatdzavvnezq0m',
        'Origin': 'https://tis.nhai.gov.in',
        'Referer': 'https://tis.nhai.gov.in/tollplazasataglance.aspx?language=en',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        }

        data = "{'TollName':''}"

        self.response = requests.post(
        'https://tis.nhai.gov.in/TollPlazaService.asmx/GetTollPlazaInfoGrid',
        cookies=cookies,
        headers=headers,
        data=data,
        )
        if len(self.response.text) == 0:
            return False
        
        return True

        
    def createDataFrame(self):
        if self.fetchTableFromURL():
            #change self.isDFCreated = False to True since we have creates the dataframe
            self.isDFCreated = True
            # Extract the table rows using regular expressions
            table_html = self.response.json()['d']
            table_rows = re.findall(r'<tr>(.*?)</tr>', table_html, re.DOTALL)
            # Create an empty list to store the rows of data
            data_rows = []

            # Iterate through each table row and extract the cell values
            for row in table_rows:
                cells = re.findall(r'<td>(.*?)</td>', row, re.DOTALL)
                data_rows.append(cells)

            # Create a DataFrame from the extracted data
            df = pd.DataFrame(data_rows)

            # Optionally, you can set column names based on your data
            column_names = ["Sr No.", "State", "NH-No.", "Toll Plaza Name", "Toll Plaza Location", "Section / Stretch"]
            df.columns = column_names
            df.dropna(axis = 0, inplace=True)
            self.df = df
        

            

    def cleaningDataFrame(self):
        if not self.df.empty:
            #Creating a new column that contains the toll plaza number
            self.df['Toll_Plaza_Num']= self.df['Toll Plaza Name'].str.extract(r'(\d+)').astype(int)
            #Creating a new column that contains the toll plaza name without the HTML formatting
            pattern = r'> (.*?)<'
            self.df['Toll_Plaza_Name'] = self.df['Toll Plaza Name'].str.extract(pattern)
            #Once we have successfully extracted the toll plaza name and toll plaza number, the original Toll Plaza Name column can be dropped
            self.df.drop(['Toll Plaza Name'],axis = 1, inplace = True)

    def saveDataFrameToSQL(self):
        conn = sqlite3.connect('TollPlazaProject.db')
        self.df.to_sql(self.dbname, conn, if_exists='replace', index = False)
        conn.commit()
        conn.close()

    def runSummaryPipeline(self):
        #Now transform this response object to a dataframe for further processing
        self.createDataFrame()
        if self.isDFCreated: 
            #Now clean the dataframe to add the toll number and toll name in proper order
            self.cleaningDataFrame()
            #Now save this dataframe to a database called nhai_toll_summary
            self.saveDataFrameToSQL()
            print('Successfully scrapped the NHAI summary portal')
        else:
            print('An error occured while processing the web scrapping request!')

if __name__ == '__main__':
    obj = NhaiSummaryScrapper('nhai_toll_summary')
    obj.runSummaryPipeline()