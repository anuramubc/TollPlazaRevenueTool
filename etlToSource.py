from summaryScrapper import NhaiSummaryScrapper
from plazaDetails import individualPlazaInfo
from functools import partial
import concurrent.futures
import sqlite3
class etlToSource:

    def __init__(self):
        pass
    #Extract the toll number for each toll plaza to use to obtain the revenue information from another url
    def getTollNumber(self,obj):
        #Run the pipeline to generate the dataframe and save in on the database then return the toll plaza column for 
        obj.runSummaryPipeline()
        return obj.df['Toll_Plaza_Num']

    def callIndividualPlazaInfo(self,tollNum,revenueDbname,feeDbname):
        obj = individualPlazaInfo(revenueDbname,feeDbname, tollNum)
        obj.runPlazaInfoPipeline()


    def completeETL(self):
        obj = NhaiSummaryScrapper('nhai_toll_summary')
        tollNum = self.getTollNumber(obj)
        partial_indiPlazaInfo = partial(self.callIndividualPlazaInfo, revenueDbname = 'r', feeDbname = 'f')
        with concurrent.futures.ThreadPoolExecutor(20) as executor:
            executor.map(partial_indiPlazaInfo, tollNum)
        
        conn = sqlite3.connect('TollPlazaProject.db')
        cur = conn.cursor()

        #check if both the table exists
        query = ''
        cur.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='r' " )
        if cur.fetchone()[0] == 1 :
            cur.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='f'" )
            if cur.fetchone()[0] == 1:
                print("Successfully scrapped and saved the data in the database")
            else:
                print("Error! The fees table from NHAI website was not saved to the database")
        else:
            print("Error! web scrapping and loading to database not successful")

if __name__ == '__main__':
    etlObj = etlToSource()
    etlObj.completeETL()