import findspark
findspark.init()

import pyspark

from pyspark.sql import SparkSession
from ETLSpark import sparkETLJob, col

class insightGen:
    
    def __init__(self):
        self.ETLSparkObj = sparkETLJob('nhai_toll_summary','revenueTable','feeTable', 'MergedSummaryRevenueTable')
        

    def connectToDB(self):
        self.driver : str = "org.sqlite.JDBC"
        self.spark = SparkSession.builder\
        .master("local")\
        .appName("SQLite JDBC")\
        .config(\
            "spark.jars",\
            '/opt/homebrew/Cellar/apache-spark/3.4.1/libexec/jars/sqlite-jdbc-3.34.0.jar')\
        .config(\
            "spark.driver.extraClassPath",\
            "/opt/homebrew/Cellar/apache-spark/3.4.1/libexec/jars/sqlite-jdbc-3.34.0.jar")\
        .getOrCreate()
        self.db_path: str = "/Users/anuram/Documents/Documents/TollPlazaRevenueTool/TollPlazaProject.db"
        #Table = 'MergedSummaryRevenueTable'
        self.jdbc_url = "jdbc:sqlite:" + self.db_path


    def loadSQLToDF(self, table_name):
        #jdbc_url: str = "jdbc:sqlite:" + self.db_path
        self.connectToDB()
        df = self.spark \
            .read \
            .format("jdbc") \
            .option("driver", self.driver) \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .load()
        return df
    
    def generateInsights(self):
        self.summaryDF = self.loadSQLToDF('MergedSummaryRevenueTable')
        cols = ('NH-No.', 'Toll_Plaza_Location', 'Toll_Plaza_Num', 'Toll_Plaza_Name', 'FeeNotificationDate','NameOfOperator', 'contactPhone', 'contactName','TollPlazaNum', 'Toll_Plaza_Num')
        self.insightDF = self.summaryDF.drop(*cols)
        """self.summaryDF.groupBy("State").count().show()
        self.insightDF = self.insightDF.withColumn("TotalCapitalCostPerState", self.summaryDF.groupBy("State").sum('CapitalCost'))
        self.insightDF.show()"""
        #self.insightsDF = self.summaryDF.groupBy("State").select(sum("CapitalCost")).show()
        #self.summaryDF.filter(self.summaryDF.State == 'Punjab').select("CapitalCost").show(100)
        #self.summaryDF.filter(self.summaryDF.CapitalCost == 1940422035).show()
        #select(sum('CapitalCost').alias('CumulativeCapitalCostPerState')).groupBy('State').show()
    


if __name__ == '__main__':
    obj = insightGen()
    obj.generateInsights()
    obj.generateInsights()
    obj.insightDF.show()


