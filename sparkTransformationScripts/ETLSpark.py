import findspark
findspark.init()

import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

class sparkETLJob:
    def __init__(self,summaryTable, revenueTable, feeTable, destinationTable):
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
        self.summaryTable = summaryTable
        self.revenueTable = revenueTable
        self.feeTable = feeTable
        self.destinationTable = destinationTable
        self.extracted = False
        self.jdbc_url = "jdbc:sqlite:" + self.db_path

    def loadSQLToDF(self, table_name):
        #jdbc_url: str = "jdbc:sqlite:" + self.db_path
        df = self.spark \
            .read \
            .format("jdbc") \
            .option("driver", self.driver) \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .load()
        return df
    
    def acquireAllDF(self):
        self.summaryDF = self.loadSQLToDF(self.summaryTable)
        self.revenueDF = self.loadSQLToDF(self.revenueTable)
        self.feeDF = self.loadSQLToDF(self.feeTable)
        if not (self.summaryDF.isEmpty() & self.revenueDF.isEmpty() & self.feeDF.isEmpty()):
            self.extracted = True
    
    def joinAndCleanDF(self):
        condition = [self.summaryDF['Toll_Plaza_Name'] == self.revenueDF['TollPlazaName'] , self.summaryDF['Toll_Plaza_Num'] == self.revenueDF['TollPlazaNum']]
        #drop the Sr.No , one of the tablePlazaName , concession period, section/stretch
        columns_to_drop = ('Sr No.', 'Section / Stretch', 'TollPlazaName', 'Concessions Period')
        self.mergedSumRevDF = self.summaryDF.join(self.revenueDF, condition, "inner").drop(*columns_to_drop)
        self.mergedSumRevDF = self.mergedSumRevDF.withColumn('operation_type', regexp_replace(col('operation_type'), r'\(Toll', ''))
        #Convert TragetTRaffic, Traffic, Design capacity, CumulativeToll revenue, capital code of project from string ro number

    def castTypeAndChangeColNames(self, old_name, new_name, new_dtype = None):
        self.mergedSumRevDF = self.mergedSumRevDF.withColumnRenamed(old_name, new_name)
        if data_type != None:
            self.mergedSumRevDF = self.mergedSumRevDF.withColumn(new_name, col(new_name).cast(new_dtype))
            self.mergedSumRevDF = self.mergedSumRevDF.fillna(0, subset = [new_name])
        #self.mergedSumRevDF.printSchema()

    def saveDFToSQL(self):
        connection_properties = {"driver": "org.sqlite.JDBC"}
        # Save the DataFrame to the SQLite database
        self.mergedSumRevDF.write.jdbc(url=self.jdbc_url, table=self.destinationTable, mode="overwrite", properties=connection_properties)
    

    
    def runSparkMergePipeline(self):
        self.acquireAllDF()
        if self.extracted:
            self.joinAndCleanDF()
            cols_changeToInt = [("Capital Cost of Project (in Rs. Cr.)", 'CapitalCost', 'int'), ("Cumulative Toll Revenue (in Rs. Cr.)", 'CumulativeTollRevenue', 'int'), \
                        ("Design Capacity (PCU)", 'DesignCapacity', 'int'), ("Traffic (PCU/day)", 'Traffic', 'int'), ("Target Traffic (PCU/day)", 'TargetTraffic', 'int'),\
                        ("Toll Plaza Location", 'Toll_Plaza_Location', None), ("Commercial Operation Data", "CommercialOperationDate", None), ("Date of fee notification", 'FeeNotificationDate', None),\
                        ("Fee Rule", "FeeRule",None ), ("Name of Concessionaire / OMT Contractor", "NameOfOperator", None) ]    
            for old_name, new_name, data_type in cols_changeToInt:
                sparkObj.castTypeAndChangeColNames(old_name, new_name, data_type)
            sparkObj.saveDFToSQL()
            print('Successfully merged, cleaned and saved data in the TollPlazaProject.db in the MergedSummaryRevenueTable')
        else:
            print('Error in merged, cleaned and saved data in the TollPlazaProject.db in the MergedSummaryRevenueTable')
        self.spark.stop()



    
if __name__ == '__main__':
    sparkObj = sparkETLJob('nhai_toll_summary','revenueTable','feeTable')
    sparkObj.acquireAllDF()
    sparkObj.joinAndCleanDF()
    cols_changeToInt = [("Capital Cost of Project (in Rs. Cr.)", 'CapitalCost', 'int'), ("Cumulative Toll Revenue (in Rs. Cr.)", 'CumulativeTollRevenue', 'int'), \
                        ("Design Capacity (PCU)", 'DesignCapacity', 'int'), ("Traffic (PCU/day)", 'Traffic', 'int'), ("Target Traffic (PCU/day)", 'TargetTraffic', 'int'),\
                        ("Toll Plaza Location", 'Toll_Plaza_Location', None), ("Commercial Operation Data", "CommercialOperationDate", None), ("Date of fee notification", 'FeeNotificationDate', None),\
                        ("Fee Rule", "FeeRule",None ), ("Name of Concessionaire / OMT Contractor", "NameOfOperator", None) ]    
    for old_name, new_name, data_type in cols_changeToInt:
        sparkObj.castTypeAndChangeColNames(old_name, new_name, data_type)
    sparkObj.saveDFToSQL('MergedSummaryRevenueTable')
    sparkObj.mergedSumRevDF.printSchema()
    sparkObj.mergedSumRevDF.show()
    #sparkObj.castTypeAndChangeColNames("`Cumulative Toll Revenue (in Rs. Cr.)`", 'int' ,'CumulativeTollRevenue')