import findspark
findspark.init()

import pyspark

from pyspark.sql import SparkSession
from ETLSpark import sparkETLJob
from pyspark.sql.functions import when, col, regexp_replace

class insightGen:
    
    def __init__(self, destinationTable):
        self.ETLSparkObj = sparkETLJob('nhai_toll_summary','revenueTable','feeTable', 'MergedSummaryRevenueTable')
        self.destinationTable = destinationTable
        

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
        self.connectToDB()
        df = self.spark \
            .read \
            .format("jdbc") \
            .option("driver", self.driver) \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .load()
        return df
    
    def countOperationType(self, op_type):
        df = self.summaryDF.filter(self.summaryDF.operation_type == op_type).groupBy('State').count().withColumnRenamed('count', 'TotNoOf'+op_type)
        return df
    

    def generateInsights(self):
        self.summaryDF = self.loadSQLToDF('MergedSummaryRevenueTable')
        countDF =self.summaryDF.groupBy('State').count().withColumnRenamed('count', 'NoOfTolls')
        cols_to_rename = ['State', 'TotCapitalCost','TotRevenue', 'TotDesignCapacity', 'TotTraffic', 'TotTargetTraffic']
        self.insightDF = self.summaryDF.groupBy('State').sum('CapitalCost', 'CumulativeTollRevenue', 'DesignCapacity','Traffic','TargetTraffic').toDF(*cols_to_rename)
        self.insightDF = self.insightDF.join(countDF, ['State'], 'left')
        self.insightDF = self.insightDF.sort('NoOfTolls')
        self.summaryDF = self.summaryDF.withColumn('operation_type', regexp_replace(col('operation_type'), r'\d+', 'Unknown'))
        self.summaryDF = self.summaryDF.withColumn('operation_type', regexp_replace(col('operation_type'), r'\s+', ''))
        distinctOpType = self.summaryDF.select("operation_type").distinct().rdd.flatMap(lambda x: x).collect()
        for optype in distinctOpType:
            df = self.countOperationType(optype)
            self.insightDF = self.insightDF.join(df, ['State'], 'left')
            self.insightDF = self.insightDF.fillna(0, subset = ['TotNoOf'+optype])
    
    def saveDFToSQL(self):
        connection_properties = {"driver": "org.sqlite.JDBC"}
        # Save the DataFrame to the SQLite database
        try:
            self.insightDF.write.jdbc(url=self.jdbc_url, table=self.destinationTable, mode="overwrite", properties=connection_properties)
            print('Successfully saved the insights database in InsightsTable in TollPlazaProject.db')
        except:
            print('Error! Insights file was not generated successfully')
   
    def runInsightsPipeline(self):
        self.generateInsights()
        self.saveDFToSQL()
        self.spark.stop()


if __name__ == '__main__':
    obj = insightGen('InsightsTable')
    obj.runInsightsPipeline()

