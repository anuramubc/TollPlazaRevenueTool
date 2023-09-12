from summaryScrapper import NhaiSummaryScrapper

#Extract the toll number for each toll plaza to use to obtain the revenue information from another url
def getTollNumber(obj):
    #Run the pipeline to generate the dataframe and save in on the database then return the toll plaza column for 
    obj.runSummaryPipeline()
    return obj.df['Toll_Plaza_Num']

obj = NhaiSummaryScrapper('nhai_toll_summary')
tollnum = getTollNumber(obj)
print(tollnum)