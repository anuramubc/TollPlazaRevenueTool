from summaryScrapper import NhaiSummaryScrapper
from plazaDetails import individualPlazaInfo

#Extract the toll number for each toll plaza to use to obtain the revenue information from another url
def getTollNumber(obj):
    #Run the pipeline to generate the dataframe and save in on the database then return the toll plaza column for 
    obj.runSummaryPipeline()
    return obj.df['Toll_Plaza_Num']

obj = NhaiSummaryScrapper('nhai_toll_summary')
obj2 = individualPlazaInfo('individual_toll_info')
obj2.runPlazaInfoPipeline()

#tollnum = getTollNumber(obj)
print(obj2.fee_info)