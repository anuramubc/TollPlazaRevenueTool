#Method to scrap the Toll Information service
#using requests module to connect to the url to get the information
import requests
import pandas as pd
import re
import sqlite3
#url = 'https://tis.nhai.gov.in/tollplazasataglance.aspx?language=en'
#response = requests.get(url)
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

response = requests.post(
    'https://tis.nhai.gov.in/TollPlazaService.asmx/GetTollPlazaInfoGrid',
    cookies=cookies,
    headers=headers,
    data=data,
)
#Extract the toll number for each toll plaza to use to obtain the revenue information from another url
def getTollNumber(response):
    #get the toll number from the text bound by 'javascript:TollPlazaPopup(5673)'. So use regex to source just the toll number
    intermediate_text = re.findall('javascript:TollPlazaPopup\(\d+\)', response.text)
    #Now apply regex on top og this intermediate text to obtain just the integers
    toll_num = [int(re.findall('\d+', toll_plaza)[0]) for toll_plaza in intermediate_text]
    return toll_num

conn = sqlite3.connect('nhai_toll_plaza_summary.db')
cursor = conn.cursor()
# Extract the table rows using regular expressions
table_html = response.json()['d']
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
df['Toll Plaza Location']
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
regex = r'\d+'
df['Toll_Plaza_Num']= df['Toll Plaza Name'].str.findall(regex)
print(df['Toll_Plaza_Num'])
#df.to_sql('nhai_toll_plaza_summary', conn, if_exists='append', index = False)

#Now store this dataframe into an sql database
