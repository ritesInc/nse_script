#!/usr/bin/env python
# coding: utf-8

# In[50]:


import pandas as pd
from pandas.tseries.offsets import BDay
PATH = "new_symbols_csv/"
PAST_DAYS = 4


# In[51]:


class NationalStockData():
    
    def getCSVData(self, url_list):
        nse_df_dict = {}
        for url in url_list:
            try:
                #print(url)
                nse_df_dict[url] = pd.read_csv(url, usecols=["SYMBOL","SERIES","OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE","TOTTRDQTY","TIMESTAMP"])
                del nse_df_dict[url]['Unnamed: 13']
            except Exception as e:
                pass
                #print("Can't Access requested URL : {}".format(url))
                #print(e)

        return nse_df_dict
    
    def generateSymbolDataKeys(self, nse_df_dict):
        symbol_data_keys = []
        for df in nse_df_dict.keys():
            for symbol in nse_df_dict[df]['SYMBOL']:
                symbol_data_keys.append(symbol)

        return symbol_data_keys    


# In[52]:


class URLHelper():
    
    def __init__(self, past_days):
        self.past_days = past_days
        
    def getWorkingDates(self):
        date_range = []
        today = pd.datetime.today()
        for day_no in range(self.past_days):
            last_working_date = str((today - BDay(day_no)).strftime("%d %b %Y")).upper()
            date_range.append(last_working_date)

        return date_range

    def URLGenerator(self, date):
        date_part = date.split(" ")
        url_value = "https://archives.nseindia.com/content/historical/EQUITIES/"+ date_part[2] +"/"+ date_part[1] +"/cm"+ date_part[0]+date_part[1]+date_part[2] +"bhav.csv.zip"
        return url_value

    def getAllURLs(self):
        date_range_list = self.getWorkingDates()
        url_list = []
        for date in date_range_list:
            #print(URLGenerator(date))
            url_list.append(self.URLGenerator(date))

        return url_list


# In[ ]:





# In[ ]:





# In[53]:


if __name__ == '__main__':
    
    url_helper = URLHelper(PAST_DAYS)
    url_list = url_helper.getAllURLs()
    
    # now fetching all url csv files in multiple dataframe
    nsd = NationalStockData()
    nse_df_dict = nsd.getCSVData(url_list)
    
    # dict for holding all symbols multiple files data
    if(bool(nse_df_dict)):
        
        symbols = nsd.generateSymbolDataKeys(nse_df_dict)
        symbol_data = dict.fromkeys(nsd.generateSymbolDataKeys(nse_df_dict), [])

        #creating list of list for each symbol in csv file
        symbol_list = []
        for df in nse_df_dict.keys():
            symbol_df_list = nse_df_dict[df].values.tolist()
            symbol_list.append(symbol_df_list)

        #making main dict to make df at end
        for outer_list in symbol_list:
            for inner_list in outer_list:
                try:
                    symbol_data[inner_list[0]] = symbol_data[inner_list[0]] + [inner_list]
                except:
                    pass

        #creating csv files for each symbol
        for symbol in symbols:
            symbol_df = pd.DataFrame(symbol_data[symbol], columns=["SYMBOL","SERIES","OPEN","HIGH","LOW","CLOSE","LAST","PREVCLOSE","TOTTRDQTY","TIMESTAMP"])
            symbol_df.to_csv(PATH+symbol+".csv", index=False)
    else:
        print("No Data Found in all URLs")
    
else:
    pass

