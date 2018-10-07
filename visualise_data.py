# -*- coding: utf-8 -*-
"""
Created on Mon Sep 17 06:22:52 2018

@author: nb
"""

import pandas as pd
from fuzzywuzzy import process

### 1) INPUT - a file
## try importing file name as csv, throw error otherwise - to be expanded later

data = pd.read_csv(r'C:\Users\nb\Documents\coding\skunkworks\data_sets\avocado_v2.csv', sep=';',encoding='utf-8',na_values= ['',' ', '  ', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null'])

### 2) get the attributes of all columns of the data
## data type
## sparcity
## number of values - ordinality - count
## numbers - mean,std,min,max,25%,50%,75%,
## strings - average length

def get_attributes(data):
    # sparcity - read_csv "By default the following values are interpreted as NaN: ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘n/a’, ‘nan’, ‘null’"
    length_data = len(data)
    number_rows = data.count()
    sparcity = number_rows/length_data

    # data types - convert to date, date_time or string. Note the first line is probably not needed
    data[data.select_dtypes(include=['object']).columns] = data.select_dtypes(include=['object']).astype(str) 
    for cols in data.select_dtypes(include=['object']).columns:
        try:
            data[cols] = pd.to_datetime(data[cols])
        except ValueError as verr:
            print('Cannot convert column {}'.format(cols))
            pass
    
    data_types = data.dtypes
    
    # ordinality
    ordinality = data.apply(lambda x: x.nunique(),axis=0)
    ordinality = ordinality/length_data
    
    # average length - when converting all variables to strings
    avg_length = data.fillna('').astype(str).apply(lambda x:x.str.len()).mean()
    
    # float/int statistics
    desc = data.describe()
    desc = desc.T
    
    ## Place all this information in a dataframe
    data_attributes = pd.DataFrame(dict(sparcity = sparcity, data_types = data_types,ordinality=ordinality,avg_length=avg_length)).reset_index()
    print(data_attributes)
    data_attributes.set_index('index',inplace=True)
    data_attributes = pd.concat([data_attributes, desc], axis=1, join='outer')
    return data_attributes

### 3) classify the type of data: string, string geographical, int, double, longtituge/latitude, 

def get_column_types(data,data_attributes):
    data_attributes['type'] = data_attributes['data_types']
    ## Fuzzy matching and regular expressions to extract types of what string contains
    # Possible strings for week or month - timeseries datetime or string fuzzy matching (day or month (abbreviated or not))
    days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    days_abb = ['Mon','Tues','Wed','Thurs','Fri','Sat','Sun']
    month = ['January','February','March','April','May','June','July','August','September','October','November','December']
    month_abb = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    # Possible strings for locations in UK: boroughs, wards, cities, and postcodes
    boroughs = pd.read_csv(r'C:\Users\nb\Documents\coding\skunkworks\Interim_Local_Authority_Districts_April_2018_Names_and_Codes_in_the_United_Kingdom.csv',skipfooter=2)
    wards = pd.read_csv(r'C:\Users\nb\Documents\coding\skunkworks\Wards_December_2017_Names_and_Codes_in_the_United_Kingdom.csv',skipfooter=2)
    #cities_uk = []
    
    # Fuzzy matching and regex
    data_temp = pd.DataFrame()
    threshold=90
    
    for cols in data.select_dtypes(include=['object']).columns:
        print(cols)
        print('\n')  
        #postcode
        if(data[cols].str.match('^([A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]? {1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA)$', case=False).all()):
            data_attributes['type'][cols] = 'postcode'
            continue
        #percentage - % sign 
        if(data[cols].str.match('\d+\.?\d*\s?%').all()):
            data_attributes['type'][cols] = 'percentage'
            continue
        #day and month strings
        data_temp['full']=data[cols].apply(lambda x: process.extractOne(x, days)[1])
        data_temp['abb']=data[cols].apply(lambda x: process.extractOne(x, days_abb)[1])
        if(data_temp['full'].mean()>threshold or data_temp['abb'].mean()>threshold):
            data_attributes['type'][cols] = 'days'
            continue
        data_temp['full']=data[cols].apply(lambda x: process.extractOne(x, month)[1])
        data_temp['abb']=data[cols].apply(lambda x: process.extractOne(x, month_abb)[1])
        if(data_temp['full'].mean()>threshold or data_temp['abb'].mean()>threshold):
            data_attributes['type'][cols] = 'month'
            continue
    #    data_temp['full']=data[cols].apply(lambda x: process.extractOne(x, boroughs['LAD18NM'])[1])
    #    if(data_temp['full'].mean()>threshold):
    #        data_attributes['type'][cols] = 'boroughs'
    #        continue
    #    data_temp['abb']=data[cols].apply(lambda x: process.extractOne(x, boroughs['LAD18CD'])[1])
    #    if(data_temp['abb'].mean()>threshold):
    #        data_attributes['type'][cols] = 'borough code'
    #        continue
    #    data_temp['full']=data[cols].apply(lambda x: process.extractOne(x, wards['WD17NM'])[1])
    #    if(data_temp['full'].mean()>threshold):
    #        data_attributes['type'][cols] = 'wards'
    #        continue
    #    data_temp['abb']=data[cols].apply(lambda x: process.extractOne(x, wards['WD17CD'])[1])
    #    if(data_temp['abb'].mean()>threshold):
    #        data_attributes['type'][cols] = 'wards code'
    #        continue
    del data_temp
    
    ## Looking at int types for logtitude/latitude or year
    for cols in data.select_dtypes(include=['int64']).columns:
        #check if year
        if('year' in cols.lower() and data[cols].between(1000, 3000).all()):
            data_attributes['type'][cols] = 'year'
        #check if longitude or latitude - column name and values within a certain range
        if(process.extractOne(cols.lower(),['long','longitude'])[1] >95 and data[cols].between(-180, 180).all()):
            data_attributes['type'][cols] = 'longitude'
        if(process.extractOne(cols.lower(),['lat','latitude'])[1] >95 and data[cols].between(-180, 180).all()):
            data_attributes['type'][cols] = 'latitude'
    
    ## Checking if proportion
    for cols in data.select_dtypes(include=['float64']).columns:
        if(data[cols].between(0, 1,inclusive=True).all()):
            data_attributes['type'][cols] = 'proportion'
        
    return data_attributes

## Looking at float types proportion percentage
# frequency (not possible), proportion, percentage - if number than '%', year
# proportion - majority 95% of numbers between 0&1 or percent - % sign
data_attributes = get_attributes(data)
data_attributes = get_column_types(data,data_attributes)
print(data_attributes)         
correlation = data.corr(method='pearson', min_periods=1)   

### 4) Tensorflow to push out type of visualisation 



