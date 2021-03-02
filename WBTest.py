from flatten_json import flatten 
import json
import os
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import shutil
import filecmp
import stat

#remove all the existing .csv file
folder = 'E:\\WBTest'
for filename in os.listdir(folder):
    if filename.endswith('.csv'):
        os.remove(filename)
   
#Read current directory path
dir_path = os.path.dirname(os.path.realpath(__file__))
lis_files = os.listdir()
#filtering .Json File
list_json_files = [i for i in lis_files if i.endswith('.json')]
#Create empty Dataframe where we will append all the json data
main_df = pd.DataFrame()
# Looping one by one  json file and created Sublist
for json_file_path in list_json_files:
    sub_lis = []
    with open(json_file_path, encoding='utf-8') as  f:  
        data = json.loads(f.read())  
        for d in data:
            sub_lis.append(flatten(d))
    #Forming dataframe for nested column
    df = pd.DataFrame.from_dict(sub_lis)  
    df.rename(columns={'attributes_0_QUANTITYORDERED' : 'QUANTITYORDERED',
                       'attributes_0_PRICEEACH': 'PRICEEACH', 'attributes_0_SALES': 'SALES',
                      'attributes_0_ORDERDATE' : 'ORDERDATE', 'attributes_0_STATUS': 'STATUS',
                       'attributes_0_PRODUCTLINE': 'PRODUCTLINE', 'attributes_0_MSRP' : 'MSRP'},
                        inplace =  True)
    #Seprated order date:year month day column                        
    df['ORDERDATE'] = pd.to_datetime(df['ORDERDATE'])
    df['Year']      = df['ORDERDATE'].dt.year
    df['Month']     = df['ORDERDATE'].dt.month
    df['Day']       = df['ORDERDATE'].dt.day
    main_df = pd.concat([df, main_df]) 
    dates = df.drop_duplicates(subset = ['Year', 'Month', 'Day'])
    #parllel looping of yr/date/month
    for year, month, day in zip(dates['Year'], dates['Month'], dates['Day']):
        day_df = df[(df['Month'] == month) & (df['Day'] == day)]
        file_dir_path = dir_path+'/ParquetFile/'+str(year)+'/'+str(month)+'/'+str(day)
        #Generated .parquet file based on existing order date
        if not os.path.exists(file_dir_path):
            os.makedirs(file_dir_path)
        file_path = file_dir_path + '/SLS.parquet'
        table = pa.Table.from_pandas(day_df)
        pq.write_table(table, file_path)

# Question2 
# Generated .csvfile for Further analysis
main_df.to_csv(r"E:\WBTest\Final_SLS_Data.csv", index = False)

df = pd.read_csv(r"E:\WBTest\Final_SLS_Data.csv")
# What is the total sales value of the cancelled orders?
df1 = df.groupby('STATUS')['SALES'].sum().to_frame()
df1.reset_index(inplace = True)
total_sale_cancel = df1[df1['STATUS'] == 'Cancelled']['SALES'][0]
print("total sales value of the cancelled orders :", total_sale_cancel)
#Answer : 83495.49

# What is the total sales value of the orders currently on hold for the year 2005?
df2 = df[(df['Year'] == 2005) & (df['STATUS'] == 'On Hold')]
df2 = df2.groupby('STATUS')['SALES'].sum().to_frame()
df2.reset_index(inplace = True)
total_sale_on_hold = df2['SALES'][0]
print("total sales value of the orders currently on hold for the year 2005 : ", total_sale_on_hold)
# Answer : 89623.28

# What is the count of distinct products per product line ?
df3 = df.drop_duplicates(subset = ['PRODUCTLINE','PRODUCTCODE'])
df3 = df3.groupby(['PRODUCTLINE'])['PRODUCTCODE'].count()
print("count of distinct products per product line :")
print(df3)
#Answer: 
# PRODUCTLINE
# Classic Cars        35
# Motorcycles         13
# Planes              12
# Ships                9
# Trains               3
# Trucks and Buses    10
# Vintage Cars        24

# What is the total sales variance for sales calculated at both sales price and MSRP (Manufacturer Suggested Retail Price)?
df['Total MSRP'] = df['MSRP']* df['QUANTITYORDERED']
df4 = df.sum(axis = 0, skipna = True, numeric_only= True)
print(df4)
variance = list(df4)[8] - list(df4)[3] 
print("total sales variance : ", variance)
# Answer: 374455.20999999996

# What has been the percentage change in sales YoY for classic cars, for years 2004 and 2005 where the status is shipped? 
df5 = df[((df['Year'] == 2005) | (df['Year'] == 2004)) & (df['STATUS'] == 'Shipped') & ( df['PRODUCTLINE'] =='Classic Cars')]
df5 = df5.groupby('Year')['SALES'].sum()
percent_change = (list(df5)[0] - list(df5)[1])*100/ (list(df5)[0])
print("percentage change in sales YoY for classic cars, for years 2004 and 2005",percent_change)
# Answer : 68.8

#Please apply the following transformations to both extracts:
# Adding calculating fields
new_sale_dis = []
msrp_price = []
for idx,row in df.iterrows():
    msrp_price.append(int(row['MSRP']) * row['QUANTITYORDERED']) 

# Dataset should be filtered for the following product lines; ‘Vintage Cars’, ‘Classic Cars’, ‘Motorcycles’, ‘Trucks and Buses’       
    if row['PRODUCTLINE'] in ['Vintage Cars', 'Classic Cars', 'Motorcycles', 'Trucks and Buses']:
        order_qty = row['QUANTITYORDERED']
        if 0 <= order_qty <= 30:
            new_sale_dis.append( int(row['SALES']))
        elif 30 < order_qty <= 60:
            new_sale = int(row['SALES']) * 0.975
            new_sale_dis.append(new_sale)
        elif 60 < order_qty <= 80:
            new_sale = int(row['SALES']) * 0.96
            new_sale_dis.append(new_sale)
        elif 80 <= order_qty <= 100:
            new_sale = int(row['SALES']) * 0.94
            new_sale_dis.append(new_sale)
        elif order_qty > 100:
            new_sale = int(row['SALES']) * 0.9
            new_sale_dis.append(new_sale)
        else:
            new_sale_dis.append(int(row['SALES']))
    else:
        new_sale_dis.append(int(row['SALES']))
# 2) Add calculated column by applying quantity-based price discounts using the following thresholds and recalculate sales:
df['DiscountedSale'] = new_sale_dis
# Add calculated column by recalculating sales using MSRP.
df['MSRPSale'] = msrp_price
df = df[['ORDERNUMBER', 'PRODUCTCODE', 'QUANTITYORDERED', 'PRICEEACH', 'SALES',
       'ORDERDATE', 'STATUS', 'PRODUCTLINE', 'MSRP', 'DiscountedSale', 'MSRPSale']]
print(df.head())
df.to_csv(r'E:\WBTest\SLS_disconted_data.csv', index = False)

