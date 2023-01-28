import psycopg2 
import pandas as pd 
import numpy as np

"""params_dic = {
    "host"      : "localhost",
    "dbname"    : "flights",
    "user"      : "postgres",
    "password"  : "thierno",
    "port"      : "5432"     
}

#connect to database
conn = psycopg2.connect(**params_dic)
cursor = conn.cursor()
cursor.execute("SELECT * FROM real_flight WHERE cancelled = '0' AND diverted = '0'; " )"""

# pulling data using psycopg2 and saving it into a pandas dataframe. 
# Then selecting rows for flights that were not diverted nor cancelled using SQL query
dbhost = 'localhost'
dbname = 'flights'
dbuser = 'postgres'
dbpassword = 'thierno'
dbport = '5432'

conn = psycopg2.connect(host=dbhost, dbname=dbname, user=dbuser, password=dbpassword, port=dbport )
cursor = conn.cursor()
cursor.execute("SELECT * FROM real_flight WHERE cancelled = '0' AND diverted = '0' ;" )

# fetch all rows 
row= cursor.fetchall()
#print(row)

cursor.close()
conn.close()

df = pd.DataFrame(row, columns=[desc.name for desc in cursor.description])
#print(df.head())

# using pandas to clean the data by dropping all the 'NaN' values in arr_del15 or dep_del15
cleaned_df = df.dropna(subset=['arr_del15', 'dep_del15'])
print(cleaned_df)

# creating a new column called `DELAYED` 
# This column will be marked as `1` if either `ARR_DEL15` or `DEP_DEL15` are `True`, 
# and be marked as `0` if both `ARR_DEL15` and `DEP_DEL15` are `False`
df['DELAYED'] = np.where((df['arr_del15'] == df['dep_del15']), np.where(df['arr_del15']== '1',1,0), np.nan)
print(df['DELAYED'])
#print(df.head())

# creating a new dataframe that groups each airline (`OP_UNIQUE_CARRIER`) into groups  
# Then calculating the ratio of delays (`DELAYED`) for each airline.
group1= df.groupby(['op_unique_carrier'])['DELAYED'].mean()
print(group1)

#Sort the delay ratio from highest to lowest, and save into a csv
group1.sort_values(inplace=True, ascending=False)
group1.to_csv('delayed_airlines.csv',index=False)

#grouping each airline (`OP_UNIQUE_CARRIER`) into groups and calculating the the ratio of delays (`DELAYED`)for each aiport
group2= df.groupby(['origin_airport_id'])['DELAYED'].mean()
print(group2)

#Sort the delay ratio from highest to lowest, and save into a csv 
group2.sort_values(inplace=True, ascending=False)
group2.to_csv('delayed_airports.csv',index=False) 

