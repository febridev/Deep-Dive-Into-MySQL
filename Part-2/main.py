import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine,text

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url_csv = params.url_csv
    partition = params.partition

    # OPEN CONNECTION
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}')

    # SET DATAFRAME
    newcol=['uniq_id','price','date_of_transfer','postcode','property_type','old_new'
          ,'duration','paon','saon','street','locality'
          ,'town','district','county','ppd_category','r_status']
    df1 = pd.read_csv(url_csv,iterator=True, chunksize=1000000)
    i=0
    while i < 1000000:
        df = next(df1)
        df.to_csv('smallcsv.csv', index=False)
        i+=1000000
        print('inserted chunk ...')


    df_iter = pd.read_csv('smallcsv.csv',names=newcol,header=0)
    # tempdf = df_iter.rename(columns={df_iter.columns[0]: 'uniq_id', 
    #                                  df_iter.columns[1]: 'price',
    #                                  df_iter.columns[2]: 'date_of_transfer',
    #                                  df_iter.columns[3]: 'postcode',
    #                                  df_iter.columns[4]: 'property_type',
    #                                  df_iter.columns[5]: 'old_new',
    #                                  df_iter.columns[6]: 'duration',
    #                                  df_iter.columns[7]: 'paon',
    #                                  df_iter.columns[8]: 'saon',
    #                                  df_iter.columns[9]: 'street',
    #                                  df_iter.columns[10]: 'locality',
    #                                  df_iter.columns[11]: 'town',
    #                                  df_iter.columns[12]: 'district',
    #                                  df_iter.columns[13]: 'county',
    #                                  df_iter.columns[14]: 'ppd_category',
    #                                  df_iter.columns[15]: 'r_status'})

    # print(df_iter)

    # DROP IF EXSITS
    dropsql = f"DROP TABLE IF EXISTS {db}.{table};"

    # CREATE TABLE
    setsql = f"""CREATE TABLE IF NOT EXISTS {db}.{table} (`seq_id` int NOT NULL AUTO_INCREMENT,`uniq_id` text,                                                                        
               `price` decimal(18,0) DEFAULT NULL,                                                    
               `date_of_transfer` datetime DEFAULT NULL,                                              
               `postcode` varchar(25) DEFAULT NULL,                                                   
               `property_type` varchar(5) DEFAULT NULL,                                               
               `old_new` varchar(5) DEFAULT NULL,                                                     
               `duration` varchar(5) DEFAULT NULL,                                                    
               `paon` varchar(200) DEFAULT NULL,                                                      
               `saon` varchar(200) DEFAULT NULL,                                                      
               `street` varchar(200) DEFAULT NULL,                                                    
               `locality` varchar(200) DEFAULT NULL,                                                  
               `town` varchar(200) DEFAULT NULL,                                                      
               `district` varchar(200) DEFAULT NULL,                                                  
               `county` varchar(200) DEFAULT NULL,                                                    
               `ppd_category` varchar(200) DEFAULT NULL,                                              
               `r_status` varchar(5) DEFAULT NULL,                                                    
               PRIMARY KEY (`seq_id`)) ENGINE=InnoDB AUTO_INCREMENT=664101 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"""
    
    with engine.connect() as dbcc:
        dbcc.execute(text(dropsql))
        dbcc.execute(text(setsql))




    # df_iter.Columns = newcol
    # df_iter.head(n=1)
    # df = next(df_iter)

    # CREATE TABLE 
    # df.columns = ["column1","column2"]
    # print(df.head(n=0))
    # df.head(n=0).to_sql(name=table, con=engine, if_exists='fail')
    # print(df.head(n=0))
    print(df_iter.head(n=0))
    # print(df)
    # df.to_sql(name=table, con=engine, if_exists='append')
    chunksize=int(partition)
    for i in range(0, len(df_iter), chunksize):
        # print(tempdf.iloc[i:i+chunksize])
        df_iter.iloc[i:i+chunksize].to_sql(name=table, con=engine, if_exists='append', index=False)
        print('inserted chunk ...')
    # while True:
        # df = next(df_iter)
        # df.to_sql(name=table, con=engine, if_exists='append', index=False)
        # print('inserted chunk ...')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to MySQL')
    parser.add_argument('--user', help='user name for mysql')
    parser.add_argument('--password', help='password for mysql')
    parser.add_argument('--host', help='host for mysql')
    parser.add_argument('--port', help='port for mysql')
    parser.add_argument('--db', help='database name for mysql')
    parser.add_argument('--table', help='table name for mysql')
    parser.add_argument('--url_csv', help='url for csv')
    parser.add_argument('--partition', help='how much data to insert')
    args = parser.parse_args()

    main(args)
