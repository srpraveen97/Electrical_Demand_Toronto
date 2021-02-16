
import pandas as pd
import psycopg2
import sys

from datetime import datetime

import os

data = pd.read_csv('Data/Toronto_climate.csv')

def strp(x):
    return x[:13]

data['date_time_local'] = data['date_time_local'].apply(strp)
data.rename(columns={'date_time_local':'timestamp'},inplace=True)
data = data.loc[::-1].reset_index(drop=True)

data.drop(['unixtime','pressure_station','pressure_sea','wind_dir', 'wind_dir_10s', 'wind_gust','dew_point',
           'windchill','visibility', 'health_index', 'cloud_cover_4', 'cloud_cover_10', 'solar_radiation',
            'humidex','max_air_temp_pst1hr','min_air_temp_pst1hr','relative_humidity','wind_speed','cloud_cover_8']
          ,axis=1,inplace=True)

data = data.set_index('timestamp').loc['2018':].reset_index()

data['temperature'] = data['temperature'].fillna(method='ffill')

data.drop_duplicates(subset='timestamp', keep="first",inplace=True)

def time_stamp(x):
    return datetime.strftime(datetime.strptime(x,'%Y-%m-%d %H'),'%Y-%m-%d %H:%M:%S')

data['timestamp'] = data['timestamp'].apply(time_stamp)

param_dic = {
    "host"      : "localhost",
    "database"  : "demand",
    "user"      : "postgres",
    "password"  : "password"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn

def copy_from_file(conn, df, table):
    """
    Here we are going save the dataframe on disk as 
    a csv file, load the csv file  
    and use copy_from() to copy it to the table
    """
    tmp_data = "tmp_dataframe.csv"
    data.to_csv(tmp_data, index=False, header=False)
    f = open(tmp_data, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_file() done")
    cursor.close()


conn = connect(param_dic)
copy_from_file(conn, data, 'temp_data')