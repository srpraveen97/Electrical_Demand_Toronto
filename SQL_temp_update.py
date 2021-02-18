
def sql_temp_update():
    
    """
    Checks and updates the temperature database
    """

    import time
    from selenium import webdriver
    import psycopg2
    import sys
    import pandas as pd
    from datetime import datetime
    import os
    import math
    
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
    
    def postgresql_to_dataframe(conn, select_query, column_names):
        """
        Tranform a SELECT query into a pandas dataframe
        """
        cursor = conn.cursor()
        try:
            cursor.execute(select_query)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)  
            cursor.close()
            return 1
        
        tupples = cursor.fetchall()
        cursor.close()
        
        data = pd.DataFrame(tupples, columns=column_names)
        return data
    
    column_names = ['timestamp', 'temperature']
    conn = connect(param_dic)
    
    data = postgresql_to_dataframe(conn, "select timestamp,temperature from temp_data", column_names)
        
    delta = math.floor(((datetime.today() - data['timestamp'].iloc[-1]).total_seconds())/3600)
    status = True
    
    if delta >= 1:
        
        while status:
            driver = webdriver.Chrome('D:\\Personal_projects\\Toronto_Electrical_Demand_Analysis\\chromedriver')
            driver.get("https://toronto.weatherstats.ca/download.html")
            
            driver.find_element_by_xpath('/html/body/div[2]/div/form/label[2]/input').click()
            
            driver.find_element_by_xpath('/html/body/div[2]/div/form/label[9]/input').clear()
            driver.find_element_by_xpath('/html/body/div[2]/div/form/label[9]/input').send_keys(delta)
            
            driver.find_element_by_xpath('/html/body/div[2]/div/form/button').click()
            
            time.sleep(10)
            driver.close()
            
            try:
                df = pd.read_csv('C:\\Users\\srpra\\Downloads\\weatherstats_toronto_hourly.csv')
                break
            except:
                time.sleep(60)
                os.remove('C:\\Users\\srpra\\Downloads\\weatherstats_toronto_daily.csv')
                status = True
        
        def strp(x):
            return x[:13]
        
        df['date_time_local'] = df['date_time_local'].apply(strp)
        df.rename(columns={'date_time_local':'timestamp'},inplace=True)
        df = df.loc[::-1].reset_index(drop=True)
        
        df.drop(['unixtime','pressure_station','pressure_sea','wind_dir', 'wind_dir_10s', 'wind_gust','dew_point',
                    'windchill','visibility', 'health_index', 'cloud_cover_4', 'cloud_cover_10', 'solar_radiation',
                    'humidex','max_air_temp_pst1hr','min_air_temp_pst1hr','relative_humidity','wind_speed','cloud_cover_8']
                  ,axis=1,inplace=True)
        
        df['temperature'] = df['temperature'].fillna(method='ffill')
        
        def time_stamp(x):
            return datetime.strftime(datetime.strptime(x,'%Y-%m-%d %H'),'%Y-%m-%d %H:%M:%S')
        
        def temp_(x):
            """
            Divides the dataset into two categories based on the temperature
            """
            if x > 15:
                return 'Hot'
            else:
                return 'NotHot'
            
        def weekday_(x):
            """
            Divides the dataset into two categories based on the day of the week
            """
            if x >= 5:
                return 'Weekend'
            else:
                return 'Weekday'
        
        df['timestamp'] = df['timestamp'].apply(time_stamp)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        
        df['year'] = df.index.year
        df['month'] = df.index.month
        df['dayofweek'] = df.index.dayofweek
        df['day'] = df.index.day
        df['hour'] = df.index.hour
        df['temp_index'] = df['temperature'].apply(temp_)
        df['week_index'] = df['dayofweek'].apply(weekday_)
        
        df = df.reset_index()
        
        def copy_from_file(conn, data, table):
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
            
        copy_from_file(conn, df, 'temp_data')
        
        os.remove('C:\\Users\\srpra\\Downloads\\weatherstats_toronto_hourly.csv')
    
        
