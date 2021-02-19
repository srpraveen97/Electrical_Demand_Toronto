
def sql_temppred_update():

    import psycopg2
    import sys
    
    import numpy as np
    import pandas as pd
    from datetime import date, datetime, timedelta
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    
    start = date.today()
    end = date.today() + timedelta(days=2)
    
    data = pd.DataFrame()
    
    options = webdriver.ChromeOptions();
    options.add_argument('headless');
    
    driver = webdriver.Chrome('chromedriver.exe',options=options)
    
    
    if datetime.today().hour == 23:
            
        print('Collecting data for: ', start + timedelta(days=1))
        formatted_lookup_URL = f"https://www.wunderground.com/hourly/ca/toronto-pearson-international/CYYZ/date/{start.year}-{start.month}-{start.day}"
        
        driver.get(formatted_lookup_URL)
        rows = WebDriverWait(driver, 60).until(EC.visibility_of_all_elements_located((By.XPATH, '//td[@class="mat-cell cdk-cell cdk-column-temperature mat-column-temperature ng-star-inserted"]')))
        
        for row in rows:
            temp = row.find_element_by_xpath('.//span[@class="wu-value wu-value-to"]').text
            data = data.append(pd.DataFrame({'Temperature':[temp]}),
                                     ignore_index = False)
    
    else:
        
        while start != end:
            
            print('Collecting data for: ', start)
            formatted_lookup_URL = f"https://www.wunderground.com/hourly/ca/toronto-pearson-international/CYYZ/date/{start.year}-{start.month}-{start.day}"
        
            start += timedelta(days=1)
            
            driver.get(formatted_lookup_URL)
            rows = WebDriverWait(driver, 60).until(EC.visibility_of_all_elements_located((By.XPATH, '//td[@class="mat-cell cdk-cell cdk-column-temperature mat-column-temperature ng-star-inserted"]')))
            
            for row in rows:
                temp = row.find_element_by_xpath('.//span[@class="wu-value wu-value-to"]').text
                data = data.append(pd.DataFrame({'Temperature':[temp]}),
                                         ignore_index = False)
            
    
    mylist = [datetime.strftime(date.today(),'%Y-%m-%d') + ' ' + str(hour) for hour in list(np.arange(datetime.today().hour+1,24))]
    mylist.extend([datetime.strftime(date.today()+ timedelta(days=1),'%Y-%m-%d') + ' ' + str(hour-1) for hour in list(np.arange(1,25))])
    
    time_list = [datetime.strftime(datetime.strptime(word,'%Y-%m-%d %H'),'%Y-%m-%d %H:%M:%S') for word in mylist]
    
    data['timestamp'] = time_list
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    
    data['temperature'] = (data['Temperature'].astype('int') - 32)/1.8
    data['temperature'] = data['temperature'].round(2)
    
    data.drop('Temperature',axis=1,inplace=True)
    
    data['time_collected'] = datetime.strftime(datetime.today(),'%Y-%m-%d %H:00:00')
    
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
        
    data = data.set_index('timestamp')
        
    data['year'] = data.index.year
    data['month'] = data.index.month
    data['dayofweek'] = data.index.dayofweek
    data['day'] = data.index.day
    data['hour'] = data.index.hour
    data['temp_index'] = data['temperature'].apply(temp_)
    data['week_index'] = data['dayofweek'].apply(weekday_)
    
    data = data.reset_index()
    
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
    
    conn = connect(param_dic)
    
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
                
    copy_from_file(conn, data, 'temp_pred')

