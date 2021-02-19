
def sql_demand_update():
    
    """ 
    Checks and updates the demand database 
    """
    
    import psycopg2
    import sys
    
    import requests
    from bs4 import BeautifulSoup
    import re
    from urllib import request
    
    import pandas as pd
    
    from datetime import datetime,timedelta
    
    import os
    
    r = requests.Session().get('http://reports.ieso.ca/public/DemandZonal/')
    soup = BeautifulSoup(r.content, 'html.parser')
    
    years_list = []
    [years_list.extend(list(set(re.findall(r'20[0-2][0-9]',str(soup.select("a[href$='.csv']")[item]))))) for item in range(len(soup.select("a[href$='.csv']")))]
    unique_list = list(set(years_list))
    unique_list.sort()
    unique_list = [item for item in unique_list if int(item)>=2021]
    
    index = [max(loc for loc, val in enumerate(years_list) if val == item)+1 for item in unique_list]
    
    relevant_files = [soup.select("a[href$='.csv']")[item] for item in index]
    
    for file in range(len(relevant_files)):
        
        response = request.urlopen("http://reports.ieso.ca/public/DemandZonal/" + relevant_files[file]['href'])
        csv = response.read()
    
        csvstr = str(csv).strip("b'")
        lines = csvstr.split("\\n")
        f = open("Data\\Daily\\Demand_" + unique_list[file] + ".csv", "w")
        for line in lines:
           f.write(line + "\n")
        f.close()
        
    def time_stamp(x,y):
        return datetime.strftime(datetime.strptime(x + ' ' + str(y-1),'%Y-%m-%d %H'),'%Y-%m-%d %H:%M:%S')
    
    data1 = pd.read_csv('Data\\Daily\\Demand_2021.csv',header=3)
    data1['timestamp'] = data1.apply(lambda x: time_stamp(x['Date'], x['Hour']), axis=1)
    
    data1 = data1.set_index('timestamp').loc[datetime.strftime(datetime.today() - timedelta(days=1),'%Y-%m-%d'):].reset_index()
    
    data = pd.DataFrame()
    
    data['timestamp'] = data1['timestamp']
    data['demand'] = data1['Toronto']
    
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    
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
    copy_from_file(conn, data, 'demand_data')
    
    os.remove("Data\\Daily\\Demand_" + unique_list[file] + ".csv")

