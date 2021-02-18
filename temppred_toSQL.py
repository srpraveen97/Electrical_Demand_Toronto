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

driver = webdriver.Chrome('chromedriver.exe')


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
        

# mylist = [datetime.strftime(date.today(),'%Y-%m-%d') + ' ' + str(hour) for hour in list(np.arange(datetime.today().hour+1,24))]
# mylist.extend([datetime.strftime(date.today()+ timedelta(days=1),'%Y-%m-%d') + ' ' + str(hour) for hour in list(np.arange(1,25))])

# time_list = [datetime.strftime(datetime.strptime(word,'%Y-%m-%d %H'),'%Y-%m-%d %H:%M:%S') for word in mylist]


# data['timestamp'] = mylist
# data['timestamp'] = pd.to_datetime(data['timestamp'])