
import streamlit as st

import numpy as np
import pandas as pd
from scipy import stats
import os
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime,timedelta
import pytz
import time

import psycopg2
import sys

import plotly.express as px
import plotly.graph_objects as go

import holidays

sns.set()
sns.set_style('whitegrid')

from sklearn.linear_model import Ridge

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
        # connect to the PostgreSQL server
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

def deal_with_holidays():
    
    holiday = holidays.Canada()
    hol_list = holiday['2018':'2021']
    for date in hol_list:
        data.loc[datetime.strftime(date,'%Y-%m-%d'),'week_index'] = 'Weekened'

def my_data(data,pred_start,last_date):   
    
    train_end = (datetime.strptime(pred_start,'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    
    pred_day = data.loc[pred_start:last_date]
    train_days = data.loc[:train_end]   
    
    return pred_day,train_days
    

def predict(data,group_by,ridge_coef):
    """
    Given, a data point, the groups by which we split and the regression coefficients, this function returns a prediction
    """
    group = tuple(data[group_by])
    idx = groupby_list.index(group)
    (inter,slope) = ridge_coef[idx]
    return inter + slope*data['temperature']

def RMSE_error(data,forecast):
    """
    This function gives the RMSE for the forecats
    """
    return np.sqrt((1/data.shape[0])*np.sum(np.square(data-forecast)))

def Ridge_error(data):
    """
    Based on the provided data, this function returns the training error while performing ridge regression
    """
    x = data['temperature'].values.reshape(-1,1)
    y = data['demand'].values.reshape(-1,1)
    model = Ridge().fit(x,y)
    train_error = np.sqrt((1/y.shape[0])*np.sum(np.square(np.squeeze(model.predict(x)) - y.T)))
    return (round(float(train_error),3))

def Ridge_model(data):
    """
    Based on the provided data, this function returns the 
    coefficients of the ridge regression.
    
    input: the training data
    output: ridge regression coefficients
    
    """
    data = data.sample(frac=1,random_state=42)
    x = data['temperature'].values.reshape(-1,1)
    y = data['demand'].values.reshape(-1,1)
    model = Ridge().fit(x, y)
    return (round(float(model.intercept_),3),round(float(model.coef_),3))

def pred_interval(prediction,test_data,test_predictions,alpha=0.95):
    """
    Obtain the prediction interval for each of the prediction
    Input: single prediction, entire test data, test set predictions
    Output: Prediction intervals and the actual prediction
    """
    y_test = test_data['demand']
    test_predictions = np.array(test_predictions)
    err = np.sum(np.square((y_test - test_predictions)))
    std = np.sqrt((1 / (y_test.shape[0] - 2)) * err)
    z = stats.norm.ppf(1 - (1-alpha)/2)
    interval = z*std
    
    return [float(prediction-interval),float(prediction),float(prediction+interval)]


conn = connect(param_dic)

column_names = ['timestamp', 'hour', 'temp_index', 'week_index', 'temperature', 'demand']

query = ("SELECT temp_data.timestamp,hour,temp_index,week_index,temperature,demand_data.demand" 
         " FROM temp_data LEFT JOIN demand_data ON temp_data.timestamp = demand_data.timestamp"
         " ORDER BY timestamp")


data = postgresql_to_dataframe(conn, query, column_names)
data.set_index('timestamp',inplace=True)

deal_with_holidays()

# data.fillna('ffill',inplace=True)   

cols = st.beta_columns(2)

cols[1].write("""
          # Predicting Toronto's Electricity Demand
         
          ## Using the powers of only Simple Linear Regression!
          """)
         

cols[0].image('toronto.jpg')

st.write("""
          #### Hi There! Welcome!
          """)

st.write("""
          Do you want to predict Toronto's electricity consumption and are scared
          of big words like SARIMA(X) and neural networks? Fear not! I have only used linear regression here.
          """)
         
st.write("""
          How many days of data would you like to see?
          """)
          
option = st.slider("",1,10,1) 

pred_start = (datetime.today() - timedelta(days=option)).strftime('%Y-%m-%d')
last_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

with st.spinner('Loading your plot...'):
 
    pred_day,train_days = my_data(data,pred_start,last_date)
    
    
    groupby_list = list(data.groupby(['temp_index','hour','week_index']).groups.keys())
    ridge_coef_3 = train_days.groupby(['temp_index','hour','week_index']).apply(Ridge_model) 
    
    demand_hat_3 = []
    for i in range(pred_day.shape[0]):
        demand_hat_3.append(predict(pred_day.iloc[i],['temp_index','hour','week_index'],ridge_coef_3))
    
    prediction_interval_3 = []
    for i in range(pred_day.shape[0]):
        prediction_interval_3.append(pred_interval(demand_hat_3[i],pred_day,demand_hat_3))
    pred_int_3 = pd.DataFrame(prediction_interval_3,columns=['Lower','Actual','Upper'])
    
    
    timestamp = pred_int_3.set_index(pred_day.index).reset_index()['timestamp']
    actual_pred = pred_int_3.set_index(pred_day.index).reset_index()['Actual']
    lower_pred = pred_int_3.set_index(pred_day.index).reset_index()['Lower']
    upper_pred = pred_int_3.set_index(pred_day.index).reset_index()['Upper']
    actual_demand = pred_day.reset_index()['demand']
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x = pd.Series.append(timestamp,timestamp[::-1]), y = pd.Series.append(upper_pred,lower_pred[::-1]), fill='toself',
                              fillcolor='rgba(0,176,246,0.1)',line = dict(color='rgba(0,176,246,0.2)', width=2),name='Forecase Interval'))
    fig.add_trace(go.Scatter(x = timestamp, y = actual_demand, name='True Value',line = dict(color='black', width=2)))
    fig.add_trace(go.Scatter(x = timestamp, y = actual_pred,mode='lines',name='Forecast',line = dict(color='goldenrod', width=2,dash='dash')))
    fig.update_layout(template="simple_white",title=f"Electricity Demand predictions from {pred_start} to {last_date}")
    fig.update_yaxes(title_text="Electricity Demand (MWh)")
    fig.update_xaxes(title_text="Time")
    
    
    st.plotly_chart(fig)




