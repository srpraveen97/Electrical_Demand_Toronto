# Analysis of electricity Demand data in Toronto

### Exploratory data analysis

I took up the publicly available data, by IESO, of electricity demand in Toronto and tried to provide a detailed analysis of various factors affecting it.

When a lockdown was imposed, earlier in 2020, I am sure a lot of us lost the distinction between weekends and weekdays. It was interesting to see this behavior translate into our electricity consumption.

It was also compelling to see the flattening of the peak throughout the eight months of daylight savings time. This is particularly significant from the electricity suppliers' point of view.

I have provided a detailed explanation of this in the [article](https://medium.com/analytics-vidhya/from-lockdown-to-daylight-savings-hidden-insights-from-electricity-demand-data-in-toronto-eff585aada66).

### Forecasting electricity demand predictions

I used multiple Ridge regression models to forecast electricity demand in Toronto. It was impressive to see how combining several such linear models result in a moderate accuracy with high interpretability.

While working on this project, I learned that this method in the literature is called a Linear Model Tree, a combination of decision trees and linear regression. It is similar to decision trees regression in the sense that we split the data into several groups, but instead of choosing the mean value as a predictor in each category, we run a linear regression. A detailed explanation of this is provided in the [article](https://srpraveen1997.medium.com/keep-it-simple-keep-it-linear-a-linear-regression-model-for-time-series-5dbc83d89fc3).

### SQL Database

I also set up a Postgres SQL database to scrape and store the values dynamically from multiple online sources for electricity demand ([IESO](https://www.ieso.ca/power-data)), temperature ([weatherstats](https://toronto.weatherstats.ca/charts/temperature-hourly.html)), and temperature forecast ([wunderground](https://www.wunderground.com/hourly/ca/toronto-pearson-international/CYYZ/date/)). I used Psycopg2 to connect Python and PostgreSQL. This is used to query data back and forth from SQL to Python.

### Streamlit Web-app

I used streamlit to build a web application and to deploy the linear model tree online.
