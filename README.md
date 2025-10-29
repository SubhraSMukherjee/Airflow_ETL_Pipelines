
![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/Airflow.PNG) 
![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/Docker.PNG)
## Docker based Airflow Instance 

This repo implements an [Airflow](https://airflow.apache.org/) instance in Docker to prototype running workflows. The project uses the file based [SQLite DB](https://sqlite.org/) (as opposed to a Postgre DB) since the project is more for prototyping purposes and not for full scale backend systems  

![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/image1.PNG)
___
*Here are details on some of the Workflows:*
### >Anomaly Detection
This Workflow reads last **N** days of Data for a particular metric and creates of an expected forecast range (based on a configurable confidence interval) of the metric for the next 7 days. As the actual data for the metric becomes available, it is compared against the precomputed range everyday and an **Anomaly** is registered if the metric is not in the range. Stakeholders are **notified** everyday. 


![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/forecast.PNG)


The DAG (i.e. workflow) uses an [ARIMAX](https://en.wikipedia.org/wiki/Autoregressive_integrated_moving_average) model to create the Forecast ranges after the Data is read from the SQLite DB. It is configured to check the for the model assumptions before searches for the best model amongst a finite countable set of models. Post forecasting the fitted model details are saved back in the DB for future diagnostics  


![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/anomaly.PNG)

___

### >Save Covid Data
This Workflow pings the [Covid Tracking Project](https://covidtracking.com/data/api) APIs to fetch Daily US Covid Data which it cleans and then saves to the Sqlite DB

(*Raw JSON Data received from API*)
![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/Covid_raw.PNG)



(*Cleaned Data stored in SQLite DB*)
![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/final_cleaned.PNG)


___
### >Initialize SQLite DB & Read SQLite DB
These are used to setup and read the SQLite DB via airflow. (The init-sqlite powershell script also initilizes the DB but can only be run via the Powershell on Windows)
