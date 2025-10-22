
![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/Airflow.PNG) 
![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/Docker.PNG)
## Docker based Airflow Instance 

This repo implements an [Airflow](https://airflow.apache.org/) instance in Docker to prototype running workflows. The project uses the file based [SQLite DB](https://sqlite.org/) (as opposed to a [Postgre DB]) since the project is more for prototyping purposes and not for full scale backend systems  

![Page_1](https://github.com/SubhraSMukherjee/Airflow_ETL_Pipelines/blob/main/screenshots/image1.PNG)
___
*Here are details on some of the Workflows:*
### Anomaly Detection
This Workflow reads last n days of Data for a particular metric and creates and notifies Stakeholders of an expected forecast range of the metric for the next 7 days 
