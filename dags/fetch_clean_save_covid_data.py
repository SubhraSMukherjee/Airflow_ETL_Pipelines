from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3
import os
import json,requests,re
import pandas as pd
import numpy as np

DB_PATH = "/opt/airflow/shared_dbs/my_data.db"

def access_covid_data():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    response = requests.get("https://api.covidtracking.com/v1/us/daily.json")
    if response.status_code == 200:
        print(json.dumps(response.json()[0:3], sort_keys=True, indent=4))
    else:
        print('Unsuccessful Fetch {0}'.format(response.status_code))

    # @title
    # Manually pulled, currently no way to automatically do it right now
    metadata = {
        "date": 'Date on which data was collected by The COVID Tracking Project',
        "dateChecked": "Deprecated. This is an old label for lastUpdateEt",
        "death": "Total fatalities with confirmed OR probable COVID-19 case diagnosis",
        "deathIncrease": "Daily increase in death, calculated from the previous day’s value",
        "hash": "A hash for this record",
        "hospitalized": "Deprecated. Old label for hospitalizedCumulative",
        "hospitalizedCumulative": "Total number of individuals who have ever been hospitalized with COVID-19",
        "hospitalizedCurrently": "Individuals who are currently hospitalized with COVID-19",
        "hospitalizedIncrease": "New total hospitalizations. Daily increase in hospitalizedCumulative, calculated from the previous day’s value",
        "inIcuCumulative": "Total number of individuals who have ever been hospitalized in the Intensive Care Unit with COVID-19",
        "inIcuCurrently": "Individuals who are currently hospitalized in the Intensive Care Unit with COVID-19",
        "lastModified": "Deprecated. Old label for lastUpdateET",
        "negative": "Total number of unique people with a completed PCR test that returns negative",
        "negativeIncrease": "Increase in negative computed by subtracting the value of negative for the previous day from the value for negative from the current day",
        "onVentilatorCumulative": "Total number of individuals who have ever been hospitalized under advanced ventilation with COVID-19",
        "onVentilatorCurrently": "Individuals who are currently hospitalized under advanced ventilation with COVID-19",
        "pending": "Total number of viral tests that have not been completed",
        "posNeg": "Deprecated. Computed by adding positive and negative values",
        "positive": "Total number of confirmed plus probable cases of COVID-19",
        "positiveIncrease": "The daily increase in API field positive, which measures Cases (confirmed plus probable) calculated based on the previous day’s value",
        "recovered": "Total number of people that are identified as recovered from COVID-19",
        "states": "The number of states and territories included in the US dataset for this day",
        "total": "Deprecated. Computed by adding positive, negative, and pending values",
        "totalTestResults": "At the national level, this metric is a summary statistic which, because of the variation in test reporting methods, is at best an estimate of US viral (PCR) testing",
        "totalTestResultsIncrease": "Daily increase in totalTestResults, calculated from the previous day’s value"
    }

    df = pd.DataFrame(response.json())
    final_columns = [column for column in metadata if 'Deprecated'.lower() not in metadata[column].lower()]       #Drop Deprecated Columns                                                                                                            #Drop Deprecated Columns
    df = df[final_columns].fillna(0)                                                                              #NaN to 0
    df = df.rename(lambda col: re.sub(r'([a-z])([A-Z])', r'\1_\2', col).lower(), axis='columns')                  #Snake Case Column names
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')                                                      #Date to Datetime format
    df['hash'] = df['hash'].astype("string")                                                                      #Object to String
    df[(df.select_dtypes(np.number)).columns]= df.select_dtypes(np.number).round().astype('Int64')                #All Floats to Ints    

    # Covid Table: create the final cleaned table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS covid_data (
            date TEXT PRIMARY KEY,
            death INTEGER NOT NULL,
            death_increase INTEGER,
            hash TEXT,
            hospitalized_cumulative INTEGER,
            hospitalized_currently INTEGER,
            hospitalized_increase INTEGER,
            in_icu_cumulative INTEGER,
            in_icu_currently INTEGER,
            negative INTEGER,
            negative_increase INTEGER,
            on_ventilator_cumulative INTEGER,
            on_ventilator_currently INTEGER,
            pending INTEGER,
            positive INTEGER,
            positive_increase INTEGER,
            recovered INTEGER,
            total_test_results INTEGER,
            states INTEGER,
            total_test_results_increase INTEGER            
        )
    """)

    records = df.to_records(index=False)
    cursor.executemany(
        'INSERT INTO covid_data (date, death, death_increase, hash, hospitalized_cumulative, hospitalized_currently, hospitalized_increase, in_icu_cumulative, in_icu_currently, negative, negative_increase, on_ventilator_cumulative, on_ventilator_currently, pending, positive, positive_increase, recovered, total_test_results, states, total_test_results_increase) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        records
    )
    conn.commit()

    conn.close()
    print("✅ Covid Data stored in local volume mounted SQLite DB")

with DAG(
    dag_id="save_cleaned_covid_data",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  
    catchup=False,
    tags=["sqlite", "covid_tracking_project"],
) as dag:

    read_customers_task = PythonOperator(
        task_id="read_clean_save_covid_data",
        python_callable=access_covid_data,
    )
