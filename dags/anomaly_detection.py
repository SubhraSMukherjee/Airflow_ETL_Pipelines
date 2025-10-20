from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime
import sqlite3
import os
import pandas as pd
import numpy as np
from itertools import product
import pickle
import joblib
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.stattools import adfuller

DB_PATH = "/opt/airflow/shared_dbs/my_data.db"

def read_advertising_data(**kwargs):
    if not os.path.exists(DB_PATH):
         raise FileNotFoundError(f"Database not found at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    sales_data = pd.read_sql_query("SELECT * FROM Advertising_Data;", conn)
    return sales_data

def check_stationarity(**kwargs):
    ti = kwargs['ti']  
    data = ti.xcom_pull(task_ids='read_advertising_data')

    result = adfuller(data.iloc[:-7].Product_Sold.values)
    s = ''
    s+= 'ADF Statistic: %f\n' % result[0]
    s+= 'p-value: %f\n' % result[1]
    s+= 'Critical Values:\n'
    for key, value in result[4].items():
         s+= '\t%s: %.3f\n' % (key, value)
    if (result[1] <= 0.05) & (result[4]['5%'] > result[0]):
         s+="\u001b[32mStationary\u001b[0m"
    else:
         s+="\x1b[31mNon-stationary\x1b[0m"
         subject = "Anomaly Detection for %f" % (datetime.today().strftime('%Y-%m-%d'))
         body ="""
         <h3>Hello!</h3>
         <p>Your Model specifications may be incorrect</p>
         <p>Execution date: {0}</p>
         <br></br>
         {1}
         """.format(datetime.today().strftime('%Y-%m-%d'),s)
        
         send_email(to="subhra.p17109@iimtrichy.ac.in", subject=subject, html_content=body)


def calculate_best_model(**kwargs):
    ti = kwargs['ti']  
    data = ti.xcom_pull(task_ids='read_advertising_data')
    sales_data_train = data.iloc[:-7]    
    exogenous = [col for col in list(data) if col!='Product_Sold']
    p = q = range(0, 7)
    d =  (0,)                                                           #Assuming Stationarity to reduce Compute Load
    pdq = list(product(p, d, q))    

    model_specification = []
    def Arimax_Grid_Search(pdq, maxiter=10000, freq='D'):
       for element in pdq:
           try:
              model = SARIMAX(sales_data_train.Product_Sold, exog = sales_data_train[exogenous], order = element, trend='n', enforce_stationarity=False, enforce_invertibility=False, mle_regression = True)
              results = model.fit(maxiter = maxiter)
              model_specification.append([element, results.bic])
           except:
              continue

       Model_df = pd.DataFrame(model_specification, columns=['pdq', 'bic']).sort_values(by=['bic'],ascending=True)[0:10]
       return Model_df.reset_index(drop=True)

    best_model = Arimax_Grid_Search(pdq)
    model_final = SARIMAX(sales_data_train.Product_Sold, exog = sales_data_train[exogenous], order = best_model.loc[0, 'pdq'], trend='n', enforce_stationarity=False, enforce_invertibility=False, mle_regression = True)
    results = model_final.fit()
    results_path = r'/tmp/results.pkl'
    joblib.dump(results, results_path)
    return results_path

def send_mail_notification(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='read_advertising_data')
    sales_data_test = data.iloc[(data.shape[0]-7):]
    exogenous = [col for col in list(data) if col!='Product_Sold']
    results_path = kwargs['results_path']
    if not results_path:
        raise ValueError("No results_path found")
    results = joblib.load(results_path)
    forecasts = results.get_forecast(steps=7, exog = sales_data_test[exogenous]).conf_int(alpha=0.01)
    forecasts = forecasts.rename(lambda col: col.replace(" ", "_").lower(), axis='columns')
    forecasts['Actual'] = sales_data_test.iloc[:(datetime.today().isoweekday()),:].Product_Sold
    forecasts['Anomaly?'] = forecasts.apply(lambda row: 'N/A' if np.isnan(row.Actual) else ('No' if row.lower_product_sold <= row.Actual <= row.upper_product_sold  else 'Yes'), axis=1)

    css = """
    <style>
    table {width: 100%; border-collapse: collapse; font-family: "Segoe UI", Roboto, sans-serif; font-size: 16px; color: #333; background-color: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);}
    thead {background-color: #f4f6f8; text-transform: uppercase; letter-spacing: 0.03em;}
    th {padding: 12px 16px; text-align: left; font-weight: 600; color: #444; border-bottom: 2px solid #e0e0e0;}
    td {padding: 12px 16px; border-bottom: 1px solid #eee;}
    tbody tr:hover {background-color: #fafafa; transition: background-color 0.2s ease;}
    tbody tr:nth-child(even) {background-color: #fcfcfc;}
    .table-container {overflow-x: auto; border-radius: 12px;}
    tbody tr:active {background-color: #e6f2ff;}
    </style>
    """
    html = forecasts.to_html(index=False)
    with open("forecasts.html", "w", encoding="utf-8") as f:
        f.write(f"<!doctype html><html><head><meta charset='utf-8'><title>Styled</title>{css}</head>"
            f"<body><h1>Upper and Lower Ranges for Products Sold</h1>{html}</body></html>")
    
    html_file_path = "forecasts.html"
    
    with open(html_file_path, "r") as f:
        content = f.read()
    #print(html_content)
    
    subject = "Anomaly Detection for {0}".format(datetime.today().strftime('%Y-%m-%d'))
    send_email(to="subhrosmukherjee@gmail.com", subject=subject, html_content=content)

def store_results(**kwargs):
    ti = kwargs['ti']    
    results_path = kwargs['results_path']
    if not results_path:
        raise ValueError("No results_path found")
    results = joblib.load(results_path)
    serialized_result = pickle.dumps(results)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create Results Table with a BLOB field
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS result_objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT,
    results BLOB
    )
    """)
    
    cursor.execute("INSERT INTO result_objects (run_date, results) VALUES (?, ?)", (datetime.today().strftime('%Y-%m-%d'), serialized_result))
    conn.commit()
    conn.close()


with DAG(
    dag_id="Anomaly_Detection_Daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  
    catchup=False,
    tags=["sqlite", "Anomaly_Detection","Email_Notification", "MLOPs_POC"],
) as dag:

    read_data_task = PythonOperator(
        task_id="read_advertising_data",
        python_callable=read_advertising_data,
    )

    check_stationarity_task = PythonOperator(
        task_id="check_stationarity",
        python_callable=check_stationarity,
    )

    calculate_best_model_task = PythonOperator(
        task_id="calculate_forecast_range",
        python_callable=calculate_best_model,
    )
    
    send_mail_task = PythonOperator(
        task_id="send_email_notification",
        python_callable=send_mail_notification,
        op_kwargs={'results_path': r'/tmp/results.pkl'}
    )
    
    store_results_task = PythonOperator(
        task_id="store_results_for_model_diagnostics",
        python_callable=store_results,
        op_kwargs={'results_path': r'/tmp/results.pkl'}
    )

    read_data_task >> check_stationarity_task >> calculate_best_model_task >> [send_mail_task, store_results_task] 
    
