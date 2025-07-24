import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
import psycopg2 as db
from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np

'''
=================================================
Milestone 3

Nama  : Fauzan Rahmat Farghani
Batch : HCK-028

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai attrition terhadap karyawan yang bekerja di perusahaan.
=================================================
'''

default_args = {
    'owner': 'Fauzan Farghani',
    'start_date': dt.datetime(2024, 11, 2),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=10),
}

def data_postgres():
    '''
     Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.
     '''
    
    conn_string="port=5432 dbname='m3' host='postgres' user='airflow' password='airflow'"

    conn=db.connect(conn_string)

    df=pd.read_sql("SELECT * FROM table_m3",conn)
    df.to_csv('/opt/airflow/dags/P2M3_fauzanfarghani_data_raw.csv', index=False)

# cleaning data
def clean_data():
    '''
     Fungsi ini ditujukan untuk membersihkan data yang telah diperoleh dari postgre.
     '''
    #read csv
    df = pd.read_csv('/opt/airflow/dags/P2M3_fauzanfarghani_data_raw.csv')

    #Drop irrelevant columns
    #Columns like EmployeeCount, Over18, and StandardHours seem constant
    columns_to_drop = ['EmployeeCount', 'Over18', 'StandardHours']
    df = df.drop(columns=columns_to_drop, errors='ignore')

    #Remove rows with missing values
    df = df.dropna()
    print("\nMissing Values After Removal:")
    print(df.isnull().sum())

    #Decode specified columns
    decode_mappings = {
        'Education': {
            1: 'Below College',
            2: 'College',
            3: 'Bachelor',
            4: 'Master',
            5: 'Doctor'
        },
        'EnvironmentSatisfaction': {
            1: 'Low',
            2: 'Medium',
            3: 'High',
            4: 'Very High'
        },
        'JobInvolvement': {
            1: 'Low',
            2: 'Medium',
            3: 'High',
            4: 'Very High'
        },
        'JobSatisfaction': {
            1: 'Low',
            2: 'Medium',
            3: 'High',
            4: 'Very High'
        },
        'PerformanceRating': {
            1: 'Low',
            2: 'Good',
            3: 'Excellent',
            4: 'Outstanding'
        },
        'RelationshipSatisfaction': {
            1: 'Low',
            2: 'Medium',
            3: 'High',
            4: 'Very High'
        },
        'WorkLifeBalance': {
            1: 'Bad',
            2: 'Good',
            3: 'Better',
            4: 'Best'
        }
    }

    # Apply decoding to the specified columns
    for column, mapping in decode_mappings.items():
        df[column] = df[column].map(mapping)

    #Correct data types
    # Ensure categorical columns are of type 'category'
    categorical_columns = ['Attrition', 'BusinessTravel', 'Department', 'EducationField', 
                        'Gender', 'JobRole', 'MaritalStatus', 'OverTime', 
                        'Education', 'EnvironmentSatisfaction', 'JobInvolvement', 
                        'JobSatisfaction', 'PerformanceRating', 'RelationshipSatisfaction', 
                        'WorkLifeBalance']
    for col in categorical_columns:
        df[col] = df[col].astype('category')

    # Ensure numerical columns are of appropriate type
    numerical_columns = ['Age', 'DailyRate', 'DistanceFromHome', 'HourlyRate', 
                        'MonthlyIncome', 'MonthlyRate', 'NumCompaniesWorked', 
                        'PercentSalaryHike', 'StockOptionLevel', 'TotalWorkingYears', 
                        'TrainingTimesLastYear', 'YearsAtCompany', 'YearsInCurrentRole', 
                        'YearsSinceLastPromotion', 'YearsWithCurrManager']
    for col in numerical_columns:
        df[col] = df[col].astype('int64')

    # Standardize Column Names
    df.columns = df.columns.str.lower()

    # Remove duplicates
    df = df.drop_duplicates()

    # Save the cleaned dataset
    df.to_csv('/opt/airflow/dags/P2M3_fauzanfarghani_data_clean.csv', index=False)

def loading_data():
    '''
     Fungsi ini ditujukan untuk memasukkan data yang sudah dibersihkan ke ElasticSearch.
     '''
    es = Elasticsearch('http://elasticsearch:9200')

    df = pd.read_csv('/opt/airflow/dags/P2M3_fauzanfarghani_data_clean.csv')

    for i, row in df.iterrows():
        res = es.index(index='employeeattrition', id=i+1, body=row.to_json())
        print(res)
        
with DAG('m3',
         default_args=default_args,
         schedule_interval=timedelta(minutes=10),
         catchup = False      # '0 * * * *',
         ) as dag:
    
    start = EmptyOperator(task_id='start')
    

    datapostgres = PythonOperator(task_id='postgredata_',
                                 python_callable=data_postgres)
    
    cleandata = PythonOperator(task_id='cleandata_',
                                 python_callable=clean_data)
    
    loadingdata = PythonOperator(task_id='loadingdata_',
                                 python_callable=loading_data)
    
    end = EmptyOperator(task_id='end')


start >> datapostgres >> cleandata >> loadingdata >> end