import json
from datetime import datetime
from textwrap import dedent
import airflow
import pandas as pd
import numpy as np
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators import PythonOperator
# [END import_module]
# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
# [END default_args]


# [START instantiate_dag]
with DAG(
    'DE3',
    default_args=default_args,
    description='DE3',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]


    # [START extract_function]
    def extract():
        olympic_original= pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/athlete_events.csv')  #Importing Dataset 
        noc_regions= pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/noc_regions.csv')  #Importing Dataset
        olympic_original.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_olympic.csv')
        noc_regions.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_noc.csv')


    
    def extract_2():
        data1 = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_olympic.csv')
        data2 = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_noc.csv')
        data1.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_data1.csv')
        data2.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_data2.csv')
    


    # [END extract_function]

    # [START transform_function]
    # def transform(**kwargs):
    #     ti = kwargs['ti']
    #     extract_data_string = ti.xcom_pull(task_ids='extract', key='order_data')
    #     order_data = json.loads(extract_data_string)

    #     total_order_value = 0
    #     for value in order_data.values():
    #         total_order_value += value

    #     total_value = {"total_order_value": total_order_value}
    #     total_value_json_string = json.dumps(total_value)
    #     ti.xcom_push('total_order_value', total_value_json_string)

    def handling_outliers_Weight():
        olympic_original = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_olympic.csv')
        #Handling outliers by replacing values below Q1-1QR*1.5 and values above Q3+IQR*1.5 with the median
        Q1 = olympic_original.Weight.quantile(0.25)
        Q3 = olympic_original.Weight.quantile(0.75)
        IQR = Q3 - Q1
        for col in olympic_original:
            olympic_original.loc[olympic_original['Weight'].between(25,31.5), 'Weight'] = 175
            olympic_original.loc[olympic_original['Weight'].between(107.5,214), 'Weight'] = 175

        olympic_original.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_olympic_WeightOutliers.csv')
        
        
    def handling_outliers_Height():
            olympic_original = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_olympic_WeightOutliers.csv')
            #Handling outliers by replacing values below Q1-1QR*1.5 and values above Q3+IQR*1.5 with the median
            Q1 = olympic_original.Height.quantile(0.25)
            Q3 = olympic_original.Height.quantile(0.75)
            IQR = Q3 - Q1
            for col in olympic_original:
                olympic_original.loc[olympic_original['Height'].between(127,145.5), 'Height'] = 175
                olympic_original.loc[olympic_original['Height'].between(205.5,226), 'Height'] = 175

            olympic_original.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_olympic_Weight_Height_Outliers.csv')

    def data_cleaning_1():
            olympic_original = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_olympic_Weight_Height_Outliers.csv')
            #Drop rows where city column is empty
            cleaned_cities = olympic_original.dropna(axis='index', subset=['City']) 

            #Remove duplicates based on games
            cleaned_cities_Duplicates = cleaned_cities.drop_duplicates(subset=['Games'])

            #Sort values
            cleaned_cities_sorted = cleaned_cities_Duplicates.sort_values(by=['Games'], ascending=True)

            #Count Sorted Values
            cleaned_cities_sorted_count = cleaned_cities_sorted['City'].value_counts()

            cleaned_cities_sorted.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_olympic_Outliers_Duplicates_Sorted.csv')



    def merge_data():
        data1 = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_data1.csv')
        data2 = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_data2.csv')
        # using merge function by setting how='inner'
        output = pd.merge(data1, data2, 
                        on='NOC', 
                        how='inner')
        output.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/merged_data.csv')
        #Save output to csv in path



    def data_cleaning_2():
        #Data Cleaning
        #Dropping the column 'notes' as it will not be used and it contains a lot of missing values
        #Removing the columns that contain irrelevant information for the question we want to answer
        output = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/merged_data.csv')
        output2 = output.drop(['notes', 'Sport', 'Height', 'Weight', 'Name', 'Age', 'Year', 'Games', 'Season', 'Event', 'Sex'], axis=1)
        output.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/merged_data_dropped.csv')


    def feature_eng_1():
        #Feature Engineering (1)
        #Quesion 7: BMI of olympians who won olympic medals

        output = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_data1.csv')
        #Remove duplicate olympians based on ID
        #Remove Columns irrelevant to BMW calculation
        output4 = output.drop(['Year', 'Games', 'Season', 'Event', 'Sport', 'NOC', 'Team', 'City'], axis=1)
        output4 = output4.drop_duplicates(subset ="ID")

        #Calculate BMI
        output4['BMI'] = (output4['Weight'])/(output4['Height']/50)

        #Obesity Indicator based on BMI
        output4['Obesity Indicator'] = (output4['BMI'] >= 25)*1


        #Remove any olympians who have not won medals
        output5 = output4.dropna(axis='index', subset=['Medal'])

        output4.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/data_BMI.csv')



    def feature_eng_2():
        #Feature Engineering (2)
        #Question 8: Number of medals won by each age group
        output = pd.read_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/extracted_data1.csv')
        #Encoding Age groups
        output6 = output.dropna(axis='index', subset=['Age'])
        output6 = output6.drop_duplicates(subset ="ID")
        output6 = output6.dropna(axis='index', subset=['Medal'])


        age_group = pd.cut(output.Age, [10,20,30,40,100],labels=['young','young-adult', 'middle-aged','old'])
        output6["age_group"] = age_group

        

        output6.to_csv('/Users/eslamehab/Desktop/airflow-tutorial/dags/data_encoded_medals.csv')





    # [END transform_function]

    # [START load_function]
    # def load(**kwargs):
    #     ti = kwargs['ti']
    #     total_value_string = ti.xcom_pull(task_ids='transform', key='total_order_value')
    #     total_order_value = json.loads(total_value_string)

    #     print(total_order_value)

    # [END load_function]

    # [START main_flow]
    # extract_task = PythonOperator(
    #     task_id='extract',
    #     python_callable=extract,
    # )
    # extract_task.doc_md = dedent(
    #     """\
    # #### Extract task
    # A simple Extract task to get data ready for the rest of the data pipeline.
    # In this case, getting data is simulated by reading from a hardcoded JSON string.
    # This data is then put into xcom, so that it can be processed by the next task.
    # """
    # )

    # transform_task = PythonOperator(
    #     task_id='transform',
    #     python_callable=transform,
    # )
    # transform_task.doc_md = dedent(
    #     """\
    # #### Transform task
    # A simple Transform task which takes in the collection of order data from xcom
    # and computes the total order value.
    # This computed value is then put into xcom, so that it can be processed by the next task.
    # """
    # )

    # load_task = PythonOperator( #Creates Nodes
    #     task_id='load', #ay id
    #     python_callable=load, #function
    # )

    get_data = PythonOperator( #Creates Nodes
        task_id='getdata', #ay id
        python_callable=extract, #function
    )

    handle_Out_Weight = PythonOperator( #Creates Nodes
        task_id='OutWeight', #ay id
        python_callable=handling_outliers_Weight, #function
    )

    handle_Out_Height = PythonOperator( #Creates Nodes
        task_id='OutHeight', #ay id
        python_callable=handling_outliers_Height, #function
    )

    data_clean_1 = PythonOperator( #Creates Nodes
        task_id='data_clean_1', #ay id
        python_callable=data_cleaning_1, #function
    )

    get_data_2 = PythonOperator( #Creates Nodes
        task_id='getdata2', #ay id
        python_callable=extract_2, #function
    )

    merge = PythonOperator( #Creates Nodes
        task_id='merge', #ay id
        python_callable=merge_data, #function
    )

    data_clean_2 = PythonOperator( #Creates Nodes
        task_id='data_clean_2', #ay id
        python_callable=data_cleaning_2, #function
    )

    feature_eng1 = PythonOperator( #Creates Nodes
        task_id='featureeng1', #ay id
        python_callable=feature_eng_1, #function
    )

    feature_eng2 = PythonOperator( #Creates Nodes
        task_id='featureeng2', #ay id
        python_callable=feature_eng_2, #function
    )

    

    # load_task.doc_md = dedent(
    #     """\
    # #### Load task
    # A simple Load task which takes in the result of the Transform task, by reading it
    # from xcom and instead of saving it to end user review, just prints it out.
    # """
    # )

    get_data >> handle_Out_Weight >> handle_Out_Height >> data_clean_1 >> get_data_2 >> merge >> data_clean_2 >> feature_eng1 >> feature_eng2#Dependencies