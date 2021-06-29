import io
import pandas as pd
import boto3
import boto.s3.connection
from botocore.exceptions import NoCredentialsError
import pymongo
from pymongo import MongoClient
import csv
import os
import json
import mysql.connector
from mysql.connector import Error
import numpy as np
import ibm_db
from datetime import datetime

from dagster import pipeline, solid


@solid
def read_from_s3(context, list_of_files):
    ACCESS_KEY = 'AKIAQGMFMSSI4DZTTKXB'  # Access Key of User
    SECRET_KEY = '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'  # Secret Key of User
    REGION = 'ap-south-1'  # Region in which bucket is stored
    BUCKET_NAME = 'slark'  # Bucket name
    count=1
    list_new=[]

    s3 = boto3.client(
        's3',  # calling client with Credentials to get connection
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    for file_name in list_of_files:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)  # Calling object with arguments
        context.log.info(f"** Reading {file_name} file ")
        context.log.info(f"** Creating dataframe for  {file_name} file ")
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')  # Creating DataFrame using Pandas
        context.log.info(f"** df  =  {df}")
        list_new.append(df)
    dict_new={
        "df1": list_new[0],
        "df2": list_new[1],
        "df3": list_new[2]
    }
    #context.log.info(f"** dict_new_df1 = {dict_new['df1']} ")
    #context.log.info(f"** dict_new_df2 = {dict_new['df2']} ")
    #context.log.info(f"** dict_new_df3 = {dict_new['df3']} ")
    #context.log.info(f"** dict_new = {dict_new} ")
    return dict_new

@solid
def connect_to_mongoDB(context,dict_new):
    context.log.info(f"** Inside connect to mongoDB function ")
    #context.log.info(f"** dict_new = {dict_new} ")
    cluster = MongoClient('mongodb+srv://sachin:vedu@cluster0.ybz2r.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    db = cluster['Covid_Collection']
    dict_new_1 = {
        "db_connection": db,
        "dict_new": dict_new
    }
    return dict_new_1


@solid
def fetch_data_from_mongo_DB(context,dict_new_1):
    db=dict_new_1['db_connection']
    dict_new=dict_new_1['dict_new']
    #context.log.info(f"** dict_new (inside fetch data from mongoDB) = {dict_new} ")

    list_collection = ['Covid_dataset1','Covid_dataset2','Covid_dataset3']  # List of all 3 collection.
    list_df = [dict_new['df1'], dict_new['df2'],dict_new['df3']]  # List of all 3 dataframes.

    for i in range(len(list_collection)):
        db.drop_collection(list_collection[i])
        db.create_collection(list_collection[i])
        collection = db[list_collection[i]]
        list_df[i].reset_index(drop=True, inplace=True)
        # Inserting records
        context.log.info(f"** shape inserted = {list_df[i].shape} ")
        records = json.loads(list_df[i].T.to_json()).values()
        collection.insert(records)

    list_of_all_data = []
    for i in range(len(list_collection)):
        collection = db[list_collection[i]]

        lst = []

        for i in collection.find():
            lst.append(i)
        list_of_all_data.append(lst)
    context.log.info(f"** length of list_of_all_data from MongoDB :  {len(list_of_all_data)} ")
    #context.log.info(f"** list_of_all_data from MongoDB :  {list_of_all_data} ")
    return list_of_all_data

@solid
def  create_dataframe_with_pandas(context,data_list):
    data_list_new=[]
    for i in range(len(data_list)):
        df = pd.DataFrame(data_list[i])
        data_list_new.append(df)
    dict_final={
        'df1':data_list_new[0],
        'df2':data_list_new[1],
        'df3':data_list_new[2]
    }
    return dict_final

@solid
def drop_id_col(context,dict_final):
    df1=dict_final['df1'].drop(labels=['_id'], axis=1)
    df2=dict_final['df2'].drop(labels=['_id'], axis=1)
    df3=dict_final['df3'].drop(labels=['_id'], axis=1)
    dict_final=[df1,df2,df3]
    return dict_final

@solid
def check_isna(context,dict_final):
    for i in dict_final:
        context.log.info(f"** Printing NA value count :  {i.isna().sum()} ")
    return dict_final

@solid
def replace_nan_values(context,dict_final):
    for i in dict_final:
        i.replace(to_replace =['', np.nan],  value ="0", inplace=True)
    return dict_final

@solid
def establish_mysql_connection(context,processed_data):
    db_connect = mysql.connector.connect(host='pumba.cr7w9bxf9xgj.eu-west-1.rds.amazonaws.com', user='admin',
                                         password='oYeUKU9ISHQ8')
    list_new=[db_connect,processed_data]
    return list_new

@solid
def create_DB(context,list_new):

    db_connect=list_new[0]

    try:
        if db_connect.is_connected():
            cursor = db_connect.cursor()
            cursor.execute("DROP DATABASE IF EXISTS DAP_Project_temp;")
            cursor.execute("CREATE DATABASE DAP_Project_temp")
            context.log.info(f"** DAP_Project_temp Database is created successfully.")
    except Error as e:
        print("Error while connecting to MYSQL", e)

    return list_new

@solid
def aws_sql_create_table(context,list_new):

    db_connect = list_new[0]

    try:
        if db_connect.is_connected():
            cursor = db_connect.cursor()
            cursor.execute("use DAP_Project_temp")  # SQL to connect to Database - DAP_Project_temp
            cursor.execute('DROP TABLE IF EXISTS CovidData_1;')  # It will remove table if already present
            context.log.info(f"** Creating 3 tables...")

            # Creating table for 1st dataset:
            cursor.execute('''                                      
            CREATE TABLE CovidData_1(
                country VARCHAR(20),
                iso_code VARCHAR(20), 
                date TEXT,
                total_vaccinations INT,
                people_vaccinated INT,
                people_fully_vaccinated INT,
                daily_vaccinations_raw INT,
                daily_vaccinations INT,
                total_vaccinations_per_hundred INT,
                people_vaccinated_per_hundred INT,
                people_fully_vaccinated_per_hundred INT,
                daily_vaccinations_per_million INT,
                vaccines VARCHAR(20),
                source_name VARCHAR(20),
                source_website TEXT)''')

            # Creating table for 2nd dataset:
            cursor.execute('DROP TABLE IF EXISTS CovidData_2;')
            cursor.execute('''                                      
            CREATE TABLE CovidData_2(
                dateofrecord TEXT, 
                country_code TEXT,
                country_name TEXT, 
                dailydeceased int, 
                totaldeceased int,  
                dailyconfirmed int,
                dailyrecovered int, 
                totalconfirmed int, 
                totalrecovered int)''')

            # Creating table for 3rd dataset:
            cursor.execute('DROP TABLE IF EXISTS CovidData_3;')
            cursor.execute('''
            CREATE TABLE CovidData_3(
                iso_code TEXT,
                continent TEXT,
                location TEXT,
                Date_noted TEXT,
                total_cases bigint,
                new_cases bigint,
                new_cases_smoothed float,
                total_deaths INT,
                new_deaths INT,
                new_deaths_smoothed float,
                total_cases_per_million float,
                new_cases_per_million float,
                new_cases_smoothed_per_million float,
                total_deaths_per_million float,
                new_deaths_per_million float,
                new_deaths_smoothed_per_million float,
                reproduction_rate float,
                icu_patients INT,
                icu_patients_per_million float,
                hosp_patients INT,
                hosp_patients_per_million float,
                weekly_icu_admissions float,
                weekly_icu_admissions_per_million float,
                weekly_hosp_admissions float,
                weekly_hosp_admissions_per_million float,
                new_tests INT,
                total_tests INT,
                total_tests_per_thousand float,
                new_tests_per_thousand float,
                new_tests_smoothed INT,
                new_tests_smoothed_per_thousand float,
                positive_rate float,
                tests_per_case float,
                tests_units TEXT,
                total_vaccinations INT,
                people_vaccinated INT,
                people_fully_vaccinated INT,
                new_vaccinations INT,
                new_vaccinations_smoothed INT,
                total_vaccinations_per_hundred float,
                people_vaccinated_per_hundred float,
                people_fully_vaccinated_per_hundred  float,
                new_vaccinations_smoothed_per_million INT,
                stringency_index float,
                population bigint,
                population_density float,
                median_age float,
                aged_65_older float,
                aged_70_older float,
                gdp_per_capita float,
                extreme_poverty float,
                cardiovasc_death_rate float,
                diabetes_prevalence float,
                female_smokers float,
                male_smokers float,
                handwashing_facilities float,
                hospital_beds_per_thousand float,
                life_expectancy float,
                human_development_index float)''')
            context.log.info(f"** CovidData_1, CovidData_2 and CovidData_3 tables are created.")
            db_connect.commit()  # Commit to save our changes
    except Error as e:
        print("Error while connecting to MySQL", e)
    return list_new

@solid
def show_tables(context,list_new):
    db_connect = list_new[0]
    cursor = db_connect.cursor()
    cursor.execute("use DAP_Project_temp")
    cursor.execute("show tables")
    context.log.info(f"List of tables: {cursor.fetchall()}")
    return list_new

@solid
def insert_into_tables(context,list_new):
    db_connect = list_new[0]
    processed_data=list_new[1]
    df1=processed_data[0]
    df2 = processed_data[1]
    df3 = processed_data[2]
    cursor = db_connect.cursor()
    try:
        if db_connect.is_connected():
            for i, row in df1.iterrows():
                sql = "INSERT INTO DAP_Project_temp.CovidData_1 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                db_connect.commit()  # Commit to save our changes


                cursor.execute("select COUNT(*) from CovidData_3")
                result = cursor.fetchall()
                for i in result:
                    print(i)

    except Error as e:
        print("Error while connecting to MySQL", e)

    print("Done importing CovidData_1 into AWS MySQL.")

    # ------------------------------Importing CovidData_2 into AWS RDS MySQL:

    try:
        if db_connect.is_connected():
            for i, row in df2.iterrows():
                sql = "INSERT INTO DAP_Project_temp.CovidData_2 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                db_connect.commit()  # Commit to save our changes

                cursor.execute("select COUNT(*) from CovidData_3")
                result = cursor.fetchall()
                for i in result:
                    print(i)

    except Error as e:
        print("Error while connecting to MySQL", e)

    print("Done importing CovidData_2 into AWS MySQL.")

    # ------------------------------Importing CovidData_3 into AWS RDS MySQL:

    try:
        if db_connect.is_connected():
            for i, row in df3.iterrows():
                sql = "INSERT INTO DAP_Project_temp.CovidData_3 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, tuple(row))
                db_connect.commit()  # Commit to save our changes

                cursor.execute("select COUNT(*) from CovidData_3")
                result = cursor.fetchall()
                for i in result:
                    print(i)

    except Error as e:
        print("Error while connecting to MySQL", e)

    print("Done importing CovidData_3 into AWS MySQL.")
    print("All 3 Datasets are imported into AWS MySQL database: DAP_Project_temp.")
    return db_connect

@solid
def retrieve_data_from_mysql(context,db_connect):
    cursor = db_connect.cursor()
    cursor.execute("use DAP_Project_temp")
    df1 = pd.read_sql('SELECT * FROM CovidData_1', con=db_connect)
    df2 = pd.read_sql('SELECT * FROM CovidData_2', con=db_connect)
    df3 = pd.read_sql('SELECT * FROM CovidData_3', con=db_connect)
    final_list_DF=[df1,df2,df3]
    #context.log.info(f" ** final_list_DF  {final_list_DF}")
    context.log.info(f'** length of DF1 {df1.shape} ,DF2  {df2.shape} and DF3 {df3.shape}')
    return final_list_DF

@solid
def dataset_by_country(context,final_list_DF):
    # Dataset3
    df3_new = final_list_DF[2]
    df2_new = final_list_DF[1]
    df3_grp_loc = df3_new.groupby('location').agg({'new_cases_smoothed': 'sum',
                                                   'new_deaths': 'sum',
                                                   'new_deaths_smoothed': 'sum',
                                                   'total_cases_per_million': 'sum',
                                                   'new_cases_per_million': 'sum',
                                                   'new_cases_smoothed_per_million': 'sum',
                                                   'total_deaths_per_million': 'sum',
                                                   'new_deaths_per_million': 'sum',
                                                   'new_deaths_smoothed_per_million': 'sum',
                                                   'reproduction_rate': 'sum',
                                                   'icu_patients': 'sum',
                                                   'icu_patients_per_million': 'sum',
                                                   'hosp_patients_per_million': 'sum',
                                                   'weekly_icu_admissions': 'sum',
                                                   'weekly_icu_admissions_per_million': 'sum',
                                                   'weekly_hosp_admissions': 'sum',
                                                   'weekly_hosp_admissions_per_million': 'sum',
                                                   'total_tests_per_thousand': 'mean',
                                                   'new_tests_per_thousand': 'mean',
                                                   'new_tests_smoothed_per_thousand': 'sum',
                                                   'positive_rate': 'sum',
                                                   'tests_per_case': 'sum',
                                                   'total_vaccinations_per_hundred': 'sum',
                                                   'people_vaccinated_per_hundred': 'sum',
                                                   'people_fully_vaccinated_per_hundred': 'sum',
                                                   'new_vaccinations_smoothed_per_million': 'sum',
                                                   'stringency_index': 'mean',
                                                   'population': 'max',
                                                   'population_density': 'max',
                                                   'median_age': 'sum',
                                                   'aged_65_older': 'sum',
                                                   'aged_70_older': 'sum',
                                                   'gdp_per_capita': 'mean',
                                                   'extreme_poverty': 'sum',
                                                   'cardiovasc_death_rate': 'sum',
                                                   'diabetes_prevalence': 'sum',
                                                   'female_smokers': 'mean',
                                                   'male_smokers': 'sum',
                                                   'handwashing_facilities': 'sum',
                                                   'hospital_beds_per_thousand': 'sum',
                                                   'life_expectancy': 'mean',
                                                   'human_development_index': 'mean'})
    l3 = list(df3_grp_loc.index.values)
    l2 = list(df2_new['country_name'].unique())
    context.log.info(f"{l2}")
    l2 = [i.upper() for i in l2]
    l3 = [i.upper() for i in l3]
    l4 = []
    for i in l3:
        if i in l2:
            l4.append(i)
    context.log.info(f"{l4}")

    #Dataset2

    df2_new = final_list_DF[1]
    df2_new['country_name'] = df2_new['country_name'].apply(lambda x: x.upper())
    df2_new = df2_new.loc[df2_new['country_name'].apply(lambda x: True if x in l4 else False), :]
    context.log.info(f"{df2_new.country_name.unique()}")
    df2_grp_loc = df2_new.groupby(['country_name']).agg(
        {'dailydeceased': 'mean', 'totaldeceased': 'max', 'dailyconfirmed': 'mean', 'dailyrecovered': 'mean',
         'totalconfirmed': 'max', 'totalrecovered': 'max'})


    # Dataset1
    df1_new = final_list_DF[0]
    df1_new['country'] = df1_new['country'].apply(lambda x: x.upper())
    df1_new = df1_new.loc[df1_new['country'].apply(lambda x: True if x in l4 else False), :]
    context.log.info(f"{df1_new.country.unique()}")
    df1_new_grp = df1_new.groupby('country').agg(
        {'total_vaccinations': 'max', 'people_vaccinated': 'max', 'daily_vaccinations_raw': 'mean',
         'daily_vaccinations': 'mean', 'total_vaccinations_per_hundred': 'mean', 'people_vaccinated_per_hundred': 'sum',
         'people_fully_vaccinated_per_hundred': 'mean', 'daily_vaccinations_per_million': 'mean'})

    list_of_df_to_combine=[df1_new_grp,df2_grp_loc,df3_grp_loc]
    return list_of_df_to_combine

@solid
def combining_dataset(context,list_of_df_to_combine):
    df_comb_loc = pd.concat(list_of_df_to_combine, axis=1)
    df_comb_loc.dropna(inplace=True)
    context.log.info(f"{df_comb_loc}")
    context.log.info(f"{df_comb_loc['dailyconfirmed']}")
    context.log.info(f"{df_comb_loc.info()}")
    df_comb_loc.reset_index(inplace=True)
    df_comb_loc.rename(columns={'index': 'country'}, inplace=True)
    context.log.info(f"{df_comb_loc.T.drop_duplicates().T}")
    return df_comb_loc


@solid
def processing_combined_data(context,df_comb_loc):
    df_comb_loc.loc[:, ['country',
                        'total_vaccinations',
                        'people_vaccinated',
                        'daily_vaccinations_raw',
                        'daily_vaccinations',
                        'people_vaccinated_per_hundred',
                        'people_fully_vaccinated_per_hundred',
                        'daily_vaccinations_per_million',
                        'dailydeceased',
                        'totaldeceased',
                        'dailyconfirmed',
                        'dailyrecovered',
                        'totalconfirmed',
                        'totalrecovered',
                        'new_cases_smoothed',
                        'new_deaths',
                        'new_deaths_smoothed',
                        'total_cases_per_million',
                        'new_cases_per_million',
                        'new_cases_smoothed_per_million',
                        'total_deaths_per_million',
                        'new_deaths_per_million',
                        'new_deaths_smoothed_per_million',
                        'reproduction_rate',
                        'icu_patients',
                        'icu_patients_per_million',
                        'hosp_patients_per_million',
                        'weekly_icu_admissions',
                        'weekly_icu_admissions_per_million',
                        'weekly_hosp_admissions',
                        'weekly_hosp_admissions_per_million',
                        'total_tests_per_thousand',
                        'new_tests_per_thousand',
                        'new_tests_smoothed_per_thousand',
                        'positive_rate',
                        'tests_per_case',
                        'total_vaccinations_per_hundred',
                        'new_vaccinations_smoothed_per_million',
                        'stringency_index',
                        'population',
                        'population_density',
                        'median_age',
                        'aged_65_older',
                        'aged_70_older',
                        'gdp_per_capita',
                        'extreme_poverty',
                        'cardiovasc_death_rate',
                        'diabetes_prevalence',
                        'female_smokers',
                        'male_smokers',
                        'handwashing_facilities',
                        'hospital_beds_per_thousand',
                        'life_expectancy',
                        'human_development_index']]
    df_comb_loc = df_comb_loc.loc[:, ~df_comb_loc.columns.duplicated()]
    #context.log.info(f"{df_comb_loc}")
    #context.log.info(f"{df_comb_loc.info()}")
    df_float = df_comb_loc.select_dtypes(float)
    df_comb_loc.drop(labels=df_float.columns.values, axis=1, inplace=True)
    df_comb_loc = pd.concat([df_comb_loc, df_float.apply(lambda x: round(x, 2))], axis=1)
    #context.log.info(f"{df_comb_loc}")
    return df_comb_loc

@solid
def IBM_watson_db_connection(context,df_comb_loc):
    dsn_hostname = "dashdb-txn-sbox-yp-lon02-13.services.eu-gb.bluemix.net"
    dsn_uid = "qkx18399"
    dsn_pwd = "k35szmcv0@68hgn9"

    dsn_driver = "{IBM DB2 ODBC DRIVER}"
    dsn_database = "BLUDB"
    dsn_port = "50000"
    dsn_protocol = "TCPIP"

    # Create the dsn connection string
    dsn = (
        "DRIVER={0};"
        "DATABASE={1};"
        "HOSTNAME={2};"
        "PORT={3};"
        "PROTOCOL={4};"
        "UID={5};"
        "PWD={6};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd)

    # print the connection string to check correct values are specified
    context.log.info(f"{dsn}")

    # DO NOT MODIFY THIS CELL. Just RUN it with Shift + Enter
    # Create database connection

    try:
        conn = ibm_db.connect(dsn, "", "")
        context.log.info(f"Connected to database: {dsn_database} , as user: {dsn_uid}, on host:  {dsn_hostname}")

    except:
        context.log.info(f"Unable to connect:  {ibm_db.conn_errormsg()}")

    # Retrieve Metadata for the Database Server
    server = ibm_db.server_info(conn)

    context.log.info(f"DBMS_NAME: {server.DBMS_NAME}")
    context.log.info(f"DBMS_VER: {server.DBMS_VER}")
    context.log.info(f"DB_NAME: {server.DB_NAME}")

    dict_IBM={
        'connection_link':conn,
        'df_comb_loc':df_comb_loc
    }
    return dict_IBM

@solid
def create_table_in_IBM(context,dict_IBM):
    conn=dict_IBM['connection_link']
    drop_query = 'DROP TABLE COVID_COMB'
    ibm_db.exec_immediate(conn, drop_query)
    query = '''CREATE TABLE COVID_COMB(country  varchar(30),
    total_vaccinations                   decimal(20,2),
    people_vaccinated                    decimal(20,2),
    daily_vaccinations_raw               decimal(20,2),
    daily_vaccinations                   decimal(20,2),
    people_vaccinated_per_hundred        decimal(20,2),
    people_fully_vaccinated_per_hundred  decimal(20,2),
    daily_vaccinations_per_million       decimal(20,2),
    dailydeceased                        decimal(20,2),
    totaldeceased                        decimal(20,2),
    dailyconfirmed                       decimal(20,2),
    dailyrecovered                       decimal(20,2),
    totalconfirmed                       decimal(20,2),
    totalrecovered                       decimal(20,2),
    new_cases_smoothed                   decimal(20,2),
    new_deaths                           int,
    new_deaths_smoothed                  decimal(20,2),
    total_cases_per_million              decimal(20,2),
    new_cases_per_million                decimal(20,2),
    new_cases_smoothed_per_million       decimal(20,2),
    total_deaths_per_million             decimal(20,2),
    new_deaths_per_million               decimal(20,2),
    new_deaths_smoothed_per_million      decimal(20,2),
    reproduction_rate                    decimal(20,2),
    icu_patients                         int,
    icu_patients_per_million             decimal(20,2),
    hosp_patients_per_million            decimal(20,2),
    weekly_icu_admissions                decimal(20,2),
    weekly_icu_admissions_per_million    decimal(20,2),
    weekly_hosp_admissions               decimal(20,2),
    weekly_hosp_admissions_per_million   decimal(20,2),
    total_tests_per_thousand             decimal(20,2),
    new_tests_per_thousand               decimal(20,2),
    new_tests_smoothed_per_thousand      decimal(20,2),
    positive_rate                        decimal(20,2),
    tests_per_case                       decimal(20,2),
    total_vaccinations_per_hundred       decimal(20,2),
    new_vaccinations_smoothed_per_million int,
    stringency_index                     decimal(20,2),
    population                           int,
    population_density                   decimal(20,2),
    median_age                           decimal(20,2),
    aged_65_older                        decimal(20,2),
    aged_70_older                        decimal(20,2),
    gdp_per_capita                       decimal(20,2),
    extreme_poverty                      decimal(20,2),
    cardiovasc_death_rate                decimal(20,2),
    diabetes_prevalence                  decimal(20,2),
    female_smokers                       decimal(20,2),
    male_smokers                         decimal(20,2),
    handwashing_facilities               decimal(20,2),
    hospital_beds_per_thousand           decimal(20,2),
    life_expectancy                      decimal(20,2),
    human_development_index              decimal(20,2))'''
    ibm_db.exec_immediate(conn, query)

    return dict_IBM

@solid
def insert_into_IBM_table(context,dict_IBM):
    conn=dict_IBM['connection_link']
    df_comb_loc=dict_IBM['df_comb_loc']
    for i in range(df_comb_loc.shape[0]):
        a = df_comb_loc.iloc[i, :]
        context.log.info(f"{len(a)}")
        print("'" + str(a[0]) + "'", a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10], a[11], a[12], a[13],
              a[14], a[15], a[16], a[17], a[18], a[19], a[20], a[21], a[22], a[23], a[24], a[25], a[26], a[27], a[28],
              a[29], a[30], a[31], a[32], a[33], a[34], a[35], a[36], a[37], a[38], a[39], a[40], a[41], a[42], a[43],
              a[44], a[45], a[46], a[47], a[48], a[49], a[50], a[51], a[52], a[53])
        print(
            "INSERT INTO COVID_COMB VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} )".format(
                a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10], a[11], a[12], a[13], a[14], a[15],
                a[16], a[17], a[18], a[19], a[20], a[21], a[22], a[23], a[24], a[25], a[26], a[27], a[28], a[29], a[30],
                a[31], a[32], a[33], a[34], a[35], a[36], a[37], a[38], a[39], a[40], a[41], a[42], a[43], a[44], a[45],
                a[46], a[47], a[48], a[49], a[50], a[51], a[52], a[53]))
        sql = "INSERT INTO COVID_COMB VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(
            "'" + str(a[0]) + "'", a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10], a[11], a[12], a[13],
            a[14], a[15], a[16], a[17], a[18], a[19], a[20], a[21], a[22], a[23], a[24], a[25], a[26], a[27], a[28],
            a[29], a[30], a[31], a[32], a[33], a[34], a[35], a[36], a[37], a[38], a[39], a[40], a[41], a[42], a[43],
            a[44], a[45], a[46], a[47], a[48], a[49], a[50], a[51], a[52], a[53])
        ibm_db.exec_immediate(conn, sql)


@solid
def get_file_List(context):
    return ['Covid_dataset1.csv','Covid_dataset2.csv','Covid_dataset3.csv','world_countries.json']
    #return ['DAP_sam1_up.csv','DAP_sam2_up.csv','DAP_sam3_up.csv']

@solid
def print_here(context,param):
    context.log.info(f"** inside print function :  {param} ")


@pipeline
def second_pipeline():
    data_list=fetch_data_from_mongo_DB(connect_to_mongoDB(read_from_s3(get_file_List())))
    #print_here(data_list)
    processed_data=check_isna(replace_nan_values(check_isna(drop_id_col(create_dataframe_with_pandas(data_list)))))
    #print_here(processed_data)
    final_list_DF=retrieve_data_from_mysql(insert_into_tables(show_tables(aws_sql_create_table(create_DB(establish_mysql_connection(processed_data))))))
    #print_here(final_list_DF)
    insert_into_IBM_table(create_table_in_IBM(IBM_watson_db_connection(processing_combined_data(combining_dataset(dataset_by_country(final_list_DF))))))
