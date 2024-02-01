import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from contextlib import contextmanager
from etl.extract import source_data_from_csv, source_data_from_parquet
from etl.transform import transform_date, transform_str
from etl.load import load
from dotenv import load_dotenv
from datetime import datetime, time, date, timedelta

load_dotenv('source.env')

#Retreive access key to database
DB_HOSTNAME = os.getenv('DB_HOSTNAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')
DB_DATABASE = os.getenv('DB_DATABASE')

#Retrieve table name
DB_TABLE_DONATE_FACILITY = os.getenv('DB_TABLE_DONATE_FACILITY')
DB_TABLE_DONATE_STATES = os.getenv('DB_TABLE_DONATE_STATES')
DB_TABLE_NEWDONATE_FACILITY = os.getenv('DB_TABLE_NEWDONATE_FACILITY')
DB_TABLE_NEWDONATE_STATES = os.getenv('DB_TABLE_NEWDONATE_STATES')
DB_TABLE_BLOOD_DONOR = os.getenv('DB_TABLE_BLOOD_DONOR')

#Retrieve data source access link/path
DONATION_FACILITY = os.getenv('DONATION_FACILITY')
DONATION_STATE = os.getenv('DONATION_STATE')
NEWDONOR_FACILITY = os.getenv('NEWDONOR_FACILITY')
NEWDONOR_STATE = os.getenv('NEWDONOR_STATE')
BLOOD_DONOR = os.getenv('BLOOD_DONOR')

# =========================================================
# Extract Data
df_donation_facility = source_data_from_csv(DONATION_FACILITY)
df_donation_state = source_data_from_csv(DONATION_STATE)
df_newdonor_facility = source_data_from_csv(NEWDONOR_FACILITY)
df_newdonor_state = source_data_from_csv(NEWDONOR_STATE)
df_blood_donor = source_data_from_parquet(BLOOD_DONOR)

# =========================================================
# Transform Data
#transform date column to datetime dtype
df_donation_facility = transform_date(df_donation_facility)
df_donation_state = transform_date(df_donation_state)
df_newdonor_facility = transform_date(df_newdonor_facility)
df_newdonor_state = transform_date(df_newdonor_state)
df_blood_donor = transform_date(df_blood_donor)

#transform hospital/state column to string dtype
df_donation_facility = transform_str(df_donation_facility)
df_donation_state = transform_str(df_donation_state)
df_newdonor_facility = transform_str(df_newdonor_facility)
df_newdonor_state = transform_str(df_newdonor_state)
df_blood_donor = transform_str(df_blood_donor)

# =========================================================
# Transform Data
load(df_donation_facility,DB_TABLE_DONATE_FACILITY)
load(df_donation_state,DB_TABLE_DONATE_STATES)
load(df_newdonor_facility,DB_TABLE_NEWDONATE_FACILITY)
load(df_newdonor_state,DB_TABLE_NEWDONATE_STATES)
load(df_blood_donor,DB_TABLE_BLOOD_DONOR)

