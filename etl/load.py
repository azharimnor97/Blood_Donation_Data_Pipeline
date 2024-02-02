import pandas as pd
import mysql.connector
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv('source.env')

#Retreive access key to database
DB_HOSTNAME = os.getenv('DB_HOSTNAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')
DB_DATABASE = os.getenv('DB_DATABASE')

@contextmanager
def session_scope():
    # Create a connection string
    connection_string = f'mysql+mysqlconnector://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOSTNAME}:{DB_PORT}/{DB_DATABASE}'

    # Create a connection engine
    engine = create_engine(connection_string)

    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


# Use the session in your function
def load(df, table_name):
    try:
        rows_imported = 0
        with session_scope() as session:
            # Write DataFrame to table_name in database
            df.to_sql(table_name, con=session.get_bind(), if_exists='replace', index=False, chunksize=1000)
            rows_imported += len(df)
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

def modify_table(table_name):
    try:
        with session_scope() as session:
            query = text(
                f"ALTER TABLE `{table_name}` CHANGE `donor_id` `donor_id` CHAR(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL;")
            session.execute(query)
            session.commit()
        print("Table column has been modified")
    except Exception as e:
        print("Data load error: " + str(e))
