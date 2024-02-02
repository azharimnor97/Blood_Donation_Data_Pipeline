import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import io
import os
from contextlib import contextmanager
from etl.transform import transform_str
from dotenv import load_dotenv
from datetime import date, timedelta

load_dotenv('source.env')

# Retrieve database key
DB_HOSTNAME = os.getenv('DB_HOSTNAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')
DB_DATABASE = os.getenv('DB_DATABASE')

# Retrieve table name
DB_TABLE_DONATE_FACILITY = os.getenv('DB_TABLE_DONATE_FACILITY')
DB_TABLE_DONATE_STATES = os.getenv('DB_TABLE_DONATE_STATES')
DB_TABLE_NEWDONATE_FACILITY = os.getenv('DB_TABLE_NEWDONATE_FACILITY')
DB_TABLE_NEWDONATE_STATES = os.getenv('DB_TABLE_NEWDONATE_STATES')
DB_TABLE_BLOOD_DONOR = os.getenv('DB_TABLE_BLOOD_DONOR')



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


# ======================================================================================================================
# Latest trends by facility
def daily_trend_facility(latest_date, previous_date):
    try:
        str_latest = latest_date.strftime('%Y-%m-%d')
        #str_previous = previous_date.strftime('%Y-%m-%d')
        with session_scope() as session:
            query1 = f" SELECT `date`,`hospital`,`daily` FROM {DB_TABLE_DONATE_FACILITY} WHERE `date` = '{str_latest}'"
            df1 = pd.read_sql_query(query1, con=session.bind)

            while df1.empty:
                latest_date = latest_date - timedelta(days=1)
                previous_date = previous_date - timedelta(days=1)
                str_latest = latest_date.strftime('%Y-%m-%d')
                query1 = f" SELECT `date`,`hospital`,`daily` FROM {DB_TABLE_DONATE_FACILITY} WHERE `date` = '{str_latest}'"
                df1 = pd.read_sql_query(query1, con=session.bind)
            else:
                str_previous = previous_date.strftime('%Y-%m-%d')
                query2 = f" SELECT `date`,`hospital`,`daily` FROM {DB_TABLE_DONATE_FACILITY} WHERE `date` = '{str_previous}'"
                df2 = pd.read_sql_query(query2, con=session.bind)
                return print_daily_trend_facility(df1, df2, latest_date, previous_date)

    except Exception as e:
        print("Data load error: " + str(e))
        return None, None


def print_daily_trend_facility(df1, df2, latest_date, previous_date):
    try:
        # Merging 2 dataframe into 1 new dataframe
        daily_trends = pd.merge(df2, df1, how='outer', on='hospital', sort=True, suffixes=('_x', '_y'))
        daily_trends.drop(['date_x', 'date_y'], axis=1, inplace=True)
        daily_trends.rename(columns={'daily_x': previous_date, 'daily_y': latest_date}, inplace=True)

        # Comparing daily donation trends
        daily_trends['trends'] = daily_trends[latest_date] - daily_trends[previous_date]

        # Print trends
        message = "The following are the latest trends of blood donation in each donation facility in Malaysia\n"
        for index, row in daily_trends.iterrows():
            message += "\n"
            message += f"Donation Facility: {row['hospital']}\n"
            message += f"Donation on {previous_date}: {row[previous_date]}\n"
            message += f"Donation on {latest_date}: {row[latest_date]}\n"
            message += f"Donation trend: {row['trends']}\n"
        return message, daily_trends, daily_trend_facility_viz(daily_trends,latest_date, previous_date)

    except Exception as e:
        print("Error: " + str(e))
        return None, None


def daily_trend_facility_viz(df,latest_date,previous_date):
    daily_trends_facility_viz = df.copy().set_index('hospital')
    ax = daily_trends_facility_viz[[previous_date, latest_date]].plot.bar(fontsize=8.0,
                                                                              xlabel="Blood Donation Facility",
                                                                              ylabel="Number of blood donation",
                                                                              figsize=(15, 5), stacked=False,
                                                                              title=F"Latest daily trend of blood donation at each facility in Malaysia")
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()  # Close the figure
    return buf


# ==========================================================================================================================
# Get data for trend by facility function (7 days & 30 days)
def trend_days_data(trends_date):
    try:
        str_trends = trends_date.strftime('%Y-%m-%d')
        with session_scope() as session:
            query = f" SELECT `date`,`hospital`,`daily` FROM {DB_TABLE_DONATE_FACILITY} WHERE `date` >= '{str_trends}'"
            df = pd.read_sql_query(query, con=session.bind)
            return trend_days_data_viz(df)

    except Exception as e:
        print("Data load error: " + str(e))
        return None


# Create data viz
def trend_days_data_viz(df):
    df_pivot = df.pivot_table(index='date', columns='hospital', values='daily')

    # Generate plot
    df_pivot_viz = df_pivot.plot(kind='line', figsize=(15, 30), subplots=True, layout=(11, 2), fontsize=8.0, rot=45.0)

    # Save the figure to a BytesIO object
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()  # Close the figure
    return buf


# ==========================================================================================================================
# Get data for yearly trend by facility (2013 - 2023)
def trend_year_data():
    try:
        with session_scope() as session:
            query = (f"SELECT `date`,`hospital`,`daily` FROM `donation_facility` WHERE `date` >= '2013-01-01' AND "
                     f"`date` < '2024-01-01' ORDER BY `donation_facility`.`date`,`donation_facility`.`hospital` ASC;")
            df = pd.read_sql_query(query, con=session.bind)
            df.loc[:, 'year'] = df['date'].dt.year
            df_year_sum = df.groupby(['year', 'hospital']).sum('daily')
            df_year_pivot = df_year_sum.pivot_table(index='year', columns='hospital', values='daily')

            # Generate plot
            x_ticks = (2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)
            df_year_viz = df_year_pivot.plot(figsize=(15, 30), subplots=True, layout=(11, 2), xticks=x_ticks,
                                             fontsize=8.0, rot=45.0)

            # Save the figure to a BytesIO object
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            plt.close()  # Close the figure
            return buf

    except Exception as e:
        print("Data load error: " + str(e))
        return None


# =========================================================================================================================
# Blood donation retention

# Retrieve retention data
def retrieve_retention_data(year):
    if year == 2023:
        # code here
        try:
            with session_scope() as session:
                query1 = f"SELECT DISTINCT donor_id, visit_date, birth_date FROM blood_donor WHERE visit_date >= '2023-01-01' AND visit_date < '2024-01-01';"
                query2 = f"SELECT DISTINCT donor_id, visit_date, birth_date FROM blood_donor WHERE visit_date >= '2022-01-01' AND visit_date < '2023-01-01';"
                df1 = pd.read_sql_query(query1, con=session.bind)
                df2 = pd.read_sql_query(query2, con=session.bind)
                return retention_rate(df1, df2, year)
                # return df1, df2
        except Exception as e:
            print("Data load error: " + str(e))
            return None
    elif year == 2022:
        # code here
        try:
            with session_scope() as session:
                query1 = f"SELECT DISTINCT donor_id, visit_date, birth_date FROM blood_donor WHERE visit_date >= '2022-01-01' AND visit_date < '2023-01-01';"
                query2 = f"SELECT DISTINCT donor_id, visit_date, birth_date FROM blood_donor WHERE visit_date >= '2021-01-01' AND visit_date < '2022-01-01';"
                df1 = pd.read_sql_query(query1, con=session.bind)
                df2 = pd.read_sql_query(query2, con=session.bind)
                return retention_rate(df1, df2, year)
                # return df1, df2
        except Exception as e:
            print("Data load error: " + str(e))
            return None
    elif year == 2021:
        # code here
        try:
            with session_scope() as session:
                query1 = f"SELECT DISTINCT donor_id, visit_date, birth_date FROM blood_donor WHERE visit_date >= '2021-01-01' AND visit_date < '2022-01-01';"
                query2 = f"SELECT DISTINCT donor_id, visit_date, birth_date FROM blood_donor WHERE visit_date >= '2020-01-01' AND visit_date < '2021-01-01';"
                df1 = pd.read_sql_query(query1, con=session.bind)
                df2 = pd.read_sql_query(query2, con=session.bind)
                return retention_rate(df1, df2, year)
                # return df1, df2
        except Exception as e:
            print("Data load error: " + str(e))
            return None
    elif year == 2020:
        # code here
        try:
            with session_scope() as session:
                query1 = f"SELECT DISTINCT donor_id, visit_date, birth_date FROM blood_donor WHERE visit_date >= '2020-01-01' AND visit_date < '2021-01-01';"
                query2 = f"SELECT DISTINCT donor_id, visit_date, birth_date FROM blood_donor WHERE visit_date >= '2019-01-01' AND visit_date < '2020-01-01';"
                df1 = pd.read_sql_query(query1, con=session.bind)
                df2 = pd.read_sql_query(query2, con=session.bind)
                return retention_rate(df1, df2, year)
                # return df1, df2
        except Exception as e:
            print("Data load error: " + str(e))
            return None
    else:
        # code here
        message = f"Data is currently not available"
        return message


# Calculate retention data and return message
def retention_rate(df1, df2, year):  # (current year, previous year)
    # Change donor_id datatype to string
    df1 = transform_str(df1)  # current (selected) year
    df2 = transform_str(df2)

    # Drop duplicate donor_id
    dupli_drop_df1 = df1.drop_duplicates(subset='donor_id', keep='first', ignore_index=True)  # current (selected) year
    dupli_drop_df2 = df2.drop_duplicates(subset='donor_id', keep='first', ignore_index=True)

    # Merge df to get the donor_id that exists in both df
    merged_df = pd.merge(dupli_drop_df1, dupli_drop_df2, how='inner', on='donor_id')

    # Donation Retention Rate = (number of returning donor in current year/Total number of donor in previous year)
    returning_donor = len(merged_df)  # number of returning donor in current (selected) year
    total_donor_previous = len(dupli_drop_df2)  # Total number of donor in previous year
    cal_retention_rate = (returning_donor / total_donor_previous) * 100
    retention_rate_percent = "{:.2f}".format(cal_retention_rate)
    message = f"The blood donor retention rate for the {year} is {retention_rate_percent} %. The number of returning blood donor in {year} is {returning_donor}, compared to {total_donor_previous} total number of blood donor in the previous year"
    return message
