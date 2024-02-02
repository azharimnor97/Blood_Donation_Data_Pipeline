# import libraries
import pandas as pd


# extract data function: Extracting data from Github repo KKM

def source_data_from_csv(csv_file_name):
    try:
        df_csv = pd.read_csv(csv_file_name)

    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"Error: {e}")
        df_csv = pd.DataFrame()

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")
        df_csv = pd.DataFrame()

    else:
        print(f"Data extraction (csv file) success")

    finally:
        return df_csv



def source_data_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)

    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"Error: {e}")
        df_parquet = pd.DataFrame()

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")
        df_parquet = pd.DataFrame()

    else:
        print(f"Data extraction (parquet file) success")

    finally:
        return df_parquet

