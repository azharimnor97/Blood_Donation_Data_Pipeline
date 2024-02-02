import pandas as pd


# transformation function: date -> change dtype from object to datetime
def transform_date(df):
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    elif 'visit_date' and 'birth_date' in df.columns:
        df['visit_date'] = pd.to_datetime(df['visit_date'])
        df['birth_date'] = pd.to_datetime(df['birth_date'])
    else:
        print("Column doesnt' exist")

    return df


# transformation function: hospital -> change dtype from object to string
def transform_str(df):
    if 'hospital' in df.columns:
        df['hospital'] = df[['hospital']].convert_dtypes(infer_objects=True)
    elif 'state' in df.columns:
        df['state'] = df[['state']].convert_dtypes(infer_objects=True)
    elif 'donor_id' in df.columns:
        df['donor_id'] = df[['donor_id']].convert_dtypes(infer_objects=True)
    else:
        print("Column doesnt' exist")

    return df
