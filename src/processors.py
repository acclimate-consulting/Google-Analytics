from pandas.io.json import json_normalize
import json
import os
import pandas as pd

def load_df(csv_path='data/test_v2.csv.zip', nrows=10000):
    
    '''
    Data loading for zipped or unpacked csv files. Specify csv path and number of rows to read. 
    
    args:
        csv_path <string>: relative path of csv file
        nrows <int>: Number of rows of file to read. Useful for reading pieces of large files
        
    returns:
        DataFrame <dataframe> Pandas DataFrame or TextFileReader for iteration
    '''
    
    file_extension = os.path.splitext(csv_path)
    JSON_COLUMNS = ['device', 'geoNetwork', 'totals', 'trafficSource']
    
    if file_extension == '.zip':
        df = pd.read_csv(csv_path, 
                         converters={column: json.loads for column in JSON_COLUMNS}, 
                         dtype={'fullVisitorId': 'str'}, # Important!!
                         nrows=nrows,
                         compression='zip')
    else:
        df = pd.read_csv(csv_path, 
                         converters={column: json.loads for column in JSON_COLUMNS}, 
                         dtype={'fullVisitorId': 'str'}, # Important!!
                         nrows=nrows)        

    for column in JSON_COLUMNS:
        column_as_df = json_normalize(df[column])

        column_as_df.columns = ["{}.{}".format(column, subcolumn) for subcolumn in column_as_df.columns]
        df = df.drop(column, axis=1).merge(column_as_df, right_index=True, left_index=True)
            
    return df

def generate_df(csv_path='data/test_v2.csv.zip', chunksize=100, nrows=10000):
    
    '''
    Data Generator for zipped or unpacked csv files. Specify csv path and chunk size for
    each batch, or nrows to read. 
    
    args:
        csv_path <string>: relative path of csv file
        chunksize <int>: number of rows to read in per batch
                        Return TextFileReader object for iteration.
                        See the `IO Tools docs
                        <http://pandas.pydata.org/pandas-docs/stable/io.html#io-chunking>`_
                        for more information on ``iterator`` and ``chunksize``.
        nrows <int>: Number of rows of file to read. Useful for reading pieces of large files
        
    returns:
        DataFrame <dataframe> Pandas DataFrame or TextFileReader for iteration
    '''
    
    file_extension = os.path.splitext(csv_path)
    JSON_COLUMNS = ['device', 'geoNetwork', 'totals', 'trafficSource']
    
    if file_extension == '.zip':
        df = pd.read_csv(csv_path, 
                         converters={column: json.loads for column in JSON_COLUMNS}, 
                         dtype={'fullVisitorId': 'str'}, # Important!!
                         nrows=nrows,
                         compression='zip',
                         chunksize=chunksize,
                         iterator=True)
    else:
        df = pd.read_csv(csv_path, 
                         converters={column: json.loads for column in JSON_COLUMNS}, 
                         dtype={'fullVisitorId': 'str'}, # Important!!
                         nrows=nrows,
                         chunksize=chunksize,
                         iterator=True)        

    for batch in df:
        for column in JSON_COLUMNS:
            column_as_df = json_normalize(batch[column])

            column_as_df.columns = ["{}.{}".format(column, subcolumn) for subcolumn in column_as_df.columns]
            batch = batch.drop(column, axis=1).merge(column_as_df, right_index=True, left_index=True)

        return df