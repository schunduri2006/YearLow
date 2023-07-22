import yfinance as yf
import pandas as pd
from io import StringIO
from google.cloud import storage
from datetime import datetime

def readFiles(fileName, bucket):
  client = storage.Client()
  bucket = client.bucket(bucket)
  blob = bucket.get_blob(fileName)
  fileContents = blob.download_as_text()
  df = pd.read_csv(StringIO(fileContents))
  return df


def runMarketTime():
    client = storage.Client()
    bucket = client.bucket('stkbucket2')
    weekday_files = []
    blobs = list(bucket.list_blobs())
    blobs.sort(key=lambda x: x.time_created, reverse=True)

    df1 = None
    df2 = None

    for blob in blobs:
        try:
            date = blob.name.split("_")[-1].split(".")[0]
            file_date = datetime.strptime(date, "%Y-%m-%d")

            if file_date.weekday() < 5:
                weekday_files.append(blob.name)

                if len(weekday_files) == 1:
                    df1 = readFiles(weekday_files[0], "stkbucket2")
                    df2 = readFiles(weekday_files[1], "stkbucket2")

        except Exception as e:
            print(e)

    if len(weekday_files) < 2:
        print("Could not find two recent files created on weekdays.")

    return df1, df2



def fetch_csv_from_bucket(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_contents = blob.download_as_text()

    df = pd.read_csv(StringIO(file_contents))

    return df

def process_stock_groups(data):
    df_filtered = pd.DataFrame(columns=['stk_code', 'dayclose', 'yearlow', 'yearhigh'])

    for stk_code in data:
        try:
            stk_info = yf.Ticker(stk_code)
            print(stk_code)
            if  ((stk_info.fast_info.last_price /stk_info.fast_info.year_low) -1) *100 < 10.0 :
                df_filtered.loc[len(df_filtered)] = [stk_code,stk_info.fast_info.last_price,stk_info.fast_info.year_low,stk_info.fast_info.year_high]
                print(((stk_info.fast_info.last_price /stk_info.fast_info.year_low) -1) *100)
        except Exception as e:
            print(f"Error processing stock: {stk_code} + {e}")

    return df_filtered

def main(bucket_name, file_name):
    stk_list = fetch_csv_from_bucket(bucket_name, file_name)['stk_code']
    df_filtered = process_stock_groups(stk_list)
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    output_file = f"stk_year_low_price_{today}.csv"

    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(output_file)
        blob.upload_from_string(df_filtered.to_csv(), content_type='text/csv') 
        print(f"Output file '{output_file}' saved and uploaded successfully.")
    except Exception as e:
        print(f"Error saving or uploading the output file: {e}")
    

def hello_pubsub(event, context):
    bucket_name = 'stkbucket2'
    file_name = 'naslist.csv'
    main(bucket_name, file_name)
    df1, df2 = runMarketTime()
    try: 
        if df1 is not None and df2 is not None:
            newDataFrame = pd.concat([df1, df2]).drop_duplicates(subset=['stk_code'])
            newDataFrame = newDataFrame.reset_index(drop=True)
            newDataFrame = newDataFrame.loc[:, ['stk_code']]
            print(newDataFrame)
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('stkbucket2')
            blob = bucket.blob(f'merged_file.csv')
            blob.upload_from_string(newDataFrame.to_csv(), content_type='text/csv')
    except Exception as e: 
        print("Error: " + e)
