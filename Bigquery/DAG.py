from airflow import models, DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
import psycopg2
from psycopg2 import sql
import pandas as pd
import datetime
import logging
from google.cloud import storage
from io import StringIO
import pandas_gbq
import numpy as np

default_dag_args = {
    'start_date': datetime.datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay':datetime.timedelta(seconds=5),
    'project_id': 'dulcet-port-400511',
}



def extract_data_fn(**kwargs):
    #Set the frequently used credentials
    USER = "postgres"
    PASSWORD = "123"
    HOST = "34.93.118.74"
    PORT = "5432"

    # Do not use 'conf' argument in your function, instead use **kwargs
    # Access task instance context using **kwargs
    ti = kwargs['ti']

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host = HOST,
        database = 'oltp',
        user = USER,
        password = PASSWORD
    )
    logging.info("HEY conn establsihed")
    # Open a cursor to perform database operations
    cur = conn.cursor()
    ##dim_orders
    dim_orders_query='SELECT orderid,order_status_update_timestamp,order_status_update from order_details;'
    cur.execute(dim_orders_query)
    results = cur.fetchall()
    # Get column names from the cursor description
    columns = [desc[0] for desc in cur.description]
    # Create a pandas DataFrame from the results
    dim_orders = pd.DataFrame(results, columns=columns)
    dim_orders.head()
    logging.info('HEY %s',dim_orders.head(2))
    
    
    ##dim_products
    dim_product_query='SELECT * from product_master;'
    cur.execute(dim_product_query)
    results = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

        # Create a pandas DataFrame from the results
    dim_product = pd.DataFrame(results, columns=columns)
    logging.info('HEY %s',dim_product.head(2))

    #dim_customers
    dim_cust_query='SELECT customer_master.customerid,customer_master.name,order_details.order_status_update_timestamp,order_details.order_status_update FROM customer_master LEFT JOIN order_details ON customer_master.customerid= order_details.customerid ;' 
    cur.execute(dim_cust_query)
    results = cur.fetchall()
      # Get column names from the cursor description
    columns = [desc[0] for desc in cur.description]

        # Create a pandas DataFrame from the results
    dim_cust = pd.DataFrame(results, columns=columns)
    
    logging.info('HEY %s',dim_cust.head(2))

    #dim_address
    dim_address_query='SELECT customerid,address,city,state,pincode FROM customer_master;' 
    cur.execute(dim_address_query)
    results = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

        # Create a pandas DataFrame from the results
    dim_address = pd.DataFrame(results, columns=columns)
    logging.info('HEY %s',dim_address.head(2))

    #f_order_details
    f_order_details_query='SELECT order_items.orderid,productid,quantity,order_status_update_timestamp,order_status_update FROM order_items LEFT JOIN order_details ON order_details.orderid=order_items.orderid; '
    cur.execute(f_order_details_query)
    results = cur.fetchall()
      # Get column names from the cursor description
    columns = [desc[0] for desc in cur.description]

        # Create a pandas DataFrame from the results
    f_order_details = pd.DataFrame(results, columns=columns)
    logging.info('HEY %s',f_order_details.head(2))

    #f_dailyOrders
    fact_daily_orders_query1='SELECT customer_master.customerid,customer_master.pincode,order_details.orderid,order_details.order_status_update_timestamp,order_details.order_status_update FROM customer_master LEFT JOIN order_details ON customer_master.customerid=order_details.customerid;'
    cur.execute(fact_daily_orders_query1)
    results1 = cur.fetchall()
      # Get column names from the cursor description
    columns = [desc[0] for desc in cur.description]

    # Create a pandas DataFrame from the results
    fact_daily_orders1 = pd.DataFrame(results1, columns=columns)
    logging.info('HEY %s',fact_daily_orders1.head(2))

    #dataframe_2
    
    sql_query1='SELECT * from product_master;'
    cur.execute(sql_query1)
    results1 = cur.fetchall()
      # Get column names from the cursor description
    columns = [desc[0] for desc in cur.description]
    # Create a pandas DataFrame from the results
    dataframe_2 = pd.DataFrame(results1, columns=columns)

    #dataframe_4
    
    sql_query2='SELECT * from order_items;'
    cur.execute(sql_query2)
    results1 = cur.fetchall()
      # Get column names from the cursor description
    columns = [desc[0] for desc in cur.description]
    # Create a pandas DataFrame from the results
    dataframe_4 = pd.DataFrame(results1, columns=columns)

     # Store the DataFrames in a dictionary
    data_to_store = {
        'dim_orders.csv': dim_orders,
        'dim_product.csv': dim_product,
        'dim_cust.csv': dim_cust,
        'dim_address.csv': dim_address,
        'f_order_details.csv': f_order_details,
        'fact_daily_orders.csv': fact_daily_orders1,
        'dataframe_2.csv':dataframe_2,
        'dataframe_4.csv':dataframe_4
    }

    # Upload each DataFrame as a separate CSV file to Google Cloud Storage
    client = storage.Client()
    bucket = client.get_bucket('arpit-bucket')
    storage_uris = {}

    for filename, dataframe in data_to_store.items():
        blob = bucket.blob(f'extracted/{filename}')
        blob.upload_from_string(dataframe.to_csv(index=False), content_type='csv')
        storage_uri = f'gs://{bucket.name}/extracted/{filename}'
        storage_uris[filename] = storage_uri
        logging.info(f'Uploaded {filename} to {storage_uri}')

    return storage_uris



def transform_data_fn(**kwargs):
    ti = kwargs['ti']
    
    storage_uris = ti.xcom_pull(task_ids='Extract_data_OLTP')  # Assuming task_id of the extraction task is 'Extract_data_OLTP'

    # Initialize an empty dictionary to store DataFrames
    dataframes = {}

    # Download and transform each CSV file
    for filename, storage_uri in storage_uris.items():
        logging.info(f'Downloading {filename} from {storage_uri}')
        
        # Download the CSV file from Google Cloud Storage
        client = storage.Client()
        bucket_name, blob_path = storage_uri.replace('gs://', '').split('/', 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        content = blob.download_as_text()

        # Create a DataFrame from the downloaded CSV content
        dataframe = pd.read_csv(StringIO(content))
        
        # Perform transformations on the DataFrame if needed
        # For example: dataframe = dataframe.drop(columns=['unnecessary_column'])

        # Store the transformed DataFrame in the dictionary
        dataframes[filename] = dataframe
        logging.info(f'Transformed {filename} DataFrame: {dataframe.head(2)}')

    # Now, you have six DataFrames in the `dataframes` dictionary
    # You can use these DataFrames for further processing or analysis
    dim_orders = dataframes['dim_orders.csv']
    dim_product = dataframes['dim_product.csv']
    dim_cust = dataframes['dim_cust.csv']
    dim_address = dataframes['dim_address.csv']
    f_order_details= dataframes['f_order_details.csv']
    fact_daily_orders1 = dataframes['fact_daily_orders.csv']
    dataframe_2 =  dataframes['dataframe_2.csv']
    dataframe_4=  dataframes['dataframe_4.csv']

    # Perform your transformations using these DataFrames
    #dim_orders
    dim_orders.rename(columns={
    'orderid': 'order_id',
    'order_status_update_timestamp': 'order_status_update_timestamp',
    'order_status_update': 'order_status'
    }, inplace=True)
    
    #dim_product
    dim_product['start_date'] = pd.to_datetime(datetime.datetime.now().date())
    dim_product['end_date'] = pd.to_datetime(datetime.date(2030, 12, 31), errors='ignore')
    dim_product['end_date'] = dim_product['end_date'].astype(np.datetime64)
    dim_product['start_date'] = dim_product['start_date'].astype(np.datetime64)
    
    dim_product.rename(columns={
    'productid': 'product_id',
    'productcode': 'product_code',
    'productname': 'product_name',
    'sku': 'sku',
    'rate': 'rate',
    'isactive': 'isactive',
    'start_date': 'start_date',
    'end_date': 'end_date'
}, inplace=True)

    
    #dim_address
    dim_address['address_id']=np.arange(100, len(dim_address) + 100)
    

    #dim_customer
    dim_cust = dim_cust[dim_cust['order_status_update'] != 'In_progress']
    # Group by customerid, pincode, and orderid, and find the first 'Received' and 'Delivered' timestamps
    start_date = dim_cust.loc[dim_cust['order_status_update'] == 'Received'].groupby(['customerid', 'name'])['order_status_update_timestamp'].first()
    end_date = dim_cust.loc[dim_cust['order_status_update'] == 'Delivered'].groupby(['customerid','name'])['order_status_update_timestamp'].first()

    # Merge start_date and end_date back to the original DataFrame
    dim_cust= dim_cust.merge(start_date.reset_index(name='start_date'), on=['customerid', 'name'], how='left')
    dim_cust = dim_cust.merge(end_date.reset_index(name='end_date'), on=['customerid','name'], how='left')


    dim_cust = dim_cust.drop(['order_status_update','order_status_update_timestamp'], axis=1)
    dim_cust=dim_cust.drop_duplicates()
    # Merge dim_cust and dim_address on 'customerid' and assign 'address_id' to dim_cust['address_id']
    dim_cust = dim_cust.merge(dim_address[['customerid', 'address_id']], on='customerid', how='left')
    dim_address.drop('customerid',axis=1,inplace=True)
    dim_cust.rename(columns={
   'customerid': 'customer_id',
    'name': 'customer_name',
    'start_date': 'start_date',
    'end_date': 'end_date',
    'address_id': 'addr_id'
}, inplace=True)
    dim_address.rename(columns={
    'address': 'addr',
    'state': 'state',
    'city': 'city',
    'pincode': 'pincode',
    'address_id': 'addr_id'  # Mapping address_id to addr_id

}, inplace=True)

    #f_dailyOrders
    fact_daily_orders1 = fact_daily_orders1[fact_daily_orders1['order_status_update'] != 'In_progress']
    
    # Group by customerid, pincode, and orderid, and find the first 'Received' and 'Delivered' timestamps
    start_date = fact_daily_orders1.loc[fact_daily_orders1['order_status_update'] == 'Received'].groupby(['customerid', 'pincode', 'orderid'])['order_status_update_timestamp'].first()
    end_date = fact_daily_orders1.loc[fact_daily_orders1['order_status_update'] == 'Delivered'].groupby(['customerid', 'pincode', 'orderid'])['order_status_update_timestamp'].first()

    # Merge start_date and end_date back to the original DataFrame
    fact_daily_orders1 = fact_daily_orders1.merge(start_date.reset_index(name='start_date'), on=['customerid', 'pincode', 'orderid'], how='left')
    fact_daily_orders1 = fact_daily_orders1.merge(end_date.reset_index(name='end_date'), on=['customerid', 'pincode', 'orderid'], how='left')
    
    fact_daily_orders1=fact_daily_orders1.drop(['order_status_update_timestamp','order_status_update'],axis=1)
    fact_daily_orders1=fact_daily_orders1.drop_duplicates()

    fact_daily_orders2=pd.merge(fact_daily_orders1, dataframe_4, on='orderid', how='left')
    fact_daily_orders=pd.merge(fact_daily_orders2, dataframe_2, on='productid', how='left')

    fact_daily_orders = fact_daily_orders.drop(['sku','productname','productcode','isactive'], axis=1)
    fact_daily_orders=fact_daily_orders.drop_duplicates()

    fact_daily_orders['total_rate']=fact_daily_orders['quantity']*fact_daily_orders['rate']
    fact_daily_orders.drop(['productid','rate'],axis=1,inplace=True)

    # Group by specified columns and aggregate values
    grouped_df = fact_daily_orders.groupby(['customerid', 'pincode', 'orderid','start_date','end_date']).agg({
    'quantity': 'sum',
    'total_rate': 'sum'
    
}).reset_index()
    
    fact_daily_orders=grouped_df
    fact_daily_orders['order_delivery_time_seconds'] = pd.to_datetime(fact_daily_orders['end_date']) - pd.to_datetime(fact_daily_orders['start_date'])
    fact_daily_orders['order_delivery_time_seconds'] = pd.to_timedelta(fact_daily_orders['order_delivery_time_seconds']).view(np.int64) / 1e9

    fact_daily_orders1=fact_daily_orders
    fact_daily_orders1.rename(columns={
    'customerid': 'cust_id',
    'pincode': 'pincode',
    'orderid': 'order_id',
    'start_date': 'order_received_timestamp',
    'end_date': 'order_delivery_timestamp',
    'order_delivery_time_seconds': 'order_delivery_time_sec',
    'quantity': 'item_count',
    'total_rate': 'order_amount'
}, inplace=True)

    #f_orderDetails

    f_order_details = f_order_details[f_order_details['order_status_update'] == 'Delivered']
    f_order_details = f_order_details.drop('order_status_update', axis=1)
    f_order_details.rename(columns={
    'orderid': 'order_id',
    'productid': 'product_id',
    'quantity': 'quantity',
    'order_status_update_timestamp': 'order_delivery_timestamp'
}, inplace=True)

    dim_orders = dim_orders.drop_duplicates()
    dim_product = dim_product.drop_duplicates()
    dim_cust = dim_cust.drop_duplicates()
    dim_address = dim_address.drop_duplicates()
    f_order_details = f_order_details.drop_duplicates()
    fact_daily_orders1 = fact_daily_orders1.drop_duplicates()
    transformed_dataframes={}
    transformed_dataframes['dim_orders'] = dim_orders
    transformed_dataframes['dim_product'] = dim_product
    transformed_dataframes['dim_address'] = dim_address
    transformed_dataframes['dim_cust'] = dim_cust
    transformed_dataframes['fact_daily_orders'] = fact_daily_orders1
    transformed_dataframes['f_order_details'] = f_order_details

    
   # Upload each DataFrame as a separate CSV file to Google Cloud Storage
    client = storage.Client()
    bucket = client.get_bucket('arpit-bucket')
    storage_uris = {}

    for filename, dataframe in transformed_dataframes.items():
        blob = bucket.blob(f'transformed/{filename}')
        blob.upload_from_string(dataframe.to_csv(index=False), content_type='csv')
        storage_uri = f'gs://{bucket.name}/transformed/{filename}'
        storage_uris[filename] = storage_uri
        logging.info(f'Uploaded {filename} to {storage_uri}')

    return storage_uris



def load_data_fn(**kwargs):
    ti = kwargs['ti']
    
    storage_uris = ti.xcom_pull(task_ids='Transform_data_OLTP_Star')  # Assuming task_id of the extraction task is 'Extract_data_OLTP'

    # Initialize an empty dictionary to store DataFrames
    dataframes = {}

    # Download and transform each CSV file
    for filename, storage_uri in storage_uris.items():
        logging.info(f'Downloading {filename} from {storage_uri}')
        
        # Download the CSV file from Google Cloud Storage
        client = storage.Client()
        bucket_name, blob_path = storage_uri.replace('gs://', '').split('/', 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        content = blob.download_as_text()

        # Create a DataFrame from the downloaded CSV content
        dataframe = pd.read_csv(StringIO(content))
        
        # Perform transformations on the DataFrame if needed
        # For example: dataframe = dataframe.drop(columns=['unnecessary_column'])

        # Store the transformed DataFrame in the dictionary
        dataframes[filename] = dataframe
        logging.info(f'Transformed {filename} DataFrame: {dataframe.head(2)}')

    # Now, you have six DataFrames in the `dataframes` dictionary
    # You can use these DataFrames for further processing or analysis
    dim_orders = dataframes['dim_orders']
    dim_product = dataframes['dim_product']
    dim_cust = dataframes['dim_cust']
    dim_address = dataframes['dim_address']
    f_order_details = dataframes['f_order_details']
    fact_daily_orders = dataframes['fact_daily_orders']
    
    dim_orders['order_status_update_timestamp'] = pd.to_datetime(dim_orders['order_status_update_timestamp'])
    logging.info('%s',dim_orders.dtypes)

    dim_cust['start_date'] = pd.to_datetime(dim_cust['start_date'])
    dim_cust['end_date'] = pd.to_datetime(dim_cust['end_date'])
    logging.info('%s',dim_cust.dtypes)

    dim_product['start_date'] = pd.to_datetime(dim_product['start_date'])
    dim_product['end_date'] = pd.to_datetime(dim_product['end_date'])
    logging.info('%s',dim_product.dtypes)

    fact_daily_orders['order_received_timestamp'] = pd.to_datetime(fact_daily_orders['order_received_timestamp'])
    fact_daily_orders['order_delivery_timestamp'] = pd.to_datetime(fact_daily_orders['order_delivery_timestamp'])
    logging.info('%s',fact_daily_orders.dtypes)

    f_order_details['order_delivery_timestamp'] = pd.to_datetime(f_order_details['order_delivery_timestamp'])
    logging.info('%s',f_order_details.dtypes)



# Now, all specified date columns in all DataFrames have been formatted as strings in the desired format

# Assuming you have a DataFrame 'data' with a column 'datetime_column' containing datetime data

    pandas_gbq.to_gbq(dim_orders, destination_table='dulcet-port-400511.Star_Schema.dim_Orders', project_id='dulcet-port-400511', if_exists='replace')
    pandas_gbq.to_gbq(dim_product, destination_table='dulcet-port-400511.Star_Schema.dim_product', project_id='dulcet-port-400511', if_exists='replace')
    pandas_gbq.to_gbq(dim_cust, destination_table='dulcet-port-400511.Star_Schema.dim_customers', project_id='dulcet-port-400511', if_exists='replace')
    pandas_gbq.to_gbq(dim_address, destination_table='dulcet-port-400511.Star_Schema.dim_address', project_id='dulcet-port-400511', if_exists='replace')
    pandas_gbq.to_gbq(f_order_details, destination_table='dulcet-port-400511.Star_Schema.fact_orderDetails', project_id='dulcet-port-400511', if_exists='replace')
    pandas_gbq.to_gbq(fact_daily_orders, destination_table='dulcet-port-400511.Star_Schema.fact_dailyOrders', project_id='dulcet-port-400511', if_exists='replace')
    logging.info('Uploaded Successfully!!!!!!!!!!!!!!')

with DAG(
        'PIPELINE-ARPIT',
        schedule_interval=None, #manual trigger
        catchup=False,  # Don't backfill previous runs
        default_args=default_dag_args,
        tags=['postgres', 'cloud-sql'],
) as dag:
    
  
    extract_data = PythonOperator(
        task_id='Extract_data_OLTP',
        python_callable=extract_data_fn,
        provide_context=True,  # Pass task instance context to the callable
    )
    
    # PythonOperator to transform data
    transform_data = PythonOperator(
        task_id='Transform_data_OLTP_Star',
        python_callable=transform_data_fn,
        provide_context=True,  # Pass task instance context to the callable
    )

    load_data= PythonOperator(
        task_id='Load_Data_Star',
        python_callable=load_data_fn,
        provide_context=True,  # Pass task instance context to the callable
        
    )

   
    # Define task dependencies
extract_data >> transform_data >> load_data