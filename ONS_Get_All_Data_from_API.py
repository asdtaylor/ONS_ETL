import requests
import pyodbc
import os
import csv
from collections import Counter

try:
    # Define the ONS datasets URL
    datasets_url = "https://api.beta.ons.gov.uk/v1/datasets"
    
    # SQL Server connection parameters. DB_NAME & DB_SERVER stored as environment variables. 
    server = os.environ.get('DB_NAME')
    database = os.environ.get('DB_SERVER')
    connection_string = f'DRIVER=SQL Server;SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    
    # Establish a connection to SQL Server
    connection = pyodbc.connect(connection_string)
    
    if not connection:
        print("Failed to connect to SQL Server.")
        exit(1)    
    
    cursor = connection.cursor()
    
    # Send a GET request to the datasets URL
    response = requests.get(datasets_url)
    
    if response.status_code != 200:
        print(f"Failed to retrieve data: {response.text}")
        exit(1)
        
    # Parse the JSON response
    datasets_data = response.json()
    
    for dataset in datasets_data['items']:
        dataset_id = dataset['id'].replace("-", "_")
        
        # Get the latest version URL for the dataset
        latest_version_url = dataset['links']['latest_version']['href']
        
        # Send a GET request to the latest version URL
        latest_version_response = requests.get(latest_version_url)
        
        if latest_version_response.status_code != 200:
            print(f"Failed to retrieve the latest version data for dataset {dataset_id}.")
            continue
        
        # Parse the JSON response for the latest version
        latest_version_data = latest_version_response.json()
        
        if 'downloads' not in latest_version_data or 'csv' not in latest_version_data['downloads']:
            print(f"CSV file not found for dataset {dataset_id}.")
            continue
        
        # Get the URL of the CSV file
        csv_url = latest_version_data['downloads']['csv']['href']
        
        # Download the CSV file
        csv_response = requests.get(csv_url)
        
        if csv_response.status_code != 200:
            print(f"Failed to download the CSV file for dataset {dataset_id}.")
            continue

        # Save the data to CSV after modifying duplicate column names
        with open(f'{dataset_id}.csv', 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            reader = csv.reader(csv_response.text.splitlines())
            
            headers = next(reader)
            counter = Counter([x.lower() for x in headers])
            
            new_headers = []
            duplicates = {}
            for header in headers:
                header = header.replace('-', '_')
                header = header.replace (' ', '_')
                header = header.replace('+','_')
                header = header.replace('/','_')
                header_lower = header.lower()
                if counter[header_lower] > 1:
                    if header_lower in duplicates:
                        duplicates[header_lower] += 1
                    else:
                        duplicates[header_lower] = 1
                    new_headers.append(f"{header}{duplicates[header_lower]}")
                else:
                    new_headers.append(header)
            
            writer.writerow(new_headers)
            for row in reader:
                writer.writerow(row)
            
        print(f"CSV file for dataset {dataset_id} downloaded and saved successfully.")
        
        # Check if the table already exists in SQL Server
        table_exists_query = f"IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{dataset_id}') SELECT 1 ELSE SELECT 0"
        table_exists_result = cursor.execute(table_exists_query).fetchone()[0]
        
        # If table doesn't exist, create one
        if table_exists_result == 0:
            header_columns = ', '.join([f"[{col}] NVARCHAR(MAX)" for col in new_headers])
            create_table_sql = f"CREATE TABLE {dataset_id} (id INT IDENTITY(1,1) PRIMARY KEY, {header_columns})"
            cursor.execute(create_table_sql)
            connection.commit()
            print(f"Table for dataset {dataset_id} created in SQL Server.")
        else:
            print(f"Table {dataset_id} already exists in SQL Server.")
        
        #Insert data from each file into its SQL table. this is slow as it loads each individual row.
        with open(f"C:/Users/Andrew/source/repos/ONS Get All Data from API/{dataset_id}.csv") as f:
            reader = csv.reader(f)
            columns = next(reader)
            query = f'INSERT INTO {dataset_id}({",".join(columns)}) values ({",".join("?" * len(columns))})'
            #query = query.format(',' .join(columns), ',' .join('?' * len(columns)))
            cursor = connection.cursor()
            for data in reader:    
                cursor.execute(query, data)
            cursor.commit()
            
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the SQL Server connection
    if connection:
        connection.close()
