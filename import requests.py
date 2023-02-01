from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

# Initialize SparkContext and SparkConf
sc = SparkContext(appName="JSONFileToHiveTable")
conf = SparkConf().setAppName("JSONFileToHiveTable")

# Create SparkSession
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
hiveContext = HiveContext(sparkContext=spark.sparkContext, hiveContext=spark)

# Read the JSON file and convert it to a DataFrame
df = spark.read.json("path/to/json_file.json")

# Register the DataFrame as a temporary table in Hive
df.createOrReplaceTempView("temp_table")

# Use the HiveContext to write the data to the Hive table
hiveContext.sql("""
  INSERT INTO table_name
  SELECT * FROM temp_table
  ON DUPLICATE KEY UPDATE
    column1 = temp_table.column1,
    column2 = temp_table.column2,
    ...
""")

# Stop the SparkContext
sc.stop()

#---
import pyodbc
import pandas as pd

# Connect to SQL Server
conn = pyodbc.connect('DRIVER={SQL Server};'
                      'SERVER=localhost;'
                      'DATABASE=mydatabase;'
                      'UID=myusername;'
                      'PWD=mypassword;')

# Create a cursor from the connection
cursor = conn.cursor()

# Load the data into a pandas DataFrame
data = pd.DataFrame({'ID': [1, 2, 3, 4],
                     'Name': ['John', 'Jane', 'Jim', 'Jill'],
                     'Age': [35, 40, 45, 50],
                     'City': ['New York', 'San Francisco', 'London', 'Paris']})

# Iterate through each row in the DataFrame and update/insert the data into the SQL Server table
for index, row in data.iterrows():
    try:
        # Try to update the data in the SQL Server table
        cursor.execute("UPDATE mytable SET Name=?, Age=?, City=? WHERE ID=?",
                       row['Name'], row['Age'], row['City'], row['ID'])
        conn.commit()
    except Exception as e:
        # If the update fails, insert the data into the SQL Server table
        cursor.execute("INSERT INTO mytable (ID, Name, Age, City) VALUES (?, ?, ?, ?)",
                       row['ID'], row['Name'], row['Age'], row['City'])
        conn.commit()

# Close the cursor and the connection
cursor.close()
conn.close()

#---
import pyodbc

# Connect to the SQL Server database
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost;'
                      'Database=mydatabase;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

# Define the data to be inserted/updated
data = [("John", "Doe", 25),
        ("Jane", "Doe", 30),
        ("Jim", "Smith", 35)]

# Loop through the data and insert/update the table
for record in data:
    first_name, last_name, age = record

    # Use the upsert syntax to either insert or update the record
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM mytable WHERE first_name=? AND last_name=?)
        BEGIN
            INSERT INTO mytable (first_name, last_name, age)
            VALUES (?, ?, ?)
        END
        ELSE
        BEGIN
            UPDATE mytable SET age=? WHERE first_name=? AND last_name=?
        END
    """, (first_name, last_name, first_name, last_name, age, age, first_name, last_name))

# Commit the changes to the database
conn.commit()

# Close the connection to the database
conn.close()



#---
This code sets up a Kafka consumer and producer, as well as connections to both the MySQL and PostgreSQL databases. 
The consumer listens for changes in the Kafka topic database_changes and the producer produces changes to the same topic. 
The code uses a while loop to continuously poll for changes in the MySQL database and insert those changes into the PostgreSQL database. 
The changes are also produced to the Kafka topic so they can be consumed by other applications.

import psycopg2
import mysql.connector
import json
from kafka import KafkaConsumer, KafkaProducer

# Connect to the MySQL database
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="source_db"
)

# Connect to the PostgreSQL database
postgres_conn = psycopg2.connect(
    host="localhost",
    database="destination_db",
    user="postgres",
    password="password"
)

# Create a cursor for the MySQL database
mysql_cursor = mysql_conn.cursor()

# Create a cursor for the PostgreSQL database
postgres_cursor = postgres_conn.cursor()

# Set up the Kafka consumer
consumer = KafkaConsumer(
    'database_changes',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Set up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Function to insert the changes in the PostgreSQL database
def insert_changes(changes):
    query = "INSERT INTO table_name (column1, column2, column3) VALUES (%s, %s, %s)"
    postgres_cursor.execute(query, changes)
    postgres_conn.commit()

# Function to retrieve the changes from the MySQL database
def retrieve_changes():
    mysql_cursor.execute("SELECT * FROM table_name WHERE updated_at > last_update")
    changes = mysql_cursor.fetchall()
    return changes

# Continuously poll for changes in the MySQL database
while True:
    changes = retrieve_changes()
    for change in changes:
        # Produce the change to the Kafka topic
        producer.send('database_changes', value=change)
        # Insert the change in the PostgreSQL database
        insert_changes(change)

#----------
import os
import logging
import subprocess
import json

from hdfs import InsecureClient

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='data_pipeline.log',
                    filemode='w')

# Set up HDFS client
hdfs = InsecureClient('http://localhost:50070')

# Function to save the json files to HDFS
def save_to_hdfs(file_path):
    with open(file_path, 'rb') as file:
        hdfs.write('/data/' + file_path, file, overwrite=True)
        logging.info(f'{file_path} saved to HDFS')

# Function to create a Hive table using beeline
def create_hive_table(table_name):
    command = f'beeline -u jdbc:hive2://localhost:10000/default -e "CREATE TABLE {table_name} (col1 STRING, col2 INT, col3 DOUBLE)"'
    subprocess.call(command, shell=True)
    logging.info(f'{table_name} Hive table created')

# Function to extract-transform-load each json file to the Hive table
def etl_to_hive(file_path, table_name):
    with open(file_path) as file:
        data = json.load(file)
        for row in data:
            command = f'beeline -u jdbc:hive2://localhost:10000/default -e "INSERT INTO {table_name} VALUES (\'{row["col1"]}\', {row["col2"]}, {row["col3"]})"'
            subprocess.call(command, shell=True)
        logging.info(f'{file_path} ETL to {table_name} Hive table completed')

# Check for new json files in the folder
folder_path = '/data'
for file in os.listdir(folder_path):
    if file.endswith('.json'):
        try:
            # Save the new file to HDFS
            save_to_hdfs(file)
            # Create a Hive table using beeline
            create_hive_table('new_table')
            # Extract-transform-load the json file to the Hive table
            etl_to_hive(file, 'new_table')
        except Exception as e:
            logging.error(f'{file} processing failed due to {e}')

#----
import os
import shutil
import subprocess

def check_for_new_files(src_folder):
    new_files = []
    for filename in os.listdir(src_folder):
        if filename.endswith(".json"):
            new_files.append(filename)
    return new_files

def save_new_files(src_folder, dst_folder, new_files):
    for file in new_files:
        src_path = os.path.join(src_folder, file)
        dst_path = os.path.join(dst_folder, file)
        shutil.copy2(src_path, dst_path)

def create_hive_table(hive_table_name):
    try:
        subprocess.run(["beeline", "-u", "jdbc:hive2://localhost:10000/default", "-e", "create table %s (json string)" % hive_table_name])
    except subprocess.CalledProcessError as e:
        print("Failed to create Hive table: ", e)

def load_data_to_hive_table(dst_folder, hive_table_name):
    for filename in os.listdir(dst_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(dst_folder, filename)
            try:
                subprocess.run(["beeline", "-u", "jdbc:hive2://localhost:10000/default", "-e", "load data local inpath '%s' into table %s" % (file_path, hive_table_name)])
            except subprocess.CalledProcessError as e:
                print("Failed to load data to Hive table: ", e)

if __name__ == "__main__":
    src_folder = "path/to/src/folder"
    dst_folder = "path/to/dst/folder"
    hive_table_name = "hive_table_name"
    new_files = check_for_new_files(src_folder)
    if new_files:
        save_new_files(src_folder, dst_folder, new_files)
        create_hive_table(hive_table_name)
        load_data_to_hive_table(dst_folder, hive_table_name)
    else:
        print("No new files found.")



#----------------
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Row
from pyspark.sql.functions import *

class WikipediaDataPipeline:
    def __init__(self, log_file_path, hive_table_name):
        self.log_file_path = log_file_path
        self.hive_table_name = hive_table_name
        
    def extract_data(self):
        conf = SparkConf().setAppName("Wikipedia Data Pipeline")
        sc = SparkContext(conf=conf)
        sql_context = HiveContext(sc)
        
        log_data = sc.textFile(self.log_file_path)
        
        log_rows = log_data.map(lambda line: line.split(" ")) \
                           .map(lambda fields: Row(timestamp=fields[0], user_id=fields[1], page_id=fields[2], action=fields[3]))
                           
        log_df = sql_context.createDataFrame(log_rows)
        
        return log_df
    
    def insert_data(self, log_df):
        log_df.write.format("hive").saveAsTable(self.hive_table_name)
        
    def run(self):
        log_df = self.extract_data()
        self.insert_data(log_df)

if __name__ == "__main__":
    pipeline = WikipediaDataPipeline("path/to/log/file", "wikipedia_logs")
    pipeline.run()





#--------
import requests
import csv
from pyhive import hive

# Function to extract data from Wikipedia log
def extract_data():
    url = "https://dumps.wikimedia.org/other/pagecounts-raw/2021/2021-01/pagecounts-20210101-000000.gz"
    response = requests.get(url)
    with open("pagecounts.gz", "wb") as f:
        f.write(response.content)

# Function to transform data
def transform_data():
    with open("pagecounts.gz", "rt") as f_input, open("pagecounts_clean.csv", "w") as f_output:
        csv_input = csv.reader(f_input, delimiter=" ")
        csv_output = csv.writer(f_output)
        for row in csv_input:
            csv_output.writerow([row[0], row[1], row[2]])

# Function to load data into Hive datalake
def load_data():
    conn = hive.Connection(host="hostname", port=10000, username="username", password="password", database="wikipedia")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS wikipedia_log (language string, title string, views int)")
    with open("pagecounts_clean.csv", "r") as f:
        cursor.copy(f, "wikipedia_log", sep=",")
    cursor.execute("SELECT * FROM wikipedia_log")
    result = cursor.fetchall()
    print(result)

# Execute functions
extract_data()
transform_data()
load_data()

#----------------------------------------------------
# Import necessary libraries
import requests
import json
from pyspark.sql import SparkSession

# Start SparkSession
spark = SparkSession.builder.appName("WikipediaDataPipeline").config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().getOrCreate()

# Define the Wikipedia log URL
wiki_log_url = "https://dumps.wikimedia.org/other/pagecounts-raw/2019/2019-03/pagecounts-20190301-000000.gz"

# Download the log file and extract the data
log_data = requests.get(wiki_log_url).content

# Convert the data to JSON format
json_data = json.loads(log_data)

# Create a DataFrame from the JSON data
df = spark.createDataFrame(json_data)

# Register the DataFrame as a temporary table
df.createOrReplaceTempView("wikipedia_logs")

# Define the Hive database and table name
hive_db = "wikipedia_data"
hive_table = "pagecounts"

# Insert the data into the Hive datalake
spark.sql(f"INSERT INTO {hive_db}.{hive_table} SELECT * FROM wikipedia_logs")

# Print a message to confirm successful insertion
print("Data successfully inserted into Hive datalake!")
#------------------
import wikipediaapi
import pyhive
from pyhive import hive
from datetime import datetime

# Connect to Wikipedia API
wiki = wikipediaapi.Wikipedia(language='en', extract_format=wikipediaapi.ExtractFormat.WIKI)

# Connect to Hive Data Lake
conn = hive.Connection(host="hostname", port=10000, username="username", password="password")
cursor = conn.cursor()

# Create table in Hive Data Lake to store Wikipedia log data
cursor.execute("CREATE TABLE IF NOT EXISTS wikipedia_log (page_title STRING, page_id INT, page_url STRING, page_date TIMESTAMP)")

# Extract data from Wikipedia log
log = wiki.get_page("Wikipedia:Recent_changes")
for i in range(len(log.text)):
    log_data = log.text[i].split(" ")
    page_title = log_data[1]
    page_id = log_data[2]
    page_url = log_data[3]
    page_date = datetime.strptime(log_data[4], '%Y-%m-%dT%H:%M:%SZ')
    
    # Insert data into Hive Data Lake
    cursor.execute("INSERT INTO wikipedia_log VALUES ('{}', {}, '{}', '{}')".format(page_title, page_id, page_url, page_date))

conn.commit()
conn.close()


