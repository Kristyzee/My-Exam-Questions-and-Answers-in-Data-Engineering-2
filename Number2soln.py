import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
import logging


# QUESTION2---
# 2. using sqlalchemy:
# write a function to extract information from two tables in the given URL and
# perform transformations on the data such as converting columns where
# necessary and joining the two tables
# where 'Average rank'(on the first table) = 'Number'(on the second table)
# load the resulting dataframe to a postgresql database using the table name.
# From the first table, extract the first three columns. Extract the 'Genre'
# from the third table and add a column 'Number'
# which should auto-increment.
# Join the two tables where 'Average rank' = 'Number'
# Save the resulting table in the database as well as on a '.csv' file
# Only the first 17 rows from the two tables are required.
# The URL to be used is
# 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
# Each stage of the process should be logged in a '.txt' file
# *Note the this code is to be run in a virtual environment***


# --------------------------------------------SOLUTION------------------------------------------------

# Set up logging
logging.basicConfig(filename='Highlyrankedfilm_process_log.txt',
                    level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# URL to scrape
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'

# Fetch the webpage
logging.info('Fetching the webpage')
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Extract tables
logging.info('Extracting tables from the webpage')
tables = pd.read_html(str(soup))

# Print the number of tables extracted
logging.info(f'Number of tables extracted: {len(tables)}')

# Check if there are at least 3 tables
if len(tables) < 3:
    logging.error("Less than 3 tables found on the webpage.Exiting the process")
    exit()

# Extract the first 17 rows from the first table and focus only on the
# necessary columns
logging.info('Extracting relevant columns and first 17 rows from the first table')
# Extracting 'Average Rank', 'Movie Title', and 'Year'
table1 = tables[0].iloc[:17, [0, 1, 2]].copy()
logging.info(f"Table 1 (first 17 rows): {table1.shape[0]} rows, {table1.shape[1]} columns")

# Extract the necessary columns from the second table (including 'Genre')
logging.info('Extracting relevant columns and first 17 rows from the second table')
table2 = tables[2].iloc[:17, [1]].copy()  # Extracting 'Genre' from the second table
logging.info(f"Table 2 (first 17 rows): {table2.shape[0]} rows, {table2.shape[1]} columns")

# Adding 'Number' column as auto-incrementing number starting from 1
table1['Number'] = range(1, len(table1) + 1)
table2['Number'] = range(1, len(table2) + 1)


# Merge the two tables on the 'Number' column (inner join by default)
logging.info("Merging the two tables on the 'Number' column")
merged_df = pd.merge(table1, table2, on='Number', how='inner')

# Drop the 'Number' column since you don't want it to appear
# in the final result
merged_df = merged_df.drop(columns=['Number'])

# Print the merged dataframe's shape for debugging
logging.info(f"Merged dataframe shape: {merged_df.shape[0]} rows,{merged_df.shape[1]} columns")

# Save the resulting dataframe to a CSV file
logging.info('Saving the resulting dataframe to a CSV file')
merged_df.to_csv('merged_tables.csv', index=False)

# Database connection
logging.info('Connecting to the PostgreSQL database')
postgres_user = 'postgres'
postgres_password ='Ezinne'  # my password isn't opensource....kindly input yours to make it run
postgres_host = 'localhost'
postgres_port = '5432'
postgres_db = 'Films_db'
db_url = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'

engine = create_engine(db_url)

if not database_exists(db_url):
    logging.info('Database does not exist, creating a new one')
    create_database(db_url)

metadata = MetaData()

# Define the table schema based on the merged dataframe columns
logging.info('Defining the table schema')
resulting_table = Table('resulting_table', metadata,
                        Column('Movie Title', String),
                        Column('Average Rank', String),
                        Column('Year', String),
                        Column('Genre', String),
                        Column('Number', Integer, primary_key=True))

# Create the table in the database
logging.info('Creating the table in the database')
metadata.create_all(engine)

# Load the dataframe into the database
logging.info('Loading the dataframe into the database')
Session = sessionmaker(bind=engine)
session = Session()
merged_df.to_sql('resulting_table', con=engine, if_exists='replace', index=False)

logging.info('Process completed successfully')
print(f"Data processed and saved to '{postgres_db}' database in PostgreSQL!")
