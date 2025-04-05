ETL Pipeline for Financial Stock Data
This project implements an ETL (Extract, Transform, Load) pipeline designed to collect, transform, analyze, and load financial stock data from multiple sources such as MarketStack API, Kaggle, local CSV files, MongoDB, and GitHub into a MongoDB database. The pipeline can be scheduled to run daily and aggregates stock data for further analysis.

Features
Data Sources: Collects data from MarketStack API, Kaggle, local files, MongoDB, and GitHub.

Data Transformation: Includes data cleansing, feature engineering, and aggregation of stock data.

Data Loading: Loads the processed data into MongoDB.

Scheduling: The pipeline can be scheduled to run daily at a specific time.

Prerequisites
Before you can run the ETL pipeline, you need the following:

Python 3.x installed.

MongoDB: You need access to a MongoDB instance where the data will be loaded.

Required Libraries: Install the required libraries by running:

bash
Copy
Edit
pip install requests pandas schedule kagglehub pymongo
Usage
1. Clone the Repository
Clone this repository to your local machine:

bash
Copy
Edit
git clone https://github.com/rayyanali00/ETL_Pipeline_SyedRayyanAli_DS042
cd ETL_Pipeline_SyedRayyanAli_DS042
2. Configure the Pipeline
You can customize the pipeline by adjusting the following parameters in the ETLPipeline class:

datefrom and dateto: Set the date range for fetching stock data (default is 2025-03-01 to 2025-04-01).

IS_MOCK: Set this to True to use mock data from a GitHub gist instead of calling the MarketStack API.

You can also modify the MongoDB connection string to match your setup.

3. Running the ETL Pipeline
To run the ETL pipeline manually, simply call:

bash
Copy
Edit
python run_etl.py
This will run the pipeline, execute all steps, and load the data into MongoDB.

4. Schedule the ETL Pipeline
You can schedule the pipeline to run daily at a specified time (e.g., 12:00 PM). To schedule the task, the following schedule library is used:

python
Copy
Edit
import schedule
import time

def run_daily_etl():
    pipeline = ETLPipeline()
    df = pipeline.run_transformation()
    pipeline.load_data(df)

schedule.every().day.at("12:00").do(run_daily_etl)

while True:
    schedule.run_pending()
    time.sleep(60)
This will run the ETL task at 12:00 PM every day.

5. Inspecting Data in MongoDB
The processed data is inserted into a MongoDB collection (stocksdata_new). You can connect to your MongoDB instance to inspect the loaded data.

Functions
ETLPipeline
This class encapsulates all the logic for the ETL pipeline. Key methods include:

get_tickers(): Fetches a list of tickers from a URL.

get_stock_data_from_marketstack(): Fetches stock data from the MarketStack API.

get_data_from_kaggle_df(): Loads stock data from a Kaggle dataset.

get_data_from_local_csv(): Loads stock data from a local CSV file.

get_data_from_mongodb(): Loads stock data from MongoDB.

get_data_from_github(): Loads stock data from a GitHub repository.

check_for_null_values(): Checks for missing values in the datasets.

handle_missing_values(): Handles missing values and adds the capital gains feature.

normalize_column_names(): Normalizes column names (lowercase, underscores).

validate_data(): Validates the data by dropping rows with invalid (negative) financial values.

add_features(): Adds new features like daily return and volatility.

aggregate_data(): Aggregates data by symbol and date.

merge_datasets(): Merges multiple datasets into one consolidated DataFrame.

run_transformation(): Runs the full ETL process: Load → Transform → Aggregate → Merge.

load_data(): Loads the final processed data into MongoDB.

Example Output
After running the pipeline, the processed data will be available in MongoDB in the stocksdata_new collection. The data will include:

symbol: Stock ticker symbol.

date_only: The date of the stock data.

open, close, high, low, volume: Stock price information.

daily_return: Daily return calculated as (close - open) / open.

volatility: Volatility calculated as (high - low).

Notes
The pipeline handles various data sources and can easily be extended to include additional sources.

It assumes the use of MongoDB, but the final dataset can be saved to other destinations if needed.

The pipeline can handle missing values, normalize column names, and add additional financial features for analysis.
