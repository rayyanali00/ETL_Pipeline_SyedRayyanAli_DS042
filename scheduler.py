from etl_pipeline import ETLPipeline
import schedule
import time

def run_daily_etl():
    # Initialize your ETL pipeline class and run the transformation
    pipeline = ETLPipeline()  # Use your class for ETL
    df = pipeline.run_transformation()
    pipeline.load_data(df)  # Load the processed data to MongoDB or other destination

# Schedule the task to run every day at a specific time (e.g., 12:00 PM)
schedule.every().day.at("12:00").do(run_daily_etl)

while True:
    schedule.run_pending()
    time.sleep(60)  # Wait for the next scheduled task