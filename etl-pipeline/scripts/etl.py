from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import requests
from sqlalchemy import create_engine


spark = SparkSession.builder \
    .appName("ETL Pipeline with PySpark") \
    .getOrCreate()


def extract_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()  # Assuming JSON response
        return spark.createDataFrame(data)
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None

# Example transformation step!
def transform_data(df):
    try:
       
        transformed_df = df.withColumnRenamed("id", "user_id") \
                           .withColumnRenamed("name", "user_name") \
                           .withColumn("processed_at", current_timestamp())
        return transformed_df
    except Exception as e:
        print(f"Error transforming data: {e}")
        return None

# Load Data into PostgreSQL
def load_data(df, db_url, table_name):
    try:
        engine = create_engine(db_url)
        df.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("user", "YOUR_USERNAME") \
            .option("password", "YOUR_PASSWORD") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print(f"Data successfully loaded into {table_name} table.")
    except Exception as e:
        print(f"Error loading data: {e}")

# Main ETL Pipeline
def etl_pipeline():
    API_URL = "https://jsonplaceholder.typicode.com/users"  # Example API
    DATABASE_URL = "jdbc:postgresql://YOUR_HOST/YOUR_DATABASE"
    TABLE_NAME = "users_data"

    # Extract
    print("Extracting data...")
    raw_data = extract_data(API_URL)
    if raw_data is None:
        return

    # Transform
    print("Transforming data...")
    transformed_data = transform_data(raw_data)
    if transformed_data is None:
        return

    # Load
    print("Loading data...")
    load_data(transformed_data, DATABASE_URL, TABLE_NAME)

if __name__ == "__main__":
    etl_pipeline()

