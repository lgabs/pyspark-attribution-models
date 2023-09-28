from pyspark.sql import DataFrame, SparkSession
from pyspark_attribution_models.preprocess import preprocess_data
from pyspark_attribution_models.attribution_models import last_touch_attribution

def run_attribution(df: DataFrame, conversion_type: str) -> DataFrame:
    """
    Run the marketing attribution process on the given DataFrame.
    
    Parameters:
    - df: DataFrame
        Spark DataFrame with columns ['user_id', 'timestamp', 'utm_source', 'conversion_type']
        
    - conversion_type: str
        The type of conversion to focus on ('sales', 'lead_generation', 'profile_generation')
        
    Returns:
    - DataFrame
        Spark DataFrame containing attribution counts for each channel.
    """
    # Preprocess data
    df_preprocessed: DataFrame = preprocess_data(df)
    
    # Calculate attribution
    attribution_df: DataFrame = last_touch_attribution(df_preprocessed, conversion_type)
    
    return attribution_df

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Marketing Attribution Model") \
        .getOrCreate()
    
    # Sample DataFrame for testing
    df = spark.createDataFrame([
        ("user1", "2023-01-01 11:01:00", "google", None),
        ("user1", "2023-01-01 12:01:00", "google", "sales"),
        ("user1", "2023-01-01 12:05:00", "facebook", "lead_generation"),
        ("user2", "2023-01-01 12:10:00", "linkedin", "profile_generation"),
        ("user2", "2023-01-01 12:15:00", "google", "sales"),
    ], ["user_id", "timestamp", "utm_source", "conversion_type"])
    
    # Run attribution for 'sales'
    result_df = run_attribution(df, "sales")
    result_df.show()
