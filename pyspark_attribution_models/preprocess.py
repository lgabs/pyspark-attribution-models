from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def preprocess_data(df: DataFrame) -> DataFrame:
    """
    Preprocess the data by converting the timestamp to a proper timestamp type.
    Also asserts that the DataFrame has the expected column names.
    Adds a journey_rnk column to indicate the journey rank for each user.
    
    Parameters:
    - df: DataFrame
        Input Spark DataFrame with columns ['user_id', 'timestamp', 'utm_source', 'conversion_type']
        
    Returns:
    - DataFrame
        Preprocessed Spark DataFrame.
    """
    # Assert that DataFrame has the expected columns
    expected_cols = ['user_id', 'timestamp', 'utm_source', 'conversion_type']
    assert set(df.columns) == set(expected_cols), f"DataFrame columns should be {expected_cols}"
    
    # Convert 'timestamp' to actual timestamp type
    df_preprocessed = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    
    # Create a window partitioned by user and ordered by timestamp
    windowSpec = Window.partitionBy("user_id").orderBy("timestamp")
    
    # Create a flag column to indicate where a conversion occurs
    df_with_flag = df_preprocessed.withColumn("conversion_flag", F.when(F.col("conversion_type").isNotNull(), 1).otherwise(0))
    
    # Calculate journey_rnk based on the conversion_flag
    df_with_journey_rnk = df_with_flag.withColumn("journey_rnk", F.sum("conversion_flag").over(windowSpec))
    
    return df_with_journey_rnk
