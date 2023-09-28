from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def last_touch_attribution(df: DataFrame, conversion_type: str) -> DataFrame:
    """
    Calculate Last-Touch Attribution for a specific conversion type.
    
    Parameters:
    - df: DataFrame
        Preprocessed Spark DataFrame with columns ['user_id', 'timestamp', 'utm_source', 'conversion_type']
        
    - conversion_type: str
        The type of conversion to focus on ('sales', 'lead_generation', 'profile_generation')
        
    Returns:
    - DataFrame
        Spark DataFrame containing attribution counts for each channel.
    """
    # Filter based on the conversion_type
    df_filtered: DataFrame = df.filter(F.col("conversion_type") == conversion_type)
    
    # Get the last touch point for each user
    df_last_touch: DataFrame = df_filtered \
        .groupBy("user_id") \
        .agg(F.max("timestamp").alias("timestamp"))
    
    # Join back to get the utm_source for the last touch point
    df_attribution: DataFrame = df_last_touch \
        .join(df_filtered, ["user_id", "timestamp"]) \
        .select("user_id", "utm_source")
    
    # Calculate attribution for each channel
    attribution_df: DataFrame = df_attribution \
        .groupBy("utm_source") \
        .agg(F.count("user_id").alias("attribution_count")) \
        .sort("attribution_count", ascending=False)
    
    return attribution_df
