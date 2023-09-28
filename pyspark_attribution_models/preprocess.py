from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def preprocess_data(df: DataFrame) -> DataFrame:
    """
    Preprocess the data by converting the timestamp to a proper timestamp type.
    Also asserts that the DataFrame has the expected column names.
    
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
    df_preprocessed: DataFrame = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    
    return df_preprocessed
