import unittest
from pyspark.sql import SparkSession
from pyspark_attribution_models.preprocess import preprocess_data
from pyspark_attribution_models.attribution_models import last_touch_attribution
from pyspark_attribution_models.main import run_attribution

class TestAttributionModels(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test Attribution Models") \
            .getOrCreate()
        
        cls.sample_data = cls.spark.createDataFrame([
            ("user1", "2023-01-01 12:01:00", "google", "sales"),
            ("user1", "2023-01-01 12:05:00", "facebook", "lead_generation"),
            ("user2", "2023-01-01 12:10:00", "linkedin", "profile_generation"),
            ("user2", "2023-01-01 12:15:00", "google", "sales"),
        ], ["user_id", "timestamp", "utm_source", "conversion_type"])
        
    def test_preprocess_data(self):
        df = preprocess_data(self.sample_data)
        self.assertEqual(str(df.schema["timestamp"].dataType), "TimestampType()")
        
    def test_last_touch_attribution(self):
        df = preprocess_data(self.sample_data)
        result = last_touch_attribution(df, "sales")
        self.assertEqual(result.count(), 1)
        
    def test_run_attribution(self):
        result = run_attribution(self.sample_data, "sales")
        self.assertEqual(result.count(), 1)
        
if __name__ == "__main__":
    unittest.main()
