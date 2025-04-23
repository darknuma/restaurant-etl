import logging
import pandas as pd
import psycopg2
from psycopg2 import Error
from typing import Dict, List
import sys
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self):
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            # database credentials
            db_user = os.getenv("POSTGRES_USER", "postgres")
            db_password = os.getenv("POSTGRES_PASSWORD", "password")
            db_host = os.getenv("POSTGRES_HOST", "localhost")
            db_port = os.getenv("POSTGRES_PORT", "5433")
            db_name = os.getenv("POSTGRES_DB", "restaurant_data")
            
            logger.info(f"Attempting to connect to PostgreSQL at {db_host}:{db_port} with user {db_user}")
            
            self.conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port
            )
            self.cur = self.conn.cursor()
            logger.info("Successfully connected to the database")
        except Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

class DataTransformer:
    def __init__(self, db: DatabaseConnection):
        self.db = db

    def load_data(self) -> Dict[str, pd.DataFrame]:
        """Load data from PostgreSQL tables into pandas DataFrames"""
        try:
            orders_df = pd.read_sql("SELECT * FROM orders", self.db.conn)
            order_items_df = pd.read_sql("SELECT * FROM order_items", self.db.conn)
            menu_items_df = pd.read_sql("SELECT * FROM menu_items", self.db.conn)
            
            logger.info("Successfully loaded data from database")
            return {
                'orders': orders_df,
                'order_items': order_items_df,
                'menu_items': menu_items_df
            }
        except Error as e:
            logger.error(f"Error loading data: {e}")
            raise

    def perform_transformations(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Perform all required transformations"""
        try:
            # Question 1: Join orders with order_items and menu_items
            joined_df = (data['orders']
                        .merge(data['order_items'], on='order_id')
                        .merge(data['menu_items'], on='item_id'))

            # Question 2: Calculate daily revenue by category
            daily_revenue = (joined_df.groupby(['order_date', 'category'])
                           .agg({'total_amount': 'sum'})
                           .reset_index())

            # Question 3: Identify top selling items
            top_items = (joined_df.groupby(['item_id', 'item_name'])
                        .agg({'quantity': 'sum'})
                        .sort_values('quantity', ascending=False)
                        .head(10)
                        .reset_index())

            # Data quality checks
            self._perform_data_quality_checks(joined_df)

            logger.info("Successfully completed all transformations")
            return {
                'joined_data': joined_df,
                'daily_revenue': daily_revenue,
                'top_items': top_items
            }
        except Exception as e:
            logger.error(f"Error during transformation: {e}")
            raise

    def _perform_data_quality_checks(self, df: pd.DataFrame):
        """Perform data quality checks"""
        # null values check
        null_counts = df.isnull().sum()
        if null_counts.any():
            logger.warning(f"Found null values in columns: {null_counts[null_counts > 0]}")

        # uniqueness check
        duplicate_orders = df['order_id'].duplicated().sum()
        if duplicate_orders > 0:
            logger.warning(f"Found {duplicate_orders} duplicate order IDs")

    def save_results(self, results: Dict[str, pd.DataFrame]):
        """Save transformed results back to database"""
        try:
            self.db.cur.execute("""
                CREATE TABLE IF NOT EXISTS daily_revenue (
                    order_date DATE,
                    category VARCHAR(50),
                    total_amount DECIMAL(10,2),
                    PRIMARY KEY (order_date, category)
                )
            """)

            self.db.cur.execute("""
                CREATE TABLE IF NOT EXISTS top_selling_items (
                    item_id VARCHAR(10),
                    item_name VARCHAR(100),
                    total_quantity INTEGER,
                    PRIMARY KEY (item_id)
                )
            """)

            # remove existing data
            self.db.cur.execute("TRUNCATE TABLE daily_revenue")
            self.db.cur.execute("TRUNCATE TABLE top_selling_items")

            #daily revenue data
            for _, row in results['daily_revenue'].iterrows():
                self.db.cur.execute(
                    "INSERT INTO daily_revenue (order_date, category, total_amount) VALUES (%s, %s, %s)",
                    (row['order_date'], row['category'], row['total_amount'])
                )

            # top selling items data
            for _, row in results['top_items'].iterrows():
                self.db.cur.execute(
                    "INSERT INTO top_selling_items (item_id, item_name, total_quantity) VALUES (%s, %s, %s)",
                    (row['item_id'], row['item_name'], row['quantity'])
                )

            self.db.conn.commit()
            logger.info("Successfully saved transformed results to database")
        except Error as e:
            logger.error(f"Error saving results: {e}")
            self.db.conn.rollback()
            raise

    def print_results(self, results: Dict[str, pd.DataFrame]):
        print("\n===== DAILY REVENUE BY CATEGORY =====")
        print(results['daily_revenue'].to_string(index=False))
        
        print("\n===== TOP 10 SELLING ITEMS =====")
        print(results['top_items'].to_string(index=False))

def main():
    db = DatabaseConnection()
    transformer = DataTransformer(db)
    
    try:
        db.connect()
        data = transformer.load_data()
        results = transformer.perform_transformations(data)
        transformer.save_results(results)
        transformer.print_results(results)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main() 