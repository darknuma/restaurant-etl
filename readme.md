# Restaurant Order Data Pipeline

This traditional end-to-end data pipeline for processing restaurant order data, performing transformations, and loading results into a PostgreSQL database.

## Setup Instructions

### Requirements

- Python
- PostgreSQL 16 (installed on your PC)

### Installation Steps

1. Clone the repository:

```bash
git clone https://github.com/darknuma/restaurant-etl.git
cd restaurant-etl
```

2. Run the Program

```bash
(activate python environment)
pip install -r requirements.txt
python transform.py
```

## Assumptions Made

1. Data Quality:
   - Order IDs are unique and not null
   - Item IDs in order_items correspond to existing menu_items
   - Dates are in a valid format

2. Business Rules:
   - Revenue is calculated based on order total_amount
   - Top selling items are determined by quantity sold

3. Technical:
   - PostgreSQL is running on localhost:5433
   - Python environment is utilised

## Approach Explanation

The pipeline follows a traditional ETL (Extract, Transform, Load) pattern:

1. Data Ingestion:
   - Raw CSV files are loaded into PostgreSQL staging tables
   - In Menu_items, created a serial key ID, as the primary key
   - Initial data validation and type checking
   - Error handling for data loading issues

2. Data Transformation:
   - Joins orders with order_items and menu_items
   - Calculates daily revenue by category
   - Identifies top selling items
   - Performs data quality checks
   - Implements error handling and logging

#### Daily Revenue by Category

![Daily Revenue by Category](assets/daily%20revenue%20by%20category.png)

#### Top Selling Items

![Top Selling Items](assets/top%20selling.png)

3. Data Loading:
   - Creates new tables for transformed data
   - Loads results with proper indexing
   - Maintains data integrity with constraints

## Further Improvements

- Set up for Incremental loading, where I could have done it by using timestamp
- Include test for incremental model 