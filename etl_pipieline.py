import pandas as pd
import sqlite3
import os

# ========== CREATE SAMPLE DATA ========== #
def create_sample_data(file_path):
    """Create sample CSV if it doesn't exist"""
    if not os.path.exists(file_path):
        data = {
            'order_id': [1001, 1002, 1003, 1004, 1005],
            'customer_id': ['C101', 'C102', 'C103', 'C101', 'C102'],
            'product': ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard'],
            'quantity': [2, 1, 3, 1, 2],
            'price': [1200, 800, 400, 300, 50],
            'order_date': ['2023-01-15', '2023-01-16', '2023-01-17', '2023-01-18', '2023-01-19']
        }
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        print(f"Created sample data file: {file_path}")

# ========== EXTRACT ========== #
def extract_data(file_path):
    """Read data from CSV"""
    return pd.read_csv(file_path)

# ========== TRANSFORM ========== #
def transform_data(df):
    """Clean and transform data"""
    # Convert date to datetime
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    # Calculate total sales
    df['total_sales'] = df['quantity'] * df['price']
    
    # Filter valid records (price > 0)
    df = df[df['price'] > 0]
    
    return df

# ========== LOAD ========== #
def load_data(df, db_name, table_name):
    """Load data into SQLite database"""
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data loaded to {table_name} in {db_name}")

# ========== MAIN ETL PIPELINE ========== #
if __name__ == "__main__":
    # Create sample data if it doesn't exist
    create_sample_data("sales_data.csv")
    
    # Extract
    raw_data = extract_data("sales_data.csv")
    
    # Transform
    cleaned_data = transform_data(raw_data)
    print("Transformed Data Sample:")
    print(cleaned_data.head())
    
    # Load
    load_data(cleaned_data, "sales_database.db", "sales")
