import pandas as pd
import sqlite3

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
    # Extract
    raw_data = extract_data("sales_data.csv")
    
    # Transform
    cleaned_data = transform_data(raw_data)
    print("Transformed Data Sample:")
    print(cleaned_data.head())
    
    # Load
    load_data(cleaned_data, "sales_database.db", "sales")