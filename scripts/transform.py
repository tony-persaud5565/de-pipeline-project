# scripts/transform.py
import pandas as pd
import snowflake.connector
from sqlalchemy import create_engine
import dotenv
import os

# Load environment variables
dotenv.load_dotenv()

user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = os.getenv("SNOWFLAKE_DATABASE")
schema_raw = os.getenv("SNOWFLAKE_SCHEMA_RAW")
schema_clean = os.getenv("SNOWFLAKE_SCHEMA_CLEAN")


# Pull raw data
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema_raw
)
cursor = conn.cursor()
cursor.execute("SELECT * FROM fakestore_products")
rows = cursor.fetchall()
cols = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=cols)

# Clean it
df.columns = [c.lower() for c in df.columns]
df = df.rename(columns={"id": "product_id", "title": "title", "price": "price"})
df = df[["product_id", "title", "category", "price"]]  
df.dropna(inplace=True)

# Upload to Snowflake
engine = create_engine(
    f'snowflake://{user}:{password}@{account}/{database}/{schema_clean}?warehouse={warehouse}'
)

df.to_sql('products_cleaned', con=engine, index=False, if_exists='replace', method='multi')

print("Transformation complete and uploaded to Snowflake")
