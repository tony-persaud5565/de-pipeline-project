CREATE OR REPLACE WAREHOUSE ingest_wh
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;


CREATE OR REPLACE DATABASE de_pipeline_db;


CREATE OR REPLACE SCHEMA raw;

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;



CREATE OR REPLACE STAGE fakestore_stage
  URL = 's3://de-project-bucket5565/raw/'
  CREDENTIALS = (
  AWS_KEY_ID = 'xxxxxxxxxxxx'
  AWS_SECRET_KEY = 'xxxxxxxxxxxx'
)
  FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
  );



CREATE OR REPLACE TABLE raw.fakestore_products (
  id STRING,
  title STRING,
  price FLOAT,
  description STRING,
  category STRING,
  image STRING,
  rating_rate FLOAT,
  rating_count INT
);


LIST @fakestore_stage;




COPY INTO raw.fakestore_products
FROM @fakestore_stage/fakestore_products.csv
FILE_FORMAT = csv_format;



SELECT * FROM raw.fakestore_products LIMIT 10;