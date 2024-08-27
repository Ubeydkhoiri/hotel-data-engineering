import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from transformations import transform_fact_table, transform_dim_tables  # Import the functions

# Load environment variables from .env
load_dotenv()

# Logging Configuration
LOG_DIR = './logs/'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, f'ETL_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__)

# Data Configuration
STAGING_AREA_PATH = './staging-area/'

def get_data_loaded_path(filename):
    return os.path.join(STAGING_AREA_PATH, f"{filename}_loaded.csv")

def get_data_transformed_path(filename):
    return os.path.join(STAGING_AREA_PATH, f"{filename}_transformed.csv")

def extractor():
    logger.info("Starting data extraction process.")
    
    oltp_databases = {
        'promotion_db': [
            ('campaigns', "SELECT * FROM Campaign"),
            ('vouchers', "SELECT * FROM Voucher")
        ],
        'payment_db': [
            ('payment_third_parties', "SELECT * FROM PaymentThirdParties"),
            ('payment_methods', "SELECT * FROM PaymentMethods"),
            ('payments', "SELECT * FROM Payments")
        ],
        'reservation_db': [
            ('users', "SELECT * FROM Users"),
            ('hotels', "SELECT * FROM Hotels"),
            ('reservations', "SELECT * FROM Reservations"),
            ('reservation_items', "SELECT * FROM ReservationItems")
        ],
        'stay_db': [
            ('stay_users', "SELECT * FROM Users"),
            ('stay_hotels', "SELECT * FROM Hotels"),
            ('rooms', "SELECT * FROM Rooms"),
            ('stays', "SELECT * FROM Stays")
        ]
    }

    file_paths = []

    for db_name, tables in oltp_databases.items():
        try:
            logger.info(f"Connecting to database: {db_name}")
            engine = create_engine(
                f"mysql://{os.getenv('OLTP_USER')}:{os.getenv('OLTP_PASSWORD')}@{os.getenv('OLTP_HOST')}:3306/{db_name}"
            )
            for table_name, query in tables:
                try:
                    logger.info(f"Querying table: {table_name} in {db_name}")
                    df = pd.read_sql(query, engine)
                    file_path = get_data_loaded_path(table_name)
                    df.to_csv(file_path, index=False)
                    file_paths.append(file_path)
                    logger.info(f"Successfully extracted and saved {table_name} to {file_path}")
                except Exception as e:
                    logger.error(f"Error querying {table_name} in {db_name}: {e}")
            engine.dispose()
        except Exception as e:
            logger.error(f"Error connecting to database {db_name}: {e}")

    return file_paths

def transformer(file_paths):
    logger.info("Starting data transformation process.")
    
    data = {}
    for file_path in file_paths:
        table_name = os.path.basename(file_path).replace('_loaded.csv', '')
        try:
            df = pd.read_csv(file_path)
            data[table_name] = df
            logger.info(f"Loaded data for table: {table_name}")
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")

    fact_table = transform_fact_table(data)
    dim_tables = transform_dim_tables(data)

    fact_table.to_csv(get_data_transformed_path('fact_table'), index=False)
    logger.info(f"Fact table saved to {get_data_transformed_path('fact_table')}")
    
    for table_name, df in dim_tables.items():
        df.to_csv(get_data_transformed_path(f'dim_{table_name}'), index=False)
        logger.info(f"Dimension table {table_name} saved to {get_data_transformed_path(f'dim_{table_name}')}")
    
    for file_path in file_paths:
        os.remove(file_path)
        logger.info(f"Removed raw data file: {file_path}")

    transformed_files = [get_data_transformed_path('fact_table')] + [
        get_data_transformed_path(f'dim_{table_name}') for table_name in dim_tables
    ]

    logger.info("Data transformation process complete.")
    return transformed_files

def loader(file_paths_transformed):
    logger.info("Starting data loading process.")

    engine = create_engine(
                f"mysql://{os.getenv('DATA_WAREHOUSE_USER')}:{os.getenv('DATA_WAREHOUSE_PASSWORD')}@{os.getenv('DATA_WAREHOUSE_HOST')}:3306/{os.getenv('DATA_WAREHOUSE_DB')}"
            )

    with engine.connect() as conn:
        for file_path in file_paths_transformed:
            table_name = os.path.basename(file_path).replace('_transformed.csv', '')

            try:
                df = pd.read_csv(file_path)

                if table_name == 'fact_table':
                    df.to_sql(
                        'mst_reservation', conn, if_exists='replace', index=False, 
                        method='multi', chunksize=1000
                    )
                    logger.info(f"Loaded fact table into mst_reservation from {file_path}")

                elif table_name.startswith('dim_'):
                    dimension_table = table_name.replace('dim_', '')
                    df.to_sql(
                        dimension_table, conn, if_exists='replace', index=False,
                        method='multi', chunksize=1000
                    )
                    logger.info(f"Loaded dimension table {dimension_table} from {file_path}")

            except Exception as e:
                logger.error(f"Error loading data into table {table_name}: {e}")

    for file_path in file_paths_transformed:
        os.remove(file_path)
        logger.info(f"Removed transformed data file: {file_path}")

    logger.info("Data loading process complete.")
    return 'Loading successful.'

if __name__ == "__main__":
    logger.info("ETL process started.")
    try:
        filepath = extractor()
        transformed_files = transformer(filepath)
        loader(transformed_files)
        logger.info("ETL process completed successfully.")
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
