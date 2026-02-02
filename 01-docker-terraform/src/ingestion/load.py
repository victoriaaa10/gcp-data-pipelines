import pandas as pd
from sqlalchemy import create_engine
import logging

# Set up basic logging to track the load process
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_engine(user, password, host, port, db):
    """
    creates a SQLAlchemy engine for PostgreSQL
    """
    conn_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_url)

def init_table(df: pd.DataFrame, table_name: str, engine):
    """
    creates table structure in Postgres.
    """
    try:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        logger.info(f"✅ Table '{table_name}' initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize table '{table_name}': {e}")
        raise

def append_data(df: pd.DataFrame, table_name: str, engine):
    """
    appends a DataFrame (or a chunk/batch) to the existing Postgres table.
    """
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        return True
    except Exception as e:
        logger.error(f"Failed to append data to '{table_name}': {e}")
        raise