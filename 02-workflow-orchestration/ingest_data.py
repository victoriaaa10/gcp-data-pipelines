import os
import click
import logging
from dotenv import load_dotenv
from src.ingestion import extract, load

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

@click.command()
@click.option('--taxi_path', required=True, help='Path to the taxi parquet file')
@click.option('--zones_path', required=True, help='Path to the zones csv file')
@click.option('--table_name', default='green_taxi_trips', help='Destination table name')
@click.option('--batch_size', default=100000, help='Number of rows per batch')
def main(taxi_path, zones_path, table_name, batch_size):
    """
    Ingestion Script
    """
    load_dotenv()
    
    # database Configuration from .env
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DB')
    # 'localhost' for local runs, 'pgdatabase' for Docker network
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5433')

    # setup DB engine
    engine = load.get_engine(user, password, host, port, db)

    # handle zones (dimension table)
    logging.info("Loading Taxi Zones...")
    df_zones = extract.load_csv(zones_path)
    load.init_table(df_zones, "zones", engine)
    load.append_data(df_zones, "zones", engine)

    # initialize taxi table schema
    logging.info(f"🚕 Initializing table schema for {table_name}...")
    schema_df = extract.get_parquet_schema(taxi_path)
    load.init_table(schema_df, table_name, engine)

    # stream and transform batches
    logging.info(f"Starting batch ingestion (size: {batch_size})...")
    
    batches = extract.get_parquet_batches(taxi_path, chunk_size=batch_size)
    
    for i, batch in enumerate(batches):
        # apply type enforcement 
        cols_to_fix = ['VendorID', 'PULocationID', 'DOLocationID', 'passenger_count', 'payment_type']
        for col in cols_to_fix:
            if col in batch.columns:
                batch[col] = batch[col].astype('Int64')
        
        # load the batch
        load.append_data(batch, table_name, engine)
        logging.info(f"Batch {i+1} ingested.")

    logging.info(f"Successfully ingested data into {table_name}.")

if __name__ == '__main__':
    main()