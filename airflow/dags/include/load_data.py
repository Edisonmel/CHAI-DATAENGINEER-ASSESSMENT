from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from psycopg2.extras import execute_values
import logging


# Sample Python callable to load bulk data with upsert
def load_data_to_postgres(table_name, data,pk_column,postgres_conn_id='postgres_spotify_conn'):
    """
    Perform bulk upsert to a Postgres table using ON CONFLICT.
    Automatically updates all columns except the primary key.
    """

    try:

        if not data:
            return

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()


        # Get columns dynamically from first row
        columns = list(data[0].keys())

        # Build query
        insert_query = f"""
        INSERT INTO staging.{table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({pk_column}) DO UPDATE SET
        {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != pk_column])};
        """

        # Prepare data as list of tuples
        values = [tuple(row[col] for col in columns) for row in data]

        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
            conn.commit()
    
    except Exception as e:
        logging.error(f"Error Occured When trying to load the data: {e}")
        raise
        