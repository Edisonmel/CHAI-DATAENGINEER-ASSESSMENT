from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

from datetime import datetime, timedelta
import logging
import sys
import os
import json
import boto3

# Custom transformation/load functions
from include.transformation.prepare_spotify_data import transform_tracks_data
from include.load_data import load_data_to_postgres
from include.ingest_spotify_data import read_spotify_ids,fetch_tracks_data,upload_json_to_minio




# Pipeline Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("spotify_pipeline")



# Access relevant Environment variables defined Docker Compose 
dbt_project_dir = os.environ.get("DBT_PROJECT_DIR", "/opt/dbt/spotify_dbt_data_pipeline")
dbt_profiles_dir = os.environ.get("DBT_PROFILES_DIR", "/home/airflow/.dbt")



# DAG configuration constants
TARGET_ENV = Variable.get("TARGET_ENV", default_var="dev")
ADMIN_EMAIL = Variable.get("ADMIN_EMAIL")




# Default DAG arguments

default_args = {
    "owner": "INKOMOKO",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 7),
    "email": [ADMIN_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# DAG Definition

@dag(
    dag_id="airflow_dbt_spotify_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["MINIO", "AIRFLOW", "DBT", "POSTGRES"],
    doc_md=__doc__,
)
def airflow_dbt_spotify_pipeline():
     
    """
        DAG to orchestrate Spotify data ingestion, staging, and DBT transformations:
        1. Extract Spotify data and upload to MinIO.
        2. Transform data into staging tables.
        3. Create analytical models and run test using dbt
        """
    
    
    @task(task_id="ingest_spotify_data_to_minio")
    def ingest_spotify_data_to_minio():
     
        script_dir = os.path.dirname(os.path.abspath(__file__))  # directory of the script
        spotify_ids_json_path = os.path.join(script_dir, "include/spotify_ids.json")

        base_url = "https://api.spotify.com/v1"
        
        # Your Spotify bearer token
        bearer_token =  Variable.get("SPOTIFY_TOKEN")
        
        # Bucket for MinIO upload
        bucket_name = "row-data"

        try:
            track_ids = read_spotify_ids(spotify_ids_json_path)
            logger.info(f"Found {len(track_ids)} track IDs.")

            # Extract data from Spotify API
            
            # Fetch tracks data
            logger.info("Fetching tracks data from Spotify API...")
            try:
                tracks_data = fetch_tracks_data(base_url, track_ids, bearer_token)
                tracks_list = tracks_data.get("tracks", [])
                num_tracks = len(tracks_list)
                logger.info(f"Fetched {num_tracks} tracks successfully.")
            
            except Exception as e:
                logger.error(f"Failed to fetch tracks data: {e}")
                tracks_data = {}
                tracks_list = []

            # Skip DAG if no tracks data or empty list
            if not tracks_list:
                logger.warning("No tracks data available. Skipping downstream tasks.")
                raise AirflowSkipException("No tracks data available.")


            # Upload to MinIO
            logger.info("Uploading tracks data to MinIO...")
            
            try:
                tracks_object_key = upload_json_to_minio(
                    tracks_data,
                    bucket_name=bucket_name,
                    data_category="tracks"
                )

                if tracks_object_key:
                    logger.info(f"Tracks data uploaded successfully to '{tracks_object_key}'")

                else:
                    logger.warning("Tracks data upload returned empty object key.")

            except Exception as e:
                logger.error(f"Error uploading tracks data to MinIO: {e}")
                tracks_object_key = ""

        except Exception as e:
            logger.critical(f"Unexpected error while trying to ingest spotify data to Minio: {e}")
            raise

    # Task 2: prepare staging data
    @task(task_id="prepare_staging_data")
    def prepare_staging_data(bucket_name: str = "row-data"):
        """
        Reads JSON from MinIO, transforms it, and returns structured data for staging environment.

        Returns:
            tuple: (transformed_artists_data, transformed_albums_data, transformed_tracks_data)
        """
        transformed_artists_data = []
        transformed_albums_data = []
        transformed_tracks_data = []

        try:
            # Connect to MinIO (S3-compatible)
            s3_client = boto3.client(
                "s3",
                endpoint_url=f"http://{Variable.get('MINIO_ENDPOINT')}",
                aws_access_key_id=Variable.get("MINIO_ACCESS_KEY"),
                aws_secret_access_key=Variable.get("MINIO_SECRET_KEY"),
            )

            # List objects in bucket
            objects = s3_client.list_objects_v2(Bucket=bucket_name).get("Contents", [])
            if not objects:
                logger.warning(f"No objects found in bucket '{bucket_name}'.")
                return transformed_artists_data, transformed_albums_data, transformed_tracks_data

            # Process each object
            for obj in objects:
                object_key = obj["Key"]
                logger.info(f"Processing object: {object_key}")

                s3_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                raw_data = s3_obj["Body"].read().decode("utf-8")

                # Skip empty files
                if not raw_data.strip():
                    logger.warning(f"Skipped empty file: {object_key}")
                    continue

                data = json.loads(raw_data)

                # Only transform tracks files (includes albums inside transform_tracks_data)
                if "tracks" in object_key:

                    if data:  # Only transform if data is not empty
                        transformed_artists_data, transformed_albums_data, transformed_tracks_data = transform_tracks_data(
                            data, object_key
                        )
                        logger.info(
                            f"Transformed {len(transformed_artists_data)} artits, {len(transformed_tracks_data)} tracks and {len(transformed_albums_data)} albums"
                        )
                    else:
                        logger.warning(f"No data found in tracks file: {object_key}")

        except Exception as e:
            logger.error(f"Error processing Spotify data: {e}")
            raise

        return transformed_artists_data, transformed_albums_data, transformed_tracks_data
    
    
    # Task 3: Create Database and Staging Tables
    create_db_and_staging_tables = PostgresOperator(
        task_id="create_spotify_db_and_staging_tables",
        postgres_conn_id="postgres_spotify_conn",
        sql="include/create_database_schema.sql",
        autocommit=True,
    )


    # Task 4: Load Processed Data into Staging
    @task(task_id="load_processed_data_into_staging")
    def load_processed_data_into_staging(transformed_data):
        """
        Load transformed data into Postgres staging tables if lists are not empty.
        
        """
        try:
            artists_data, albums_data, tracks_data = transformed_data

            loaded = False  # Flag to check if any table got loaded

            if artists_data:
                load_data_to_postgres("stg_artists", artists_data, "artist_id")
                logger.info(f"Loaded {len(artists_data)} records into stg_artists.")
                loaded = True
            else:
                logger.warning("No artists data to load.")

            if albums_data:
                load_data_to_postgres("stg_albums", albums_data, "album_id")
                logger.info(f"Loaded {len(albums_data)} records into stg_albums.")
                loaded = True
            else:
                logger.warning("No albums data to load.")

            if tracks_data:
                load_data_to_postgres("stg_tracks", tracks_data, "track_id")
                logger.info(f"Loaded {len(tracks_data)} records into stg_tracks.")
                loaded = True
            else:
                logger.warning("No tracks data to load.")

            if not loaded:
                logger.warning("No data was loaded. All input lists are empty or None.")

        except Exception as e:
            logger.error(f"Error while loading processed data into staging: {e}")
            raise


    # Branching: Decide to Run DBT Tests
    def branch_func():
        """
        Branch logic to determine whether to run dbt test on staging data.
        Skips tests if environment is production.
        """
        if TARGET_ENV == "prod":
            return "skip_dbt_test_staging_data"
        return "dbt_test_staging_data"
    

    branch_task = BranchPythonOperator(
    task_id="branch_dbt_test_staging_data",
    python_callable=branch_func,
    )
    

    
    # Task 5: DBT Test Staging Data
    dbt_test_staging_data = BashOperator(
        task_id="dbt_test_staging_data",
        bash_command=f"dbt test --select source:* --target {TARGET_ENV}",
        cwd=f"{dbt_project_dir}",
        env={**os.environ, #inherit all container env vars
            "DBT_PROFILES_DIR": dbt_profiles_dir},
    )

    # Task 6: Dummy Task to Skip DBT Test
    skip_dbt_test_staging_data = DummyOperator(task_id="skip_dbt_test_staging_data")


    # Task 7: Run DBT Models
    modelling = BashOperator(
        task_id="modelling",
        bash_command=f"dbt deps && dbt run --select path:models --target {TARGET_ENV}",
        cwd=f"{dbt_project_dir}",
        env={**os.environ, #inherit all container env vars
            "DBT_PROFILES_DIR": dbt_profiles_dir},
        trigger_rule="one_success",  # Run if either branch succeeds
    )

    
    
    # Define Task Dependencies

    # Instantiate decorated tasks
    ingest_task = ingest_spotify_data_to_minio()
    staging_data_task = prepare_staging_data()

    # Data ingestion -> staging -> create tables -> load data -> branch
    ingest_task >> staging_data_task >> create_db_and_staging_tables
    create_db_and_staging_tables >> load_processed_data_into_staging(staging_data_task) >> branch_task

    # Branching DBT test
    branch_task >> [dbt_test_staging_data, skip_dbt_test_staging_data]

    # DBT models run after either branch
    dbt_test_staging_data >> modelling
    skip_dbt_test_staging_data >> modelling



airflow_dbt_spotify_pipeline()
