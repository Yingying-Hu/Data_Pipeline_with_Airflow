from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)

from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past' : False,
    'retries': 3,
    'retry_delay' : timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}


with DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')
    
    create_table_operator = PostgresOperator(
        task_id='Create_table',
        postgres_conn_id='redshift',
        sql='sql/create_tables.sql'
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='udacity-dend',
        s3_key='log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json'
        region='us-west-2',
        jsonpath='s3://udacity-dend/log_json_path.json'
        
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='udacity-dend',
        s3_key='song_data/{execution_date.year}/{execution_date.month}/{ds}-events.json'
        region='us-west-2'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        select_sql_stmt=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql_stmt=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql_stmt=SqlQueries.song_table_insert 
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql_stmt=SqlQueries.artist_table_insert 
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql_stmt=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        dq_checks=[{'sql':SqlQueries.songplay_table_check, 'expected_result':0},
                {'sql':SqlQueries.user_table_check, 'expected_result':0},
                {'sql':SqlQueries.time_table_check, 'expected_result':0},
                {'sql':SqlQueries.artist_table_check, 'expected_result':0},
                {'sql':SqlQueries.song_table_check, 'expected_result':0}
           ]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator>>create_table_operator
    
    create_table_operator>>stage_events_to_redshift
    create_table_operator>>stage_songs_to_redshift

    stage_events_to_redshift>>load_songplays_table
    stage_songs_to_redshift>>load_songplays_table

    load_songplays_table>>load_user_dimension_table
    load_songplays_table>>load_song_dimension_table
    load_songplays_table>>load_artist_dimension_table
    load_songplays_table>>load_time_dimension_table

    load_user_dimension_table>>run_quality_checks
    load_song_dimension_table>>run_quality_checks
    load_artist_dimension_table>>run_quality_checks
    load_time_dimension_table>>run_quality_checks

    run_quality_checks>>end_operator
