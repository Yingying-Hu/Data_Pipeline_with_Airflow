from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    load_sql = "INSERT INTO {} {}"
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 delete_flag=0,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_stmt=sql_stmt
        self.delete_flag = delete_flag

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #allows switching between insert modes when loading dimensions
        if self.delete_flag:
           self.log.info('LoadDimensionOperator: running delete function')
           delete_stmt = f'Delete FROM {self.table}'
            
        self.log.info('LoadDimensionOperator: execute sql query')
        load_dim_sql = LoadDimensionOperator.load_sql.format(self.table, self.sql_stmt)
        redshift.run(load_dim_sql)
