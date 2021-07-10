from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 dq_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dq_checks = dq_checks
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_counts = 0
        failing_tests=[]
        
        self.log.info('DataQualityOperator: Starting to Data Quality Checks')
        for check in self.dq_checks:
            sql = check.get('sql')
            actual_result = redshift.get_records(sql)[0]
            
            if check.get('expected_result')!=actual_result[0]:
                self.log.info(actual_result)
                error_counts+=1
                failing_tests.append(sql)
        
        if error_counts > 0:
            self.log.error('tests failed')
            self.log.error(failing_tests)
            raise ValueError('Data Quality Check Failed')
        self.log.info('Data Quality Check Passed!')