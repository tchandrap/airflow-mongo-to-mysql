from airflow.hooks.base import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class MongoToMySqlOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 mongo_collection,
                 mongo_database,
                 mongo_query,
                 target_table=None,
                 identifier=None,
                 mongo_conn_id='mongo_default',
                 mysql_conn_id='mysql_default', 
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)
        # Conn Ids
        self.mongo_conn_id = mongo_conn_id
        self.s3_conn_id = s3_conn_id
        # Mongo Query Settings
        self.mongo_db = mongo_database
        self.mongo_collection = mongo_collection
        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_query = mongo_query
        self.is_pipeline = True if isinstance(self.mongo_query, list) else False

        self.mysql_conn_id = mysql_conn_id

    def execute(self, context):
        
        start_date=context['data_interval_start'].strftime('%Y-%m-%d %H:%M:%S')
        end_date=context['data_interval_end'].strftime('%Y-%m-%d %H:%M:%S')
        
        self.sql = self.sql.format(start_date=start_date, end_date=end_date)
        
        mongo_conn = MongoHook(self.mongo_conn_id).get_conn()
        mysql_conn = MySqlHook(self.mysql_conn_id)

        # Grab collection and execute query according to whether or not it is a pipeline
        collection = mongo_conn.get_database(self.mongo_db).get_collection(self.mongo_collection)
        results = collection.find(self.mongo_query)
        
        mysql_client = mysql_conn.get_conn()
        
        mysql_client.insert_rows(self.target_table,rows=results)
        
        mysql_client.close()
        mongo_conn.close()