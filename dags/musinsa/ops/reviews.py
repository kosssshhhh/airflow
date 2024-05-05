from airflow.models.baseoperator import BaseOperator

class printOperator(BaseOperator):    
    def print_good():
        print("굿굿")