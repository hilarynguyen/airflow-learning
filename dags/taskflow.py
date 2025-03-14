from airflow.decorators import dag, task
from datetime import datetime
import random

@dag(
    start_date = datetime(2024, 12, 26),
    schedule_interval = "@daily",
    catchup = False,
    tags = ['taskflow']
)

def taskflow():

    @task 
    def task_a():
        return random.randint(1, 100)
    @task
    def task_b(num):
        print("Task B")
        if num % 2 == 0:
            print(f"{num} là số chẵn")
        else:
            print(f"{num} là số lẻ")
#The name of the Python function corresponds to the name of the task that you will see on the airflow
    task_b(task_a())

taskflow()