from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id='lanza_consulta_inversiones_publicas_en_i_mas_d',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  
    catchup=False,
    tags=['atbd_consultas'],
) as dag:

    # Spark submit task
    spark_submit_task = SparkSubmitOperator(
        task_id='ejecuta_consulta',
        application='hdfs://172.31.20.226:9000/user/ec2-user/scripts/total_award_vs_rnd_multitype.py',
        conn_id='spark_default', 
        name='consulta_inversiones_publicas_en_i_mas_d',
        verbose=True,
        conf={
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
            'spark.yarn.archive': 'hdfs://172.31.20.226:9000/user/ec2-user/spark-hadoop-libs.zip',
        },
        env_vars={
            'HADOOP_CONF_DIR': '/etc/hadoop/conf',
            'YARN_CONF_DIR': '/etc/hadoop/conf',
        },
    )

    spark_submit_task
