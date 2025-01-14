# atbd_trabajo_grupal
### Arquitectura de la solución
Hemos mantenido la arquitectura que desplegamos anteriormente para el cluster de Hadoop, y hemos añadido un instancia más para ejecutar airflow en ella.
![Alt text](img/Arki-airflow-yarn.png)
### Airflow como orquestador
Hemos usado airflow para orquestar la ejecución de los procesos de consulta spark a través de yarn. Para ello generamos DAGs que hemos agrupado bajo la etiqueta de atb_consultas
![Alt text](img/airflow_dag.png)

#### Configuraciones de Airflow
Para solventar problemas de dependencias y configuración generamos un zip con todos los jars necesarios para ejecutar todos nuestros scripts de consultas y se lo pasamos al dag como parte del conf, ademas de expecificar donde se encuentran los xml necesarios para una conexión correcta con el api de yarn.
```python
        conf={
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
            'spark.yarn.archive': 'hdfs://172.31.20.226:9000/user/ec2-user/spark-hadoop-libs.zip',
        },
        env_vars={
            'HADOOP_CONF_DIR': '/etc/hadoop/conf',
            'YARN_CONF_DIR': '/etc/hadoop/conf',
        },
```
