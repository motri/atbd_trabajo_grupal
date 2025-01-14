# atbd_trabajo_grupal
#### Arquitectura de la solución
Hemos mantenido la arquitectura que desplegamos anteriormente para el cluster de Hadoop, y hemos añadido un instancia más para ejecutar airflow en ella.
![Alt text](img/Arki-airflow-yarn.png)
#### Airflow como orquestador
Hemos usado airflow para orquestar la ejecución de los procesos de consulta spark a través de yarn. Para ello generamos DAGs que hemos agrupado bajo la etiqueta de atb_consultas
![Alt text](img/airflow_dag.png)
