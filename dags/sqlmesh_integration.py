from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

sqlmesh_airflow = SQLMeshAirflow(
    engine_operator="bigquery", 
    default_catalog="sqlmesh-public-demo",
    engine_operator_args={
        "bigquery_conn_id": "airflow_bigquery_conn"
    },
)

for dag in sqlmesh_airflow.dags:
    globals()[dag.dag_id] = dag