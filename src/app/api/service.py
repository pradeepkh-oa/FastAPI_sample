from google.cloud import bigquery
from app.api.config_loader import AuditConfig
import logging
import json

client = bigquery.Client()
config = AuditConfig()


def get_lineage_data(domain: str):

    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("domain", "STRING", domain)
        ]
    )
    query_job = client.query(config.properties['GET_DOMAIN_LINEAGE_DATA'], job_config=job_config)
    # query_job.use_legacy_sql = False
    print(query_job)
    rows = query_job.result()
    if rows.total_rows > 0:
        df = rows.to_dataframe()
        return df.to_json(orient='records', force_ascii=False)

    return {"detail": {"status_code": "success", "message":"no data found"}}


# def get_lineage_target_table_data(target_table: str):

#     job_config = bigquery.QueryJobConfig(
#         query_parameters = [
#             bigquery.ScalarQueryParameter("target_table", "STRING", target_table)
#         ]
#     )
#     query_job = client.query(config.properties['GET_TARGET_TABLE_LINEAGE_DATA'], job_config=job_config)
#     rows = query_job.result()
#     if rows.total_rows > 0:
#         df = rows.to_dataframe()
#         return df.to_json(orient='records', force_ascii=False)
#     return json.dumps({"detail": {"status": "success", "message":"no data found"}})


def get_lineage_target_table_data(domain:str, trg_table:str):
    if domain is None and trg_table is not None:
        query = "SELECT workflow_dag_id,workflow_dag_type,src_tables,env,flow_id,domain,trg_table FROM `itg-btdpmonitor-gbl-ww-dv.btdp_monitoranalysis.t_lineage_table` where trg_table = @trg_table"
    if domain is not None and trg_table is None:
        query = "SELECT workflow_dag_id,workflow_dag_type,src_tables,env,flow_id,domain,trg_table FROM `itg-btdpmonitor-gbl-ww-dv.btdp_monitoranalysis.t_lineage_table` where domain = @domain"
    else:
        query = "SELECT workflow_dag_id,workflow_dag_type,src_tables,env,flow_id,domain,trg_table FROM `itg-btdpmonitor-gbl-ww-dv.btdp_monitoranalysis.t_lineage_table` where domain = @domain and trg_table = @trg_table"
    job_config = bigquery.QueryJobConfig(
    query_parameters=(
            bigquery.ScalarQueryParameter('domain', 'STRING', domain),
            bigquery.ScalarQueryParameter('trg_table', 'STRING', trg_table)))
    query_job = client.query(
        query,
        job_config=job_config
        )
    print(query_job)
    for row in query_job:
        print(row)
    rows = query_job.result()
    # Start the query and wait for the job to complete.
    if rows.total_rows > 0:
        df = rows.to_dataframe()
        return df.to_json(orient='records', force_ascii=False)
    return json.dumps({"detail": {"status": "success", "message":"no data found"}})


def get_all_lineage_data():

    query_job = client.query(config.properties['GET_ALL_LINEAGE_DATA'])
    rows = query_job.result()
    if rows.total_rows > 0:
        df = rows.to_dataframe()
        return df.to_json(orient='records', force_ascii=False)

    return {"detail": {"status": "success", "message":"no data found"}}


def insert_lineage_data(json_rows):
    table = client.get_table(config.properties['LINEAGE_DATA_TABLE'])
    errors = client.insert_rows_json(table, json_rows)

    if errors:
        logging.exception('Error during saving rows to the BigQuery...', errors)
        return {"detail": {"status": "error", "message":"Oops, something went wrong. Try again in a moment."}}

    return {"detail": {"status": "error", "message":"log successfully saved"}}


