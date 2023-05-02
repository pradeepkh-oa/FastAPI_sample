from fastapi import APIRouter, Request, HTTPException
from app.api import service
from app.api.models import Log
import ujson as json
import logging
from typing import List, Union
from fastapi.encoders import jsonable_encoder

import base64
from IPython.display import Image, display
import matplotlib.pyplot as plt

router = APIRouter()


# @router.get("/v1/lineage", status_code=200)
# def get_all_lineage_data():
#     try:
#         response = service.get_all_lineage_data()
#     except Exception as exp:
#         logging.exception("unhandled error in get_all_lineage_data")
#         raise HTTPException(status_code=500,
#                             detail={"status": "error", "message": "Oops, something went wrong. Try again in a moment."})
#     return json.loads(response)


# @router.get("/v1/lineage/domain/{domain}", status_code=200)
# def get_lineage_data(domain: str):
#     try:
#         response = service.get_lineage_data(domain)
#     except Exception as exp:
#         logging.exception("unhandled error in get_lineage_data")
#         raise HTTPException(status_code=500,
#                             detail={"status": "error", "message": "Oops, something went wrong. Try again in a moment."})
#     return json.loads(response)


def mm(graph, data):
    graphbytes = graph.encode("ascii")
    base64_bytes = base64.b64encode(graphbytes)
    base64_string = base64_bytes.decode("ascii")
    display(Image(url="https://mermaid.ink/img/" + base64_string))



@router.get("/v1/lineage/", tags=["Operational Data Lineage"],description="Returns operational data lineage for given value.", status_code=200)
def get_lineage_target_table_data(domain: Union[str, None] = None,trg_table: Union[str, None] = None):
    try:
        response = service.get_lineage_target_table_data(domain,trg_table)
    except Exception as exp:
        logging.exception("unhandled error in get_lineage_data")
        raise HTTPException(status_code=500,
                            detail={"status": "error", "message": "Oops, something went wrong. Try again in a moment."})
    res = json.loads(response)
    # for i in res: 
    #     A='Flowid:' + i['flow_id']
    #     B='Bq-Error:Error-tag'
    #     # C='SourceBucket:NA'
    #     C='WorkFlow:'+ i['workflow_dag_id']
    #     D='Flow-step:801_sql_transfo_rangeunika'
    #     E='TargetTables:'+ i['trg_table']
    #     Gra="graph LR;{A1}--> {B1} & {C1};{B1}--> {D1} & {E1};{C1}".format(A1=A,B1=B,C1=C,D1=D,E1=E)
    #     # mm(Gra,i)                   
    return res  



# @router.post("/v1/lineage", status_code=201)
# async def insert_lineage_data(log: List[Log]):
#     try:
#         response = service.insert_lineage_data(jsonable_encoder(log))
#     except Exception as exp:
#         logging.exception("unhandled error in insert_lineage_data")
#         raise HTTPException(status_code=500,
#                             detail={"status": "error", "message": "Oops, something went wrong. Try again in a moment."})
#     return response

