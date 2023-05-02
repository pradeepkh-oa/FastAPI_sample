from pydantic import BaseModel
from typing import List


class Action(BaseModel):
    type: str
    sub_type: str = None


class Log(BaseModel):
    workflow_dag_id:str
    workflow_dag_type:str
    src_tables:List[str]
    env:str
    flow_id:str
    domain:str
    trg_table:str
