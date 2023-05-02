from fastapi import FastAPI
from app.api import audit


app = FastAPI(title="BTDP Monitoring APIs",
    description= "",
    version="1.0.0",)

app.include_router(audit.router)



# import os

# from application import make_app  # application definition

# # In pytest, we use a test app instead

# is_local_runtime = (__name__ == "__main__") or os.environ.get("UVICORN_DEBUG")

# if os.environ.get("TEST_ENV", "0") != "1":  # pragma: no cover
#     app = make_app(is_local_runtime=is_local_runtime)