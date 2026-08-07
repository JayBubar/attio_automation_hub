"""
main.py

Attio Automation Hub -- single FastAPI service for all webhook receivers
(Smartlead, ActiveCampaign, and later Allo/others) plus a place for the
eventual automations UI to talk to. One service, one domain, one set of
logs, instead of a separate Railway shell per integration.

Run locally:    uvicorn main:app --reload --port 8000
Run on Railway: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

from fastapi import FastAPI

from smartlead_routes import router as smartlead_router
from activecampaign_routes import router as activecampaign_router
from ops_center_routes import router as ops_center_router
from graph_mail import router as graph_mail_router

app = FastAPI(title="Attio Automation Hub")

app.include_router(smartlead_router)
app.include_router(activecampaign_router)
app.include_router(ops_center_router)
app.include_router(graph_mail_router)


@app.get("/")
def root():
    return {"status": "ok", "service": "attio-automation-hub"}


# The Ops Center status board polls /health, so it exists as well as / --
# they return the same thing. Keeping both means neither the dashboard nor
# any existing uptime check has to change.
@app.get("/health")
def health_check():
    return {"status": "ok", "service": "attio-automation-hub"}
