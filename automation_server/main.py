"""
main.py

Attio Automation Hub -- single FastAPI service for all webhook receivers
(Smartlead, ActiveCampaign engagement and form fills, and later Allo/others) plus a place for the
eventual automations UI to talk to. One service, one domain, one set of
logs, instead of a separate Railway shell per integration.

Run locally:    uvicorn main:app --reload --port 8000
Run on Railway: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import ipv4_only  # noqa: F401  -- must be first; see module docstring

from fastapi import FastAPI

from smartlead_routes import router as smartlead_router
from form_fill_routes import router as form_fill_router
from ops_center_routes import router as ops_center_router
from campaign_routes import router as campaign_router
from graph_mail import router as graph_mail_router
import ac_engagement_routes

app = FastAPI(title="Attio Automation Hub")

app.include_router(smartlead_router)
app.include_router(form_fill_router)
app.include_router(ops_center_router)
app.include_router(campaign_router)
app.include_router(graph_mail_router)
app.include_router(ac_engagement_routes.router)


# The AC engagement receiver buffers events in memory and judges them in a
# background sweep (see ac_engagement_routes.py). Flush on shutdown so a
# redeploy does not drop the last few seconds of clicks.
@app.on_event("startup")
def _start_ac_engagement():
    ac_engagement_routes.start_worker()


@app.on_event("shutdown")
def _stop_ac_engagement():
    ac_engagement_routes.stop_worker()


@app.get("/")
def root():
    return {"status": "ok", "service": "attio-automation-hub"}


# The Ops Center status board polls /health, so it exists as well as / --
# they return the same thing. Keeping both means neither the dashboard nor
# any existing uptime check has to change.
@app.get("/health")
def health_check():
    return {"status": "ok", "service": "attio-automation-hub"}
