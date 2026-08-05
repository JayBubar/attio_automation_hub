# Attio Automation Hub

Single home for webhook receivers and automation scripts that keep Attio in
sync with the outreach channels working around it (Smartlead now,
ActiveCampaign next, others as they come online). One FastAPI service, one
Railway deployment, one place to check logs -- instead of a separate shell
service per integration.

Deliberately separate from the 990 CRM project (990 CRM -> Smartlead ->
positive reply -> Attio as Lead is its own pipeline, not this repo).

## Structure

```
automation_server/
  main.py                    FastAPI app, wires up all routers
  smartlead_routes.py        /webhooks/smartlead
  activecampaign_routes.py   /webhooks/activecampaign
  requirements.txt
scripts/
  outreach_rotation.py       Daily Smartlead capacity top-up (AC paused for now)
.env.example                 Copy to .env for local runs; real values go in Railway
```

## Local setup

```
cd automation_server
pip install -r requirements.txt
cp ../.env.example ../.env      # fill in real values
uvicorn main:app --reload --port 8000
```

## Deploying

Push to `main`. If Railway auto-deploy is enabled on this repo, that's it.
Set the variables from `.env.example` in the Railway service's Variables
tab first -- the app won't start without `ATTIO_API_KEY`.

## Adding a new integration

1. New `<name>_routes.py` in `automation_server/`, following the pattern in
   `smartlead_routes.py` -- an `APIRouter`, one POST route per webhook.
2. Import and `app.include_router(...)` it in `main.py`.
3. Add any new env vars to `.env.example`.
4. Document the route's trigger/endpoint in the team's Webhooks & API Calls
   tracker tab.

## Roadmap

A lightweight UI for viewing/managing these automations is the eventual
goal -- this structure (one backend, routes split by integration) is meant
to make that a frontend-only addition later rather than a rebuild.
