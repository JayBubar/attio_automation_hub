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
automation_server/             <- Railway Root Directory points HERE
  main.py                      FastAPI app, wires up all routers
  smartlead_routes.py          /webhooks/smartlead
  activecampaign_routes.py     /webhooks/activecampaign
  ops_center_routes.py         /trigger/*, /status/*, /tasks/* for the Ops Center
  graph_mail.py                /auth/microsoft/*, Outlook draft creation
  requirements.txt
  scripts/
    outreach_rotation.py       Daily Smartlead capacity top-up (AC paused for now)
    outreach.py                Per-rep "Never Contacted" batch + cadence builder
.env.example                   Copy to .env for local runs; real values go in Railway
```

**`scripts/` lives inside `automation_server/` on purpose.** The Railway
service's Root Directory is `automation_server`, so anything above that
folder is not present in the running container. Scripts the API shells out
to have to live under the root directory or the subprocess call gets a
`FileNotFoundError` in production while working fine locally.

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

## Ops Center routes

The RaiseTell Ops Center (Streamlit, second service in the same Railway
project) calls these. All of them require
`Authorization: Bearer $OPS_CENTER_TOKEN`, and they return 503 rather than
serving if that variable is unset -- an empty token must never be a valid one.

| Route | What it does |
| --- | --- |
| `POST /trigger/outreach-batch?rep=&batch_size=&dry_run=` | Runs `scripts/outreach.py`. Blocks until done, 600s cap. Dry-run ~25s; a real run adds ~20-30s per contact for the Claude call and Attio task creates. |
| `GET /tasks/{rep}` | Open Attio tasks for that rep, left-joined to `outreach_email_drafts` on `task_id`. |
| `PATCH /tasks/{task_id}/complete` | Marks the Attio task completed. |
| `POST /tasks/{task_id}/draft-email?rep=` | Creates an Outlook draft in the rep's own mailbox. |
| `GET /status/smartlead` | Campaign state + `total_leads` + last rotation run. |
| `GET /status/snitcher-review` | Snitcher Review entries at Status = New. |
| `GET /status/allo-tag-registry` | `allo_tag_registry` from MotherDuck. |

### Outlook drafts

`graph_mail.py` uses **delegated** Graph permissions, not application ones:
each rep signs in once at `/auth/microsoft/start?rep=<name>` and their
refresh token is stored in `graph_oauth_tokens`. That way `/me` is genuinely
that rep and the draft appears in their own Drafts folder. Application
permissions would have granted tenant-wide mailbox access and authored the
drafts as the app instead.

Nothing in this repo sends mail — it only creates drafts. Once the rep hits
Send in Outlook, Attio's native mail-sync logs the message on the contact
record. There is no API for writing into Attio's own draft area, and none
is needed.

Setup steps for the Entra app registration are documented at the top of
`automation_server/graph_mail.py`.

## Roadmap

A lightweight UI for viewing/managing these automations is the eventual
goal -- this structure (one backend, routes split by integration) is meant
to make that a frontend-only addition later rather than a rebuild.
