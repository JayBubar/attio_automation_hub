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
  form_fill_routes.py          /webhooks/ac-form-fill
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

## AC form fills → Attio (`/webhooks/ac-form-fill`)

One-way, AC → Attio, and that's the whole pipeline. A contact fills one of
the five tracked forms, the automation's Webhook action POSTs here, and this
route logs the event and appends the matching option to that person's **Form
Filled** multiselect in Attio (`form_filled`). No workflow or sequence fires
from it, nothing is written back to AC, and there is no reverse sync — so
there is no loop-prevention logic here, unlike the tag/list route next door.

Filled-form contacts keep receiving AC's existing marketing email exactly as
they do now. This only makes that history visible on the Attio side.

**The form name comes from the URL.** AC's Webhook action has no "which form
was this" field, so each of the five automations points at its own URL:

```
https://<hub>/webhooks/ac-form-fill?form=Free%20Trial%20Sign%20Up&token=$AC_WEBHOOK_TOKEN
```

**The automation name is not the Attio option title.** The Attio field
predates this pipeline and its options are named for the offer, not for the
automation that fires — three of the five pairs share no words. `TRACKED_FORMS`
in `form_fill_routes.py` is the entire translation layer:

| `?form=` (AC automation) | Attio option written |
| --- | --- |
| Free Trial Sign Up | `Free Trial` |
| Newsletter Signup | `Newsletter` |
| Get the Guide | `Get the Guide` |
| Send Us a Message | `Contact Us` |
| RX Send Us a Message | `RX Contact Us` |

Both columns are exact strings — Attio resolves select options by title and
400s on anything that doesn't match one. Either column is accepted in
`?form=`, so a URL built from the Attio label instead of the automation name
still resolves.

`Form Filled` also carries **ROI Calculator** and **Request a Demo**, which
belong to other pipelines (ROI Calculator flows through the separate
Lovable/Supabase path). This route never writes them and never clears them —
PATCH only adds, so options it doesn't send are untouched. Posting either
name here is treated as an unrecognized form: logged, no Attio write.

An unrecognized form name is still logged, so a typo in a webhook URL is
visible rather than silent. Same for a contact with no matching Attio person
— the row lands, `attio_updated` comes back false, and that row *is* the
"not in the CRM yet" signal.

**Manual prerequisites, neither creatable over an API:**

1. Attio: the `Form Filled` options **Get the Guide** and **RX Contact Us**
   are still being added by hand. Until they exist, a fill on either of those
   two forms logs its row and returns `attio_error` ("Cannot find select
   option…"). The other three work today.
2. ActiveCampaign: contact custom field **Form Filled** — AC-side bookkeeping,
   not read by this route

**MotherDuck:** writes to `hubspot_email_archive.main.contact_activity_log`
(`source`, `event_type`, `contact_email`, `timestamp`, `details`). The route
creates the table if it's missing. `details` is a JSON text blob rather than
typed columns on purpose — Social and Conference follow-ups are meant to
land in this same table under a different `source`, carrying whatever they
need in there instead of a migration.

**`AC_WEBHOOK_TOKEN`** is optional. Unset, the route serves unauthenticated,
matching `/webhooks/activecampaign`. Set it and append `&token=` to all five
URLs — this endpoint writes to MotherDuck and mutates Attio on every call,
so it's worth having.

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
