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
  automation_config.py         Runtime on/off flags (MotherDuck -> env -> default)
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

## AC tag ↔ Attio list sync (`/webhooks/activecampaign`)

Keeps Attio's **Active Marketing Contact** flag, **Prospect Path**, and
membership of the list **Current Marketing Prospects in ActiveCampaign**
(`active_campaign_target_list`) in step with AC's **Attio Marketing Contact**
tag (tag id 3).

**Automation 15 is designed to carry both triggers** — tag added and tag
removed — pointed at the same URL, so `seriesid` is 15 either way and cannot
tell them apart. The route reads `contact[tags]` instead, which lists only
currently-applied tags: marketing tag present means added, absent means
removed. `seriesid` is still checked, as a gate — a payload from any other
automation is logged and ignored rather than acted on.

> ⚠️ **Open manual prerequisite (as of 2026-08-12): the Tag Removed trigger
> does not exist yet.** `GET /api/3/automations/15` reports a single start:
> `{"id":"24","series":"15","type":"tagadd"}`. There is no `tagremove`. The
> receiver handles removals correctly, but AC never sends one — so removing
> the tag in AC is silently a no-op and the contact stays
> `active_marketing_contact=true` in Attio forever. Fix in AC's UI: automation
> 15 → add a second trigger, **Tag Removed**, tag id 3, same webhook URL. This
> cannot be done over the API; AC exposes no trigger-creation endpoint.

A payload with **no `contact[tags]` key at all** is ignored, not treated as a
removal. "Contact has no tags" and "tags field was never mapped on the webhook
action" are indistinguishable otherwise, and guessing wrong un-flags people who
are still in outreach. Every event returning `no_tags_field` means the field is
not mapped in AC.

**Lookup is by `contact[fields][attio_record_id]` first**, email second. The id
survives a contact changing their email address; a stale id (record deleted or
merged) falls back to email rather than failing.

| AC state | Attio writes |
| --- | --- |
| tag present | `active_marketing_contact=true`, path `In Outreach`, date today, + list add |
| tag absent | `active_marketing_contact=false`, path `Cold/Retry Pending`, date today |

List *removal* stays with the Attio-native workflow (Active Marketing Contact =
false → remove from list); only the add is done here.

### Is the bridge alive? (`/status/ac-bridge`)

Every inbound AC webhook is appended to `webhook_event_log` in MotherDuck
(`webhook_log.py`) — **including the ignored ones**, which are the diagnostic:
a run of `no_tags_field` means the tags field isn't mapped on the AC webhook
action, and a run of `unexpected_seriesid` means another automation is pointed
here. Log only successes and both look identical to silence.

Logging is best-effort and never fails the webhook. A MotherDuck outage must
not make the hub 500 back at AC, which would trigger AC's retry-then-disable
behaviour and turn a reporting problem into a real outage.

`GET /status/ac-bridge` returns two facts, deliberately not merged into one
light:

- `route_registered` — is the receiver mounted? Read from the app's own router,
  so a route dropped from `main.py` shows False while `/health` still says 200.
- `events` — last event, 7-day count, and a recent tail. A mounted route with a
  mis-wired AC automation looks perfectly healthy without this.

**Quiet is not broken.** No events just means nobody's tag changed. The
endpoint reports the timestamp and lets the caller judge; it does not
manufacture an up/down verdict out of silence. If the log can't be read it
returns `available: false` with a reason, rather than reporting "no traffic".

### Pausing and resuming the daily rotation

`scripts/outreach_rotation.py` reads its AC pause state from
`automation_config.py` at the start of each run: MotherDuck `automation_config`
row → `AC_PAUSED` env var → default. MotherDuck wins so the Ops Center toggle
takes effect on the next run without a redeploy.

```
GET   /config/flags              current value + source (motherduck/env/default)
PATCH /config/flags/ac_paused?value=false&updated_by=jay
```

**The default is paused.** If the config lookup fails, the run skips AC rather
than pushing — a missed day is recoverable, an unintended send to real people
is not. `source: default` in the GET response is also what a MotherDuck outage
looks like, so the Ops Center can show that instead of implying it read a
stored value.

### Manual prerequisite: `ac_contact_id`

There is **no `ac_contact_id` attribute on People** in the workspace. Writing it
400s the whole PATCH, taking the path and flag updates down with it, so the
rotation script only writes it when `ATTIO_AC_CONTACT_ID_SLUG` is set — and it
should stay empty until the attribute exists. Until then eviction looks the AC
contact up by email instead, which costs one extra AC call per evicted contact.

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
| `GET /status/smartlead` | Campaign state + `total_leads` + last rotation run. Note `total_leads` is lifetime, not active — see below. |
| `GET /status/ac-bridge` | AC↔Attio bridge: receiver mounted, last webhook event, recent tail. |
| `GET /status/snitcher-review` | Snitcher Review entries at Status = New. |
| `GET /status/allo-tag-registry` | `allo_tag_registry` from MotherDuck. |
| `GET /config/flags` | Automation on/off flags with their resolved source. |
| `PATCH /config/flags/{key}?value=` | Toggles a flag. 503 if it could not be persisted. |

### `total_leads` is lifetime, not active

Smartlead documents `total_leads` on `GET /campaigns/{id}/leads` as the count
of leads **matching the filter criteria**, and the callers here send no status
filter — so it spans every status the campaign holds: `STARTED`, `INPROGRESS`,
`COMPLETED`, `PAUSED`, `STOPPED`.

Nothing in this repo ever removes a lead from a Smartlead campaign.
`smartlead_routes.py` handles `SEQUENCE_COMPLETED` / `EMAIL_REPLIED` /
`EMAIL_BOUNCED` / `LEAD_UNSUBSCRIBED` by writing to **Attio only**. So
`total_leads` is monotonic — it only ever goes up.

`outreach_rotation.py` used to subtract it from the target pool as if it were
the active count. That math inverts itself over time: once the campaign's
lifetime total passes `max_leads_per_day × sequence length`, headroom pins to
zero permanently and the campaign silently stops being topped up — precisely
when the sending inboxes are emptiest. The script now sizes the pool from
Attio instead (`active_cold_outreach_contact = true`), which is the one place
"still in sequence" actually decays, and prints the lifetime total alongside it
as a diagnostic. The two converging means the Smartlead webhooks have stopped
clearing the flag.

### Smartlead list membership: the checkbox, not a list

The AC path adds people to an Attio list (`active_campaign_target_list`). The
Smartlead path deliberately **does not**, even though a `Smartlead Target List`
(`smartlead_target_list`) exists in the workspace.

That list has no custom attributes — only Attio's built-in `entry_id`,
`created_at`, `created_by` — so membership carried nothing that
`active_cold_outreach_contact` doesn't already carry, and it sat at zero
entries with nothing reading it. The checkbox is what the Smartlead webhook
handlers clear and what sizes the pool, making it the one signal that actually
decays; "when did this person enter outreach" is answered by the
`outreach_rotation_log` checkpoint table. Two sources of truth for one fact is
how they drift, so there is only one.

The empty `Smartlead Target List` can be deleted in Attio's UI whenever
convenient. Left in place it's harmless but misleading — it reads like a
half-finished integration.

### The "Add to Smartlead" intake queue

`Add to Smartlead` (`add_to_smartlead`) is a **hand-curated queue**, and it is
now wired up: `push_smartlead_batch` drains it ahead of the Never Contacted
pool, because someone deliberately put a person there while the pool is just
whoever sorts earliest. The entry is removed only after the lead is in
Smartlead *and* the Attio flag is set — dropping it earlier would lose the
request if the push failed, and the list is the only record that a human ever
asked.

It had been holding 90+ people added on 2026-07-29 that nothing read: the push
only ever queried the Never Contacted pool, so a deliberate "put these in
outreach" did nothing and looked exactly like a queue being worked.

Three cases are not pushed:

| Case | What happens |
| --- | --- |
| already `active_cold_outreach_contact` | entry dropped — the request is already fulfilled, and leaving it would strand it forever |
| `do_not_migrate = true` | **left on the list** for a human |
| no email address | **left on the list** for a human |

The last two stay put on purpose. They're the cases someone needs to look at,
and silently dropping them is how a request disappears with nobody noticing.

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
