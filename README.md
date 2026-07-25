# Azure Data Factory - Pipelines

# 1. CDC Ingestion Pipeline — Azure Data Factory
 
Incremental (CDC-style) load of SQL tables into ADLS Gen2 Parquet, orchestrated by two ADF pipelines and backed by a Logic App for failure alerts.
 
## Architecture
 
```
Scheduled (daily trigger)
  └─ Execute Pipeline1 ──▶ Ingestion
        ├─ On Success: done
        └─ On Failure: Alerts (Web) ──▶ Logic App ──▶ Email
```
 
**Components**
- ADLS Gen2 storage account — `source` container, `monitor` directory (holds `cdc.json` checkpoint and the throwaway `empty.json`)
- Azure SQL DB — schema `source`, table `Orders` (parameterized, so other schema/table pairs work too)
- `cdc.json` — seeded at `1900-01-01` so the first run picks up the entire table
- Logic App (Consumption) — HTTP-triggered, sends the failure email
---
 
## Pipeline: `Ingestion`
 
Does the actual incremental load for one schema/table.
 
| Parameter | Default | Purpose |
|---|---|---|
| `schema` | `source` | target schema |
| `table` | `Orders` | target table |
| `backdate` | `2025-09-24` | manual override date; blank = trust checkpoint |
<img width="1285" height="507" alt="Ingestion pipeline" src="https://github.com/user-attachments/assets/3e0fd401-63ba-4b61-bc0a-3521f33b8aaf" />

<img width="995" height="222" alt="If new rows" src="https://github.com/user-attachments/assets/553485da-5a3b-4ed5-a7c7-eba100a0791c" />

### Activities
 
1. **`last_cdc`** (Lookup, `ls-datalake`)
   Reads `cdc.json` to get the last-processed timestamp.
2. **`Totalresults`** (Script, `ls-database`)
   Counts rows changed since the checkpoint (or since `backdate` if supplied):
```sql
   SELECT count(*) as total_count
   FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table}
   WHERE last_updated > '@{if(empty(pipeline().parameters.backdate),
                               activity('last_cdc').output.value[0].cdc_timestamp,
                               pipeline().parameters.backdate)}'
```
   The `if(empty(...))` expression is the key logic: blank `backdate` → trust `cdc.json`; supplied `backdate` → force a re-scan from that date without touching the checkpoint.
 
3. **`If New Rows`** (If Condition)
   `@greater(activity('Totalresults').output.resultSets[0].rows[0].total_count, 0)`
   - **False** → no-op (nothing changed since last run)
   - **True** →
     - **`Load`** (Copy) — same dynamic `WHERE last_updated > ...` query, Azure SQL → Parquet in ADLS Gen2
     - **`maxCDC`** (Script) — `SELECT MAX(last_updated) as cdc_timestamp FROM @{schema}.@{table}`, computes the new high-water mark
     - **`change_cdc`** (Copy) — reads a throwaway `empty.json`, adds an Additional Column `cdc_timestamp = @activity('maxCDC').output.resultSets[0].rows[0].cdc_timestamp`, and sinks into `cdc.json` — this overwrite is the checkpoint update for the next run
**Tested:** insert rows → confirm True branch fires and loads only the delta → re-run with no new rows → confirm False branch (no-op).
 
---
 
## Pipeline: `Scheduled`
 
Wraps `Ingestion` with a daily trigger and failure alerting.
 
| Parameter | Default | Purpose |
|---|---|---|
| `schema` | `source` | forwarded to `Ingestion` |
| `table` | `Orders` | forwarded to `Ingestion` |
| `backdate` | *(empty string)* | forwarded to `Ingestion`; left blank so scheduled runs always trust the checkpoint |
| `alertsUrl` | *(empty string)* | Logic App trigger URL used by the `Alerts` Web activity; supplied at trigger/deployment time, never checked in with a real value |
<img width="686" height="190" alt="Scheduled pipeline" src="https://github.com/user-attachments/assets/580c6cae-6ee4-488f-aec7-f9de3cf36279" />

### Activities
 
1. **`Execute Pipeline1`** (Execute Pipeline → `Ingestion`, wait on completion) — forwards `schema` / `table` / `backdate` straight through.
2. **`Alerts`** (Web activity, runs only when `Execute Pipeline1` **Failed**) — `POST`s to the Logic App trigger URL, now read from the `alertsUrl` pipeline parameter (`secureInput: true` so the resolved URL is masked in activity run history):
```json
   { "pipeline name": "@{pipeline().Pipeline}", "pipeline runid": "@{pipeline().RunId}" }
```
3. **Trigger** — daily recurrence. `backdate` is intentionally left blank for scheduled runs; it's only meant to be overridden manually for recovery/backfill runs.
 
---
 
## Logic App: Failure Alert
 
Consumption-tier workflow:
 
```
When an HTTP request is received  ──▶  Send an email (V2)
```
<img width="365" height="349" alt="Logic app" src="https://github.com/user-attachments/assets/3e7cf297-eb0f-454f-9d05-72c3e3fdb617" />

 
- **Trigger**: `When an HTTP request is received`, with a Request Body JSON Schema expecting `pipeline name` and `pipeline runid` (strings). Its callback URL is what's configured as the `Alerts` Web activity URL in `Scheduled`.
- **Action**: `Send an email (V2)` (Outlook connector)
  - Subject: `Pipeline Failed, Wake UP`
  - Body: includes the `pipeline name` and `pipeline runid` dynamic tokens from the trigger
 
---
 
## Recovery / Backfill Runs
 
To re-process data from an arbitrary date without disturbing the checkpoint, trigger `Scheduled` (or `Ingestion` directly) manually with `backdate` set to the desired date (e.g. `2025-01-01`). `cdc.json` is only updated by the `change_cdc` activity, so backfills never overwrite the running checkpoint.

---

# 2. REST API Ingestion — PokeAPI, Two Pagination Strategies
 
Two ADF pipelines that pull the full Pokémon list from the public [PokeAPI](https://pokeapi.co/api/v2/pokemon) into ADLS Gen2 as JSON. Both do the same job end-to-end but paginate through the API differently — one follows the API's own "next page" link, the other computes offsets itself. Which one to use depends entirely on whether the API you're integrating with hands you a next-page URL or not.
 
## Common setup
- **Linked service**: `ls_rest` — base URL set to the API root, authentication: Anonymous
- **Sink**: `ds_API` — Azure Data Lake Gen2, JSON, file path `sink/API/pokemon.json`
- **API shape**: `GET /api/v2/pokemon` returns a paged JSON body containing a `count` (total items — 1300 for Pokémon), a `next` URL (or `null`/absent when there's no more data), and a `results` array of 20 items per page
- At 20 items per call and ~1300 total items, a full pull takes ~65 API calls
Both pipelines follow the same two-activity shape:
 
```
Endpoint (Web)  ──▶  CopyAllData (Copy)
```
<img width="625" height="162" alt="REST API absoluteurl" src="https://github.com/user-attachments/assets/a25ae9d0-f63d-4566-860f-f6c38d1db5d4" />
 
1. **`Endpoint`** (Web Activity) — `GET https://pokeapi.co/api/v2/pokemon`. Used purely to probe the API once (e.g. to read `count`) before the paginated Copy activity runs.
2. **`CopyAllData`** (Copy Activity) — `RestSource` → `JsonSink`, with a **pagination rule** that tells ADF how to fetch every subsequent page automatically. This rule is the only thing that differs between the two pipelines.
---
 
## Pipeline: `RestAPI` — AbsoluteUrl pagination
 
Use this approach whenever the API response includes a direct link to the next page (the common, well-behaved case).
 
**Source dataset**: `ds_Rest_source` (base URL only — no relative path/query needed, since ADF gets the URL to call next straight from the response)
 
**Pagination rule**:
| Name | Value |
|---|---|
| `AbsoluteUrl` | `$.next` |
 
**How it works**: after each call, ADF reads the `next` field from the JSON response body and calls that exact URL for the following page. It keeps going until `next` is empty/null, at which point pagination stops automatically — no manual end condition needed.
 
**Setup steps**
1. Create pipeline `RestAPI`.
2. Add Web activity `Endpoint` → method `GET` → URL `https://pokeapi.co/api/v2/pokemon` → Debug to confirm it pulls data.
3. Add Copy activity `CopyAllData`, connected from `Endpoint`'s green (Success) output.
4. Source: `+ New` → REST → name `ds_Rest_source` → linked service `ls_rest` (base URL set, Anonymous auth) → Create.
5. Sink: `+ New` → Azure Data Lake Gen2 → JSON → name `ds_API` → file path `sink/API/pokemon.json`.
6. Debug — confirms only the first 20 records land initially (no pagination rule yet).
7. In `CopyAllData` → Source → Pagination rules → delete any existing rule → Add New → Name: `AbsoluteUrl`, Value: `$.next`.
8. Debug again — now the pipeline follows `next` across every page until it's empty, pulling all ~1300 records.
---
 
## Pipeline: `RestAPI_offset` — Query Parameter Offset pagination
 
Use this approach when the API does **not** return a next-page URL, so you have to compute the next page yourself.
 
**Source dataset**: `ds_rest_offset` — base URL already set on the linked service; relative URL: `?offset={offset}`
 
**Pagination rule**:
| Name | Value |
|---|---|
| Query Parameter `offset` | `RANGE:0:@{activity('Endpoint').output.count}:20` |
| End Condition `$.next` | `Empty` |
 
**How it works**:
- `RANGE:start:end:step` drives a loop over the `offset` query parameter: starting at `0`, going up to the total item count (read dynamically from `Endpoint`'s response `count` field), in steps of `20` (the page size) — i.e. `offset=0`, `offset=20`, `offset=40`, … up to ~1300.
- The **End Condition** (`$.next` = `Empty`) is a safety net: even though the range is computed from `count`, pagination also stops early if a response's `next` field comes back empty — covering cases where the total count changes mid-run or the API stops returning data sooner than expected.
**Setup steps**
1. Clone the `RestAPI` pipeline (or build fresh) and rename to `RestAPI_offset`.
2. Keep `Endpoint` (Web, `GET https://pokeapi.co/api/v2/pokemon`) — its `count` field feeds the range below.
3. In `CopyAllData` → Source → Pagination rules → delete the existing `AbsoluteUrl` rule.
4. Add rule: **Query parameter** `offset` → value:
```
   RANGE:0:@{activity('Endpoint').output.count}:20
```
5. Add rule: **End condition** `$.next` → `Empty`.
6. Source dataset `ds_rest_offset` — base URL from the linked service, relative URL `?offset={offset}`.
7. Debug — pipeline pulls all pages by incrementing `offset` in steps of 20 until the range is exhausted or `next` comes back empty, storing everything in `sink/API/pokemon.json`.
---
 
## Which one to use
 
| Scenario | Use |
|---|---|
| API response includes a usable "next page" URL | `RestAPI` (AbsoluteUrl) — simpler, no need to know total count upfront |
| API does **not** expose a next-page URL, but supports an `offset`/`limit`-style query parameter | `RestAPI_offset` (Query Parameter Offset + Range) |
 
## Notes / Caveats
- The offset approach depends on knowing the total item count (`count`) up front from a single probe call (`Endpoint`) — if the API doesn't expose a total count either, the range would need a hardcoded or otherwise-derived upper bound.
- This is a manual/on-demand pull (not scheduled) and pulls the full dataset from scratch each run — it does **not** implement incremental/CDC-style loading. For very large or frequently-changing datasets (millions of rows), a real-time/incremental design is more appropriate; this pattern is meant for pulling a full reference/lookup dataset such as a paginated public API listing.
- Pagination behavior is API-specific — always check the target API's docs to see whether it returns a next-page link (`AbsoluteUrl`) or expects the caller to compute paging (`offset`/`limit`, `page`, cursor tokens, etc.) before picking a rule.
---

# 3. Router Pipeline — Route Files to Destination Folders by Filename
 
Watches for a small "flag" file, then scans a source folder and routes every file it finds into the correct destination folder based on the file's name (e.g. `customers.csv` → `sink/customers/`, `drivers.csv` → `sink/drivers/`, `trips.csv` → `sink/trips/`). Built as a lightweight alternative to a Storage Event trigger.
 
## Architecture
 
```
Validate ──▶ Get Metadata ──▶ ForEachFile ──▶ Delete
                                  └─ SwitchFile (per item)
                                       ├─ customers ─▶ Copy Customers
                                       ├─ drivers   ─▶ Copy Drivers
                                       └─ trips     ─▶ Copy Trips
```
 
**Why this shape**: Storage Event triggers aren't available on every subscription tier and add cost, so this pipeline polls instead — it waits for a flag file to appear, then does the routing work, then deletes the flag file so the next run starts clean.
<img width="1291" height="347" alt="Router" src="https://github.com/user-attachments/assets/29f4e4ea-975d-45ac-95a1-d3bad6b3330b" />
<img width="257" height="610" alt="Router1" src="https://github.com/user-attachments/assets/d74d25ba-0917-482d-8e28-0168d45bfb93" />

---
 
## Step 1: `Validate` (Validation Activity)
 
Polls for a specific flag file before anything else runs.
 
- **Dataset**: `ds_csv_dynamic` → `p_container = source`, `p_folder = trigger`, `p_file = locations.csv`
- **Timeout**: `0.12:00:00` (12 hours)
- **Sleep**: `10` (seconds between existence checks)
While debugging, this activity keeps re-checking the `trigger` folder for `locations.csv`. Once that file is uploaded, the activity succeeds and the pipeline moves forward. This is effectively a manual, polling-based stand-in for a Storage Event trigger — the timeout/sleep can also be tuned or scheduled for a specific window (e.g. 12 PM–4 PM) so the pipeline only fires once per upload during business hours.
 
---
 
## Step 2: `Get Metadata`
 
Lists every file sitting in the folder that needs routing.
 
- **Dataset**: `ds_meta` (Azure Data Lake Gen2, CSV) → file path set to `source/files`, "First row as header" checked
- **Field list**: `childItems`
- **Depends on**: `Validate` (Succeeded)
`childItems` comes back as an array of objects, one per file:
```json
[
  { "name": "customers.csv", "type": "File" },
  { "name": "drivers.csv",   "type": "File" },
  { "name": "trips.csv",     "type": "File" }
]
```
 
---
 
## Step 3: `ForEachFile` (ForEach) → `SwitchFile` (Switch)
 
- **Items**: `@activity('Get Metadata').output.childItems` — iterates once per file found above
- Inside the loop, a **Switch** activity (`SwitchFile`) decides which Copy activity to run:
  - **On**: `@split(item().name,'.')[0]` — strips the extension off the current file's name (e.g. `customers.csv` → `customers`)
  - **Cases**:
| Case value | Activity | Sink folder |
|---|---|---|
| `customers` | `Copy Customers` | `sink/customers/` |
| `drivers` | `Copy Drivers` | `sink/drivers/` |
| `trips` | `Copy Trips` | `sink/trips/` |
| *(Default)* | — (empty) | file is silently skipped |
 
Each Copy activity uses the same **dynamic dataset** (`ds_csv_dynamic`) on both source and sink, driven by three dataset parameters — `p_container`, `p_folder`, `p_file` — instead of needing a separate hardcoded dataset per file type:
 
- **Source**: `p_container = source`, `p_folder = files`, `p_file = @item().name`
- **Sink**: `p_container = sink`, `p_folder = customers` / `drivers` / `trips` (matching the case), `p_file = @item().name`
This is what lets one dataset + one Copy-activity pattern serve all three file types — only the sink's `p_folder` value changes per case.
 
---
 
## Step 4: `Delete`
 
- **Depends on**: `ForEachFile` (Succeeded)
- **Dataset**: `ds_csv_dynamic` → `p_container = source`, `p_folder = trigger`, `p_file = locations.csv`
- **Enable logging**: unchecked (off)
Removes the flag file from the `trigger` folder once routing is complete, so the pipeline is ready to detect the *next* upload rather than re-triggering on the same file.
 
---
 
## End-to-end flow
 
1. Someone uploads `customers.csv`, `drivers.csv`, `trips.csv` into `source/files/`.
2. Someone (or an upstream process) uploads `locations.csv` into `source/trigger/` — this is the signal that files are ready.
3. `Validate` detects `locations.csv` and lets the pipeline proceed.
4. `Get Metadata` lists everything in `source/files/`.
5. `ForEachFile` + `SwitchFile` routes each file to its matching destination folder based on filename.
6. `Delete` removes `locations.csv` from `trigger/`, resetting for the next run.
---
 
## Notes / Things to double-check
 
- **Case values are case- and spelling-sensitive.** `SwitchFile` matches on the exact string produced by `split(item().name,'.')[0]`. If a source file is actually named `customer.csv` (singular) but the switch case is `customers` (plural), it won't match — it silently falls into the empty **Default** case and never gets copied, with no error or alert raised. Worth confirming the real file-naming convention in `source/files/` matches the case values exactly (`customers`, `drivers`, `trips`).
- **Unmatched files are silent.** Since Default has no activities, any file that doesn't match one of the three cases is simply skipped with no logging or notification. Consider adding a Default action (e.g. logging the unmatched filename, or a failure/alert branch) if unexpected files showing up in `source/files/` should be visible.
- **This is a polling design, not an event-driven one.** The `Validate` activity's `sleep`/`timeout` control how promptly and how long it waits for the flag file — tune these (or run the pipeline on a schedule/window) based on how quickly routing needs to happen after upload.
- **Storage Event triggers were intentionally avoided** here due to cost and subscription-tier availability; if that changes, replacing `Validate` with a native Storage Event trigger on `source/trigger/` would remove the polling overhead entirely.
---

# 4. Schema Pipeline — Dynamic Column Mapping in ADF
 
Copies every file from a source folder to a destination folder using **one** Copy activity, while applying a **different column mapping (translator) per file** — `customers.csv` gets the customers mapping, `drivers.csv` gets the drivers mapping, `trips.csv` gets the trips mapping — instead of needing a separate Copy activity per file type.
 
## Why this exists
A single dynamic dataset (`ds_csv_dynamic` + a `ForEach` over the source folder) already lets one Copy activity move any file by filename. The remaining problem is **schema**: each file has different columns, so the mapping ("translator") can't be hardcoded — it has to be picked dynamically based on which file is currently being copied. This pipeline solves that by storing each file's mapping as a **pipeline parameter** and picking the right one at runtime with an `if/equals` expression.
 
## Architecture
 
```
Get Metadata ──▶ ForEachFile
                     └─ Data Load (Copy, translator picked per-file)
```
 
- **`Get Metadata`** — dataset `ds_meta`, field list `childItems`, lists every file in `source/files/`.
- **`ForEachFile`** — iterates `@activity('Get Metadata').output.childItems`; inside, a single Copy activity (`Data Load`) handles every file.
<img width="640" height="365" alt="Schema" src="https://github.com/user-attachments/assets/1a7419ce-3aaa-4127-87ae-f9bb9bb27343" />

---
 
## Pipeline parameters — one schema per file
 
Three parameters hold the actual column mappings, each pasted in as a full `TabularTranslator` JSON object:
 
| Parameter | Type | Holds |
|---|---|---|
| `customers_schema` | object | Mapping for `customers.csv` (`customer_id`, `first_name`, `last_name`, `email`, `phone_number`, `city`, `signup_date`, `last_updated_timestamp`) |
| `drivers_schema` | object | Mapping for `drivers.csv` (`driver_id`, `first_name`, `last_name`, `phone_number`, `vehicle_id`, `driver_rating`, `city`, `last_updated_timestamp`) |
| `trips_schema` | object | Mapping for `trips.csv` (`trip_id`, `driver_id`, `customer_id`, `vehicle_id`, `trip_start_time`, `trip_end_time`, `start_location`, `end_location`, `distance_km`, `fare_amount`, `payment_method`, `trip_status`, `last_updated_timestamp`) |
 
Each default value is a full source→sink column mapping (kept 1:1, same names/types — the point here is dynamic *selection* of mappings, not transformation).
 
## `Data Load` (Copy Activity)
 
**Source** (`ds_csv_dynamic`): `p_container = source`, `p_folder = files`, `p_file = @item().name`
**Sink** (`ds_csv_dynamic`): `p_container = sink`, `p_folder = schema`, `p_file = @item().name`
 
**Translator** — instead of a static mapping, this is an expression that switches on the current file's name:
```
@if(equals(item().name,'customers.csv'), pipeline().parameters.customers_schema,
    if(equals(item().name,'drivers.csv'), pipeline().parameters.drivers_schema,
        pipeline().parameters.trips_schema))
```
For each file the loop hits, this resolves to exactly one of the three parameter objects and ADF applies that mapping for the copy. Since `trips_schema` is the final `else`, any file that isn't `customers.csv` or `drivers.csv` falls through to the trips mapping by default.
 
---
 
## How this was built (repeatable process)
 
1. **Get one mapping working manually first.** Build a throwaway Copy activity with source = `customers.csv` (hardcoded `p_file`), sink = `schema/customers.csv`, then in **Mapping → Import schemas** let ADF infer the column list — this is the fastest way to get a correct mapping without typing it by hand.
2. **Grab the generated code.** Open the Copy activity's **Code** view and copy everything from `"translator"` onward (the mapping JSON ADF just built for you).
3. **Delete the throwaway Copy activity** — it was only there to generate the mapping code.
4. **Repeat step 1–2 for each file** (`drivers.csv`, `trips.csv`) to get all three mapping blocks.
5. **Create the pipeline parameters.** In the pipeline's Parameters tab, add `customers_schema`, `drivers_schema`, `trips_schema` (all type `object`), and paste each file's generated mapping JSON in as that parameter's default value.
6. **Build the real dynamic pipeline**: `Get Metadata` (dataset `ds_meta`, field list `childItems`) → `ForEachFile` (Items = child items) → inside, one Copy activity (`Data Load`) using the dynamic dataset for source/sink.
7. **Set the translator as an expression** (not a static mapping) using the `if(equals(...))` chain above, referencing the three parameters.
8. Debug, confirm all three files land in `sink/schema/` with the correct columns, then publish.
---
 
## Scaling beyond 3 file types
 
Hardcoding one pipeline parameter per file type works fine for a handful of schemas, but doesn't scale to, say, 100 file types. For that case, consider instead:
- Storing all mappings as one JSON array/config file (e.g. `{ "customers.csv": {...}, "drivers.csv": {...}, ... }`) and looking up the right mapping by filename via a `Lookup` activity, or
- Storing the mapping definitions in a SQL table (one row per file type) and driving the translator lookup from a query, rather than a hardcoded `if/equals` chain per file.
Both avoid needing a new pipeline parameter and a new `if(equals(...))` branch every time a new file type is added.
 
## Notes / Things to double-check
- The `if/equals` chain matches on **exact filename**, including extension (`'customers.csv'`, `'drivers.csv'`) — case-sensitive and extension-sensitive. Any filename that doesn't match either of the first two conditions silently falls through to `trips_schema`, so an unexpected file (e.g. a typo'd name or a 4th file type accidentally dropped into `source/files/`) would get copied using the wrong mapping rather than failing loudly.
- All three schemas currently keep source and sink column names identical (straight pass-through mapping with type conversion allowed and truncation permitted) — if any target table needs renamed or reordered columns, that's where the mapping objects would need actual source→sink differences rather than 1:1 copies.
---

# 5. Deltalake Pipeline — Upsert into Delta Lake (Silver Layer)
 
Reads a CSV, applies a couple of light transformations, and **upserts** (insert new + update existing) the result into a Delta Lake table — keyed on `location_id`. Built with a Mapping Data Flow rather than a plain Copy activity, since Delta merge/upsert logic needs the data flow engine (Copy activities can only append/overwrite, not merge on a key).
 
## Why Data Flow instead of Copy Data
 
| | Use for |
|---|---|
| **Copy Data** | Moving data from source into the **bronze** layer — straight land-as-is copies |
| **Data Flow** | Moving data from **bronze into silver/gold** — anything needing transformations, upserts, or Delta merge logic |
 
Upserts specifically require Data Flow: it's low-code/no-code on the surface, but runs as generated Spark under the hood, which is what makes key-based merge (update-if-matched, insert-if-not) possible.
 
## Architecture
 
```
Deltalake (pipeline)
  └─ Data flow (ExecuteDataFlow) ──▶ deltaflow (Mapping Data Flow)
                                       source → UpperState → selectCols → alterRow → sinkDelta
```
 
---
 
## Pipeline: `Deltalake`
 
One activity: **`Data flow`** (Execute Data Flow), pointing at the `deltaflow` Mapping Data Flow.

<img width="315" height="171" alt="Deltalake" src="https://github.com/user-attachments/assets/5ee86afd-02cc-4ca6-9916-41b9f9b95804" />
 
**Dataset parameters passed to the data flow's `source`:**
| Parameter | Value |
|---|---|
| `p_container` | `source` |
| `p_folder` | `Deltasource` |
| `p_file` | `locations.csv` |
 
**Compute**: `coreCount: 8`, `computeType: General` (per notes: use **Small** compute size for this workload)
**Trace level**: `None` (matches "Logging level: None" from notes)
**Cache sinks**: `firstRowOnly: true`

---
 
## Data Flow: `deltaflow`

<img width="1741" height="197" alt="Deltaflow" src="https://github.com/user-attachments/assets/14391b2d-0905-45a8-b564-b88c5cea6746" />

### 1. `source`
Reads `ds_csv_dynamic` with an explicit projection:
 
| Column | Type |
|---|---|
| `location_id` | short |
| `city` | string |
| `state` | string |
| `country` | string |
| `latitude` | double |
| `longitude` | double |
| `last_updated_timestamp` | timestamp |
 
Settings: `allowSchemaDrift: true` (lets the CSV's schema evolve over time without breaking the flow), `validateSchema: false` (only worth turning on if the schema is guaranteed never to change), `ignoreNoFilesFound: false`.
 
### 2. `UpperState` (Derived Column)
```
state = upper(state)
```
Normalizes the `state` column to uppercase.
 
### 3. `selectCols` (Select)
Maps forward only: `location_id`, `city`, `state`, `country`, `last_updated_timestamp` — **`latitude` and `longitude` are dropped here** and don't reach the sink.
 
### 4. `alterRow` (Alter Row)
```
upsertIf(1==1)
```
Marks every row as an upsert candidate (the `1==1` condition is always true, so this applies to 100% of incoming rows — no rows are filtered out at this stage).
 
### 5. `sinkDelta` (Sink)
- **Type**: Inline dataset, format `delta`
- **Linked service**: `ls_datalake`
- **Path**: `sink/deltasink`
- **Row policies**: `insertable: true`, `upsertable: true`, `updateable: false`, `deletable: false` — new rows are inserted and existing rows matching the merge key are upserted; a separate/plain "update" and "delete" are not enabled
- **Merge key**: `location_id` — this is what determines whether an incoming row is treated as a new insert or an update to an existing row
- **Other settings**: `mergeSchema: false`, `autoCompact: false`, `optimizedWrite: false`, `vacuum: 0`, no compression
---
 
## Upsert logic, visually
 
```
Existing table:        Incoming write:        Result:
  1, 2, 3        +        1, 2, 4, 5      =    1 (updated), 2 (updated), 3 (untouched), 4 (inserted), 5 (inserted)
```
Rows whose `location_id` already exists get updated in place; rows with a new `location_id` get inserted; rows not present in the incoming batch are left alone.
 
---
 
## How this was built (repeatable process)
 
1. Create folder `source/Deltasource` and upload `locations.csv`.
2. Create pipeline `Deltalake`, drag in a **Data flow** activity, settings → `+New` → build/name the data flow `deltaflow`.
3. In the data flow: **Add Source** → name `source` → source type: dataset → `ds_csv_dynamic`.
4. Turn on **Data Flow Debug** (spins up a live cluster) to preview data as you build.
5. In source **debug settings → parameters**, set `p_container = source`, `p_folder = Deltasource`, `p_file = locations.csv` so previews reflect the real file.
6. Review source options:
   - **Allow schema drift** — lets future columns show up without breaking the flow
   - **Validate schema** — only if the schema is guaranteed stable
   - **Skip line count** — to skip header/junk rows if needed
   - **Sampling** — pull a small sample for faster iteration while building/debugging
7. Add a **Derived Column** transformation named `UpperState`: column `state`, expression `upper(state)`. Refresh Data Preview to confirm.
8. Add a **Select** transformation named `selectCols`: remove `latitude` and `longitude`. Refresh Data Preview.
9. Add an **Alter Row** transformation named `alterRow`: condition `Upsert if` → `1==1` (passes all rows through as upsert candidates).
10. Add a **Sink** named `sinkDelta`:
    - Sink type: Inline, dataset type: Delta
    - Linked service: `ls_datalake`
    - File path: `sink/deltasink`
    - No compression, vacuum: 0
    - Enable **Allow upsert**, set merge **key column**: `location_id`
11. Refresh Data Preview once more to confirm the end-to-end result, then **turn off the data flow debug cluster** and save (leaving debug on burns compute unnecessarily).
12. Back in the `Deltalake` pipeline, set the Execute Data Flow activity's parameter values, **Compute size: Small**, **Logging level: None**, then **Debug → Use activity runtime**.
13. Once it runs successfully, save, open a Pull Request, and merge into the `master` branch.
14. On the `master` branch in ADF, the pipeline is already present from the merge — click **Publish**.
## Notes / Things to double-check
- **Update and delete are off at the sink** (`updateable: false`, `deletable: false`) even though `alterRow` marks every row as an upsert candidate. This is expected for a pure insert-or-update-on-match upsert pattern (upsert covers both the "new row" and "matching row changed" cases together), but if a genuine separate "update-only" or "delete" scenario is ever needed, those flags would need to be enabled explicitly.
- **`mergeSchema: false`** means if the source CSV picks up new columns in the future (allowed via schema drift on the source), those new columns won't automatically propagate into the Delta table structure — they'd need `mergeSchema: true` or a manual schema update to land in the sink.
- **`vacuum: 0`** means no automatic cleanup of old Delta file versions is happening — worth revisiting if storage/versioning cost becomes a concern over time.
- The merge key is `location_id` alone — make sure this is genuinely unique per real-world location in the source data; a non-unique key here would cause updates to apply non-deterministically across matching rows.
  
---

# 6. Ingestion_Logging & Scheduled_Logging — CDC Load with DB-Based Run Logging
 
Evolution of the original `Ingestion`/`Scheduled` pipelines: the JSON-file checkpoint (`cdc.json`) is replaced with a proper **SQL log table** (`source.pipeline_run_log`). Every run — successful, no-op, or failed — writes a row, giving full history, correct per-table/schema scoping, and a queryable audit trail instead of a single overwritten blob file.
 
## Architecture
 
```
Scheduled_Logging (daily trigger)
  └─ Execute Pipeline 2 ──▶ Ingestion_Logging
        ├─ On Success: done
        └─ On Failure ─┬─▶ Alerts (Web → Logic App email)
                        └─▶ LogFailure (marks the run row Failed)
 
Ingestion_Logging:
  last_cdc ──▶ LogStart ──▶ Totalresults ──▶ If New Rows
                                                 ├─ True  ─▶ Load ─▶ LogSuccess
                                                 └─ False ─▶ LogNoOp
```
 
---
 
## SQL tables schema 

```sql
CREATE schema source;

CREATE TABLE Orders (
    order_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    product_name VARCHAR(100),
    quantity INT,
    price DECIMAL(10,2),
    order_date DATETIME,
    last_updated DATETIME DEFAULT GETDATE()
);


-- INITIAL
INSERT INTO Orders (order_id, customer_name, product_name, quantity, price, order_date, last_updated)
VALUES
(1, 'John Doe', 'Laptop', 1, 1200.00, '2025-09-20', '2025-09-20 10:15:00'),
(2, 'Alice Smith', 'Phone', 2, 800.00, '2025-09-21', '2025-09-21 14:30:00'),
(3, 'Bob Johnson', 'Headphones', 3, 150.00, '2025-09-22', '2025-09-22 09:45:00'),
(4, 'Charlie Brown', 'Monitor', 1, 300.00, '2025-09-22', '2025-09-22 18:10:00'),
(5, 'Diana Prince', 'Keyboard', 2, 100.00, '2025-09-23', '2025-09-23 11:20:00'),
(6, 'Ethan Hunt', 'Mouse', 5, 50.00, '2025-09-23', '2025-09-23 16:40:00'),
(7, 'Fiona Adams', 'Printer', 1, 200.00, '2025-09-24', '2025-09-24 09:05:00'),
(8, 'George Wilson', 'Tablet', 2, 450.00, '2025-09-24', '2025-09-24 13:25:00'),
(9, 'Hannah Lee', 'Smartwatch', 1, 220.00, '2025-09-25', '2025-09-25 08:50:00'),
(10, 'Ian Carter', 'Camera', 1, 900.00, '2025-09-25', '2025-09-25 15:15:00');


-- INCREMENTAL
INSERT INTO Orders (order_id, customer_name, product_name, quantity, price, order_date, last_updated)
VALUES
(11, 'Jack Miller', 'Drone', 1, 1500.00, '2025-09-26', '2025-09-26 10:00:00'),
(12, 'Kelly Green', 'Speakers', 2, 180.00, '2025-09-26', '2025-09-26 14:45:00'),
(13, 'Liam Scott', 'Charger', 3, 25.00, '2025-09-27', '2025-09-27 09:30:00'),
(14, 'Mia Turner', 'Desk Lamp', 1, 60.00, '2025-09-27', '2025-09-27 12:20:00'),
(15, 'Noah Brooks', 'Gaming Chair', 1, 350.00, '2025-09-27', '2025-09-27 18:10:00');

-- Logging Table
CREATE TABLE source.pipeline_run_log (
    log_id            INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name     VARCHAR(200) NOT NULL,
    run_id            VARCHAR(100) NOT NULL,
    schema_name       VARCHAR(100) NOT NULL,
    table_name        VARCHAR(100) NOT NULL,
    start_time        DATETIME2 NOT NULL,
    end_time          DATETIME2 NULL,
    status            VARCHAR(20) NOT NULL,      -- 'Started' / 'Succeeded' / 'Failed'
    cdc_timestamp     DATETIME2 NULL,             -- checkpoint used going INTO this run
    new_cdc_timestamp DATETIME2 NULL,             -- checkpoint coming OUT of this run
    rows_processed    INT NULL,
    error_message     VARCHAR(MAX) NULL
);

-- Index
CREATE INDEX ix_pipeline_run_log_lookup
    ON source.pipeline_run_log (table_name, schema_name, status);


-- INCREMENTAL
INSERT INTO Orders (order_id, customer_name, product_name, quantity, price, order_date, last_updated)
VALUES
(16, 'Jack Miller', 'Drone', 1, 1500.00, '2025-09-26', '2025-09-28 10:00:00'),
(17, 'Kelly Green', 'Speakers', 2, 180.00, '2025-09-26', '2025-09-29 14:45:00'),
(18, 'Liam Scott', 'Charger', 3, 25.00, '2025-09-27', '2025-09-30 09:30:00'),
(19, 'Mia Turner', 'Desk Lamp', 1, 60.00, '2025-09-27', '2025-09-30 12:20:00'),
(20, 'Noah Brooks', 'Gaming Chair', 1, 350.00, '2025-09-27', '2025-09-30 18:10:00');
```
 
Every run gets exactly one row, inserted at the start and updated (never re-inserted) as the run progresses through its outcome.
 
---
 
## Pipeline: `Ingestion_Logging`
 
| Parameter | Default | Purpose |
|---|---|---|
| `schema` | `source` | target schema |
| `table` | `Orders` | target table |
| `backdate` | `2025-09-26` | manual override date; blank = trust the log table's checkpoint |

<img width="1435" height="492" alt="Ingestion Logging" src="https://github.com/user-attachments/assets/f60042d6-5a81-4e82-931b-5b6c3f7fcb6e" />
 
### 1. `last_cdc` (Lookup, `ls_database`/`ds_database`)
```sql
SELECT COALESCE(MAX(new_cdc_timestamp), '1900-01-01') as cdc_timestamp
FROM source.pipeline_run_log
WHERE table_name = '@{pipeline().parameters.table}'
  AND schema_name = '@{pipeline().parameters.schema}'
  AND status = 'Succeeded'
```
`firstRowOnly: true`. The `COALESCE` is what makes this self-seeding: a brand-new table with **no** successful run history yet gets `1900-01-01` automatically — no manual seed row needed in the log table before a table's first run.
 
### 2. `LogStart` (Script)
Inserts a `Started` row immediately, capturing which checkpoint this run is using:
```sql
INSERT INTO source.pipeline_run_log (pipeline_name, run_id, schema_name, table_name, start_time, status, cdc_timestamp)
VALUES ('@{pipeline().Pipeline}', '@{pipeline().RunId}', '@{pipeline().parameters.schema}', '@{pipeline().parameters.table}',
        SYSUTCDATETIME(), 'Started',
        '@{if(empty(pipeline().parameters.backdate), activity('last_cdc').output.firstRow.cdc_timestamp, pipeline().parameters.backdate)}')
```
Every attempt gets a row here, whether it later succeeds or fails — that's what makes this a true audit log rather than just a checkpoint.
 
### 3. `Totalresults` (Script)
Counts rows changed since the checkpoint — same logic as before, just now reading `activity('last_cdc').output.firstRow.cdc_timestamp` instead of a file-based lookup.
 
### 4. `If New Rows` (If Condition)
`@greater(activity('Totalresults').output.resultSets[0].rows[0].total_count, 0)`
 
- **True** → `Load` (Copy, unchanged — SQL → Parquet) → `LogSuccess` (Script):
```sql
  UPDATE source.pipeline_run_log
  SET status = 'Succeeded', end_time = SYSUTCDATETIME(),
      new_cdc_timestamp = (SELECT MAX(last_updated) FROM @{schema}.@{table}),
      rows_processed = @{activity('Totalresults').output.resultSets[0].rows[0].total_count}
  WHERE run_id = '@{pipeline().RunId}'
```
  Replaces the old `maxCDC` + `change_cdc` two-activity JSON-write with a single `UPDATE` — the new high-water mark is computed and stored in one round trip.
 
- **False** → `LogNoOp` (Script): closes the row out as `Succeeded` with `rows_processed = 0` and the checkpoint held steady (no new data means no advance) — so the next run's `last_cdc` lookup still finds a valid `Succeeded` row to build on.
---
 
## Pipeline: `Scheduled_Logging`
 
| Parameter | Default | Purpose |
|---|---|---|
| `schema` | `source` | forwarded to `Ingestion_Logging` |
| `table` | `Orders` | forwarded to `Ingestion_Logging` |
| `backdate` | *(none)* | forwarded; leave blank for scheduled runs, set only for manual recovery |
| `alertsUrl` | *(none)* | Logic App trigger URL, supplied at trigger/deployment time — never hardcoded |

<img width="632" height="347" alt="Scheduled Logging" src="https://github.com/user-attachments/assets/4cc6da0b-ae83-491b-bde4-61ba170d9bf2" />

1. **`Execute Pipeline 2`** → `Ingestion_Logging`, forwards all three parameters.
2. **`Alerts`** (Web, fires on `Execute Pipeline 2` **Failed**) — POSTs `pipeline_name`/`run_id` to `@pipeline().parameters.alertsUrl`. `secureInput: true` masks the resolved URL in run history.
3. **`LogFailure`** (Script, fires on `Execute Pipeline 2` **Failed**, in parallel with `Alerts` — not chained after it):
```sql
   UPDATE source.pipeline_run_log
   SET status = 'Failed', end_time = SYSUTCDATETIME(),
       error_message = '@{activity('Execute Pipeline 2').Error.Message}'
   WHERE run_id = '@{activity('Execute Pipeline 2').output.pipelineRunId}'
```
   Matches back to the exact log row `LogStart` created inside the failed child run, via `pipelineRunId`. Running this **parallel to `Alerts`, not dependent on it succeeding**, was a deliberate fix — if it depended on `Alerts` succeeding, a broken alert URL or a Logic App outage would leave the log row stuck at `Started` forever, silently breaking the whole audit trail for that run.
 
---
 
## Debugging notes / lessons learned building this
 
Two real bugs surfaced during testing, both worth remembering if this pattern gets reused elsewhere:
 
1. **Lookup output shape depends on `firstRowOnly`.** With `firstRowOnly: true`, the Lookup's output is `{ firstRow: {...} }` — **not** `{ value: [...] }`. Every downstream expression needs `activity('last_cdc').output.firstRow.cdc_timestamp`, not the array-style `.value[0].cdc_timestamp` used with `firstRowOnly: false`. Mixing the two throws `InvalidTemplate: property 'value' doesn't exist`.
2. **A failure-logging activity must not depend on the alert succeeding.** Chaining `LogFailure` after `Alerts` (Succeeded) means any hiccup in the alert delivery silently prevents the run from ever being marked `Failed`. `LogFailure` and `Alerts` need to be two independent branches off the same `Failed` condition.
---
 
## Verified behavior (from test runs)
 
A query against `source.pipeline_run_log` after several debug runs confirmed the full range of expected states:
 
| Scenario | Result |
|---|---|
| First-ever run, no prior log history | `cdc_timestamp` seeded to `1900-01-01` automatically via `COALESCE`, full table loaded, row closed `Succeeded` |
| Subsequent run, new rows present | Checkpoint correctly picked up from prior `Succeeded` row, only delta rows loaded, `rows_processed` populated |
| Run with no new data | `LogNoOp` path taken, `rows_processed = 0`, checkpoint held steady |
| Forced failure | Row closed out as `Failed` with a populated `error_message`, independent of whether the alert email itself succeeded |
 
```sql
SELECT * FROM source.pipeline_run_log ORDER BY log_id DESC;
```
is the standard way to review pipeline health going forward — no more digging through ADLS for a JSON checkpoint file or cross-referencing ADF's run history separately.

<img width="1662" height="511" alt="results" src="https://github.com/user-attachments/assets/f2c6e069-41a0-4c25-8882-b3c91d513eeb" />
 
## Notes / Things to double-check
- `Scheduled_Logging`'s `backdate` and `alertsUrl` parameters have **no default value** at all (rather than an explicit empty string). Confirm this behaves as expected on an unattended scheduled trigger run — if ADF requires an explicit value for parameters with no default on trigger-based execution, the trigger definition itself needs to pass `backdate: ""` and the real `alertsUrl` explicitly rather than relying on an implicit default.

---

# Ingestion_Multitables & Scheduled_Multitables — Multi-Table CDC Load with DB Logging

Extends the single-table `Ingestion_Logging`/`Scheduled_Logging` design to load **any number of tables** from one control-table-driven orchestrator, with no risk of one table's checkpoint clobbering another's — every table's incremental state lives in its own isolated rows in the log table.

## Architecture

```
Scheduled_Multitables (daily trigger)
  └─ LookForTables (Lookup: active tables from source.table_config)
       └─ ForEachTable (parallel, batchCount 4)
             └─ Execute Pipeline 3 ──▶ Ingestion_Multitables
                    ├─ On Success: done
                    └─ On Failure ─┬─▶ Alerts   (Web → Logic App email)
                                   └─▶ LogFailure (marks that table's run row Failed)

Ingestion_Multitables:
  last_cdc ──▶ LogStart ──▶ Totalresults ──▶ If New Rows
                                                 ├─ True  ─▶ Load ─▶ LogSuccess
                                                 └─ False ─▶ LogNoOp
```

---

## Supporting SQL objects

**Control table** — the list of tables to process each run:
```sql
CREATE TABLE source.table_config (
    config_id     INT IDENTITY(1,1) PRIMARY KEY,
    schema_name   VARCHAR(100) NOT NULL,
    table_name    VARCHAR(100) NOT NULL,
    is_active     BIT NOT NULL DEFAULT 1
);
```
Adding or removing a table from the nightly load is just an `INSERT`/`UPDATE` here — no pipeline redeploy needed.

**Run log** — one row per pipeline attempt, per table:
```sql
CREATE TABLE source.pipeline_run_log (
    log_id            INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name     VARCHAR(200) NOT NULL,
    run_id            VARCHAR(100) NOT NULL,
    schema_name       VARCHAR(100) NOT NULL,
    table_name        VARCHAR(100) NOT NULL,
    start_time        DATETIME2 NOT NULL,
    end_time          DATETIME2 NULL,
    status            VARCHAR(20) NOT NULL,      -- 'Started' / 'Succeeded' / 'Failed'
    cdc_timestamp     DATETIME2 NULL,             -- checkpoint used INTO this run
    new_cdc_timestamp DATETIME2 NULL,             -- checkpoint coming OUT of this run
    rows_processed    INT NULL,
    error_message     VARCHAR(MAX) NULL
);

CREATE INDEX ix_pipeline_run_log_lookup
    ON source.pipeline_run_log (table_name, schema_name, status);
```

---

## Pipeline: `Ingestion_Multitables`

Unchanged in shape from the single-table version — parameterized so the *same* pipeline handles every table, one call per table.

| Parameter | Default | Purpose |
|---|---|---|
| `schema_name` | `source` | which schema to load from |
| `table_name` | *(none)* | which table to load — supplied per call by the orchestrator |
| `backdate` | *(none)* | manual override date; blank = trust the log table's checkpoint |

<img width="1237" height="397" alt="Ingestion_Multitables" src="https://github.com/user-attachments/assets/6059b3db-2afb-4173-9db6-3a552b5af06c" />

1. **`last_cdc`** (Lookup) — `SELECT COALESCE(MAX(new_cdc_timestamp), '1900-01-01') ... WHERE table_name = @{table_name} AND schema_name = @{schema_name} AND status = 'Succeeded'`. Self-seeding: a table with no run history yet automatically starts from `1900-01-01` — no manual seed row required per table.
2. **`LogStart`** — inserts a `Started` row for this run, capturing the checkpoint it's using, scoped to this exact `schema_name`/`table_name` pair.
3. **`Totalresults`** — `SELECT count(*) as total_count FROM ... WHERE last_updated > <checkpoint>`. (The `as total_count` alias matters — `If New Rows` and `LogSuccess` both bind to that column name directly.)
4. **`If New Rows`**:
   - **True** → `Load` (Copy, SQL → Parquet) → `LogSuccess` (updates the row: `Succeeded`, new high-water mark via `MAX(last_updated)`, `rows_processed` from `Totalresults`)
   - **False** → `LogNoOp` (updates the row: `Succeeded`, `rows_processed = 0`, checkpoint held steady)

Because every read/write here is scoped by `table_name` + `schema_name`, running this pipeline for `Orders` never touches, reads, or overwrites `Customers`' checkpoint history (or vice versa) — each table accumulates its own independent lineage of rows in `pipeline_run_log`.

---

## Pipeline: `Scheduled_Multitables`

| Parameter | Default | Purpose |
|---|---|---|
| `schema_name` | `source` | schema passed through to every table's `Ingestion_Multitables` call |
| `backdate` | *(none)* | forwarded to every table; leave blank for scheduled runs |
| `alertsUrl` | *(none)* | Logic App trigger URL, supplied at trigger/deployment time — never hardcoded |

<img width="807" height="336" alt="Scheduled_Multitables" src="https://github.com/user-attachments/assets/fd860665-c53b-47ac-89ea-9be2e609c303" />

<img width="647" height="382" alt="ForEachTable" src="https://github.com/user-attachments/assets/8ea86c19-768e-437e-97e0-d8bf378a4525" />

### 1. `LookForTables` (Lookup)
```sql
SELECT table_name FROM @{pipeline().parameters.schema_name}.table_config WHERE is_active = 1
```
`firstRowOnly: false` — returns the full active table list.

### 2. `ForEachTable` (ForEach)
- **Items**: `@activity('LookForTables').output.value`
- **`isSequential: false`**, **`batchCount: 4`** — up to 4 tables load concurrently. Since every table's checkpoint read/write is independently scoped (see above), running several tables in parallel is safe; it just speeds up the nightly load.

Inside each iteration:

**`Execute Pipeline 3`** → `Ingestion_Multitables`, with:
- `schema_name = @pipeline().parameters.schema_name`
- `table_name = @item().table_name`
- `backdate = @pipeline().parameters.backdate`

**`Alerts`** (Web, fires on `Execute Pipeline 3` **Failed**):
- URL: `@pipeline().parameters.alertsUrl` (never a hardcoded SAS URL in the JSON)
- Body:
  ```json
  {
      "pipeline_name": "@{pipeline().Pipeline}",
      "run_id": "@{activity('Execute Pipeline 3').output.pipelineRunId}",
      "schema_name": "@{pipeline().parameters.schema_name}",
      "table_name": "@{item().table_name}"
  }
  ```
  Two details that matter here: the JSON keys (`schema_name`, `table_name`) must match the Logic App trigger's schema exactly, or those fields render blank in the email even though the request succeeds; and `run_id` uses `activity('Execute Pipeline 3').output.pipelineRunId` — the **child** `Ingestion_Multitables` run's own ID — rather than the parent orchestrator's `pipeline().RunId`, so the value in the email actually matches the `run_id` stored in `pipeline_run_log` for that failure and can be looked up directly.

**`LogFailure`** (Script, fires on `Execute Pipeline 3` **Failed**, in **parallel** with `Alerts` — not chained after it):
```sql
UPDATE source.pipeline_run_log
SET status = 'Failed', end_time = SYSUTCDATETIME(),
    error_message = '@{activity('Execute Pipeline 3').Error.Message}'
WHERE run_id = '@{activity('Execute Pipeline 3').output.pipelineRunId}'
```
Running independently of `Alerts` is deliberate: if the alert email itself fails to send (bad URL, Logic App outage), the log row still gets correctly closed out as `Failed` rather than staying stuck at `Started` forever.

---

## Why multi-table doesn't cause cross-table data loss

Every checkpoint read (`last_cdc`) and write (`LogStart`, `LogSuccess`, `LogNoOp`) filters explicitly on `table_name` + `schema_name`. Adding a 3rd, 10th, or 50th table to `table_config` never touches existing rows for other tables — each one just gets its own new rows in `pipeline_run_log`. The only real risk is the **same table** being run twice concurrently (e.g. a manual backfill overlapping the scheduled run) — that's a concurrency concern to manage via the orchestrator's Concurrency setting or an in-pipeline "already running" guard, not something the multi-table design itself introduces.

## Verifying it end to end
```sql
SELECT * FROM source.pipeline_run_log ORDER BY log_id DESC;
```
After a run, confirm:
- One row per active table in `table_config`, each closing out `Succeeded` or `Failed` independently.
- `rows_processed` and `new_cdc_timestamp` populated correctly per table.
- A forced failure on one table produces a `Failed` row **and** a correctly-detailed alert email (with `schema_name`/`table_name` populated, not blank), while every other table in the same run still completes normally.

## Notes
- Add a new table to load nightly by inserting a row into `source.table_config` — no ADF changes needed.
- To pause a table without removing it, set `is_active = 0` in `table_config` rather than deleting its row (keeps its history intact in `pipeline_run_log`).
- If tables ever need to span multiple schemas in a single run, `LookForTables` would need to also select `schema_name` per row (instead of relying on the single top-level `schema_name` parameter) and `Execute Pipeline 3` would map `schema_name = @item().schema_name`.
