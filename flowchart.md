# Cribl Framework — Flowcharts

## End-to-End Onboarding Flow

```mermaid
flowchart TD
    CLIENT([Client]) --> PORTAL["/portal — Onboarding Form<br>LAN ID, Name, APM ID, App Name,<br>Region, Log Dest, Log Type, Groups"]
    PORTAL --> ES_INDEX["POST /portal/api/submit<br>→ Index to Elasticsearch<br>→ Returns REQ-YYYYMMDD-XXXXXXXX"]
    ES_INDEX --> PENDING["ES Document<br>status = pending"]

    PLATFORM([Platform Team]) --> PUSHER["/cribl/app — Cribl Pusher<br>Paste Request ID + App details"]
    PUSHER --> DRYRUN{Dry Run?}
    DRYRUN -- Yes --> PREVIEW["Preview diff<br>No writes"]
    DRYRUN -- No --> EXECUTE

    subgraph EXECUTE[Execute]
        direction TB
        DEST["POST destinations<br>to Cribl"] --> ROUTES["PATCH routes<br>to Cribl"]
        ROUTES --> STATUS["Auto-update ES<br>status = done"]
    end

    EXECUTE --> DONE["ES Document<br>status = done"]
    PREVIEW --> PUSHER
```

---

## Portal Submit Flow

```mermaid
flowchart TD
    START([Client opens /portal]) --> FORM["Fill form:<br>LAN ID, Name, APM ID, App Name,<br>Region, Log Dest, Log Type, Groups"]
    FORM --> VALIDATE{Client-side<br>validation}
    VALIDATE -- Errors --> SHOW_ERR[Show error list]
    SHOW_ERR --> FORM
    VALIDATE -- OK --> POST["POST /portal/api/submit<br>JSON body"]

    POST --> SRV_VAL{Server-side<br>validation}
    SRV_VAL -- Errors --> RET_400["400 + errors JSON"]
    SRV_VAL -- OK --> LOAD_CFG[Load config.json]
    LOAD_CFG --> BUILD_DOC["Build ES document:<br>@timestamp, request_id,<br>lan_id, requester_name,<br>apmid, appname, region,<br>log_destinations, log_types,<br>entitlement_groups,<br>status = pending"]
    BUILD_DOC --> ES_WRITE["POST to ES<br>datastream.elk_url / index"]
    ES_WRITE --> ES_OK{Success?}
    ES_OK -- No --> RET_500["500 + error"]
    ES_OK -- Yes --> RET_200["200 + request_id<br>REQ-YYYYMMDD-XXXXXXXX"]
    RET_200 --> SUCCESS([Show success + Request ID])
```

---

## Cribl Pusher Flow (app.py /cribl/api/run-pusher)

```mermaid
flowchart TD
    START([POST /cribl/api/run-pusher]) --> PARSE["Parse form data:<br>workspace, worker_groups,<br>region, mode, request_id"]
    PARSE --> FVAL{Validation}
    FVAL -- Errors --> RET_400["400 + errors"]
    FVAL -- OK --> LOAD_CFG[Load config.json]
    LOAD_CFG --> LOOP

    subgraph LOOP["For each worker group"]
        direction TB
        BUILD_CMD["Build cribl-pusher.py<br>subprocess command"] --> RUN["Run subprocess<br>capture stdout + exit code"]
        RUN --> COLLECT["Append output"]
    end

    LOOP --> RC{Exit code = 0<br>and not dry_run<br>and request_id?}
    RC -- Yes --> UPDATE["portal_update_status_internal<br>→ ES _update_by_query<br>status = done"]
    RC -- No --> SKIP_UPDATE[Skip portal update]
    UPDATE --> RESPOND
    SKIP_UPDATE --> RESPOND["Return JSON:<br>output, returncode,<br>commands, portal_update"]
```

---

## rode_rm.py Flow

```mermaid
flowchart TD
    START([Start]) --> ARGS[Parse CLI arguments]

    ARGS --> APPS{--from-file?}
    APPS -- Yes --> FILE[read_apps_from_file<br>appids.txt or --appfile]
    APPS -- No  --> SINGLE["apps = [(app_name, apmid)]"]
    FILE    --> VAL
    SINGLE  --> VAL

    subgraph VAL[Validate]
        direction TB
        V2[ELK Nonprod URL + creds required]
        V4[ELK Prod URL + creds required]
        V7[Workspace required]
        V1[All skipped if --skip-elk / --skip-cribl]
    end

    VAL --> VALERR{Errors?}
    VALERR -- Yes --> DIE1([Exit with error])
    VALERR -- No  --> TMPL

    TMPL["save_templates (always runs)<br>Write 4 JSON files per app →<br>ops_rm_r_templates_output/"] --> CONFIRM

    CONFIRM{"--yes or<br>--dry-run?"} -- No  --> PROMPT[Prompt: type YES]
    CONFIRM -- Yes --> SESSIONS
    PROMPT --> PCONF{Confirmed?}
    PCONF -- No  --> DIE2([Exit: aborted])
    PCONF -- Yes --> SESSIONS

    SESSIONS["Build ELK sessions<br>Nonprod + Prod"] --> ORDER

    ORDER{--order} -- elk-first   --> ELK
    ORDER          -- cribl-first --> CRIBL2

    subgraph ELK[run_elk]
        direction TB
        ES{--skip-elk?}
        ES -- Yes --> ELKSKIP([ELK skipped])
        ES -- No  --> ELKLOOP

        subgraph ELKLOOP["For each app x 4 configs"]
            direction TB
            ENVCHECK{"environment<br>== prod?"}
            ENVCHECK -- Yes --> USEPROD[Prod URL + session]
            ENVCHECK -- No  --> USENP[Nonprod URL + session]
            USEPROD --> GEN
            USENP   --> GEN
            GEN[generate_templates<br>PUSER + USER<br>role + role_mapping] --> DR1{--dry-run?}
            DR1 -- Yes --> DRL1[Log DRY-RUN]
            DR1 -- No  --> ELPUT[PUT role + role_mapping<br>x4 per app]
            ELPUT --> PUTRES{200/201?}
            PUTRES -- Yes --> PUTOK[Log OK]
            PUTRES -- No  --> PUTERR[Log ERR]
        end
    end

    subgraph CRIBL[run_cribl]
        direction TB
        CS{--skip-cribl?}
        CS -- Yes --> CRSKIP([Cribl skipped])
        CS -- No  --> LOADCFG[Load config + workspace]
        LOADCFG --> AUTH{token?}
        AUTH -- No  --> LOGIN[POST /api/v1/auth/login]
        AUTH -- Yes --> GR
        LOGIN --> GR
        GR["GET /routes + GET /outputs"] --> SMIN{"total_routes<br>>= min_routes?"}
        SMIN -- No  --> DIED3([Exit: safety check])
        SMIN -- Yes --> BUILD["Build new routes + dests<br>Skip duplicates"]
        BUILD --> SAFTER{"total_after<br>>= total_before?"}
        SAFTER -- No  --> DIED4([Exit: safety check])
        SAFTER -- Yes --> DR2{--dry-run?}
        DR2 -- Yes --> DRL2[Log DRY-RUN]
        DR2 -- No  --> SNAP[Write snapshot] --> POST_D[POST new dests]
        POST_D --> PATCH["PATCH routes"]
        PATCH --> PLOG[Log OK + rollback path]
    end

    ELK    --> CRIBL
    CRIBL2 --> CRIBL3[run_cribl]
    CRIBL3 --> ELK2[run_elk]
    ELK2   --> DONE

    CRIBL  --> DONE([Done])
```

---

## Admin Status Update Flow

```mermaid
flowchart TD
    ADMIN([Admin opens /portal/admin/update-status]) --> FORM["Enter Request ID,<br>select new Status,<br>enter Admin Secret"]
    FORM --> POST["POST with<br>X-Admin-Secret header"]
    POST --> AUTH{Secret matches<br>config admin_secret?}
    AUTH -- No --> DENY["403 Unauthorized"]
    AUTH -- Yes --> QUERY["ES _update_by_query<br>term: request_id<br>script: set status"]
    QUERY --> FOUND{Documents<br>updated > 0?}
    FOUND -- No --> NOT_FOUND["404 Request ID not found"]
    FOUND -- Yes --> OK["200 + updated count"]
```

---

## Summary Table

| Step | Always runs | Description |
|------|:-----------:|-------------|
| Portal submit | on request | Client fills form, doc indexed to ES with `status=pending` |
| Parse args (CLI) | yes | Single app or bulk file |
| Validate | yes | URLs, credentials, workspace |
| Save ELK templates | yes | 4 JSON files per app in `ops_rm_r_templates_output/` |
| Confirm | yes | Auto-confirmed with `--yes` or `--dry-run` |
| `run_elk` | if not `--skip-elk` | PUT roles + role-mappings to correct cluster |
| `run_cribl` | if not `--skip-cribl` | GET → plan → snapshot → POST dests → PATCH routes |
| Auto-update status | if request_id set | ES `_update_by_query` sets `status=done` |

## ELK Environment Routing

| Config block | Cluster |
|---|---|
| `test` onshore + offshore | `--elk-url` nonprod |
| `prod` onshore + offshore | `--elk-url-prod` prod |

## Cribl Safety Gates

| Gate | Prevents |
|---|---|
| `total_before >= min_routes` | Running against an empty/broken config |
| `total_after >= total_before` | Accidentally deleting existing routes |
| Duplicate name/filter check | Adding the same route twice |
| Snapshot written before write | Provides rollback point |
