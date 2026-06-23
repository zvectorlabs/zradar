# zradar Architecture Diagrams

Six diagrams covering component structure, write path, read path, and background jobs.

---

## 1. Component Overview

High-level crate layers and primary data-flow edges.

```mermaid
flowchart TD
    subgraph EXT["External"]
        SDK["OTLP SDK / Agent\nTraces · Metrics · Logs"]
        ADM["Admin Client\nHTTP REST"]
    end

    subgraph BIN["zradar-server  (binary)"]
        GRPC["gRPC  :4317"]
        HTTP["HTTP  :8081"]
    end

    subgraph SVC["Services"]
        OPTEL["api-optel\nIngestion · Guards · Conversion"]
        API["api\nQuery · Retention · Policy · Audit"]
    end

    subgraph CORE["Core"]
        PAR["zradar-parquet\nWrite buffer · Parquet I/O · Caching · FileMover"]
        RET["zradar-retention\nQuery enforcement · Cleanup"]
        POL["zradar-policy\nQuota · Usage tracking"]
        WALC["zradar-wal  (optional)\nDurability"]
    end

    subgraph PLG["Plugins"]
        PGP["zradar-plugin-postgres\nAll repository impls"]
        S3P["zradar-plugin-s3\nS3BlockStorage"]
    end

    subgraph STG["Storage"]
        PG[("PostgreSQL\nControl plane")]
        DSK[("Local Disk\nParquet files")]
        S3[("Amazon S3\nParquet files")]
    end

    SDK --> GRPC --> OPTEL
    ADM --> HTTP --> API

    OPTEL -->|write| PAR
    OPTEL -.->|wal.enabled| WALC --> PAR

    API --> RET & POL & PAR

    PAR --> PGP & DSK
    PAR --> S3P

    PGP --> PG
    S3P --> S3
    DSK -.->|FileMover| S3
```

---

## 2. Write Path Architecture

Data flow from OTLP ingestion through the guard chain, write buffer, and on to durable storage.

```mermaid
flowchart TD
    CLIENT(["OTLP SDK / Agent"])

    subgraph GRPC_SRV["gRPC Server  :4317  —  api-optel"]
        OT["OtlpTrace / Metrics / LogsService"]

        subgraph GUARDS["Guard Chain  (in order)"]
            direction TB
            G1["① Authenticator\ntoken → RequestContext"]
            G2["② CircuitBreaker\ndisk · memory · queue thresholds"]
            G3["③ PolicyEnforcer\nquota · byte-rate check"]
            G4["④ ProjectRateLimiter\ntoken bucket  via SettingsRepository"]
            G5["⑤ OtlpConverter\nprotobuf → Span / Metric / Log"]
            G1 --> G2 --> G3 --> G4 --> G5
        end
    end

    TW{{"TelemetryWriter\ntrait boundary"}}

    subgraph BUFFERED["Buffered mode  (default)"]
        direction LR
        WB["WriteBuffer\nDashMap keyed by\ntenant · project · signal · hour"]
        FW["FlushWorker\ntimer + size triggers"]
        WB --> FW
    end

    subgraph DURABLE["WAL mode  (wal.enabled = true)"]
        direction LR
        WT["WalTelemetryWriter\nwraps TelemetryWriter slot"]
        WAL["Wal  append-only segments"]
        WF["WalFlusher"]
        WJ["WalJanitor  prunes old segments"]
        WT --> WAL --> WF
    end

    PFW["ParquetFileWriter\nSpan/Metric/Log → Arrow RecordBatch → .parquet"]

    PG[("PostgreSQL\nfile_list · stream_stats")]
    DISK[("Local Disk\n.parquet files")]

    subgraph BGWRITE["Background Jobs"]
        FM["FileMover\nmove local → S3 after delay"]
        COMP["Compactor\nmerge small files per bucket"]
        CJ["CleanupJob\ndelete expired files"]
    end

    S3[("Amazon S3\n.parquet files")]

    CLIENT -->|"ExportSpans / ExportMetrics / ExportLogs"| OT
    OT --> G1
    G5 --> TW

    TW -->|"write_buffer = true"| WB
    TW -.->|"wal.enabled = true"| WT

    FW --> PFW
    WF --> PFW

    PFW -->|".parquet file"| DISK
    PFW -->|"register_file + upsert_stream_stats"| PG

    DISK --> FM --> S3
    DISK --> COMP
    COMP -->|"merged file"| DISK
    COMP -.->|"soft-delete originals"| PG
    FM -.->|"update location = s3"| PG

    PG -.->|"file queries"| FM & COMP & CJ
    CJ -->|"delete expired"| DISK & S3
    CJ -.->|"hard-delete entries"| PG
```

---

## 3. Read Path Architecture

Data flow from an admin HTTP query through the guard chain, file discovery, cache layers, and DataFusion.

```mermaid
flowchart TD
    CLIENT(["Admin Client"])

    subgraph HTTP_SRV["HTTP Server  :8081  —  api"]
        QH["QueryHandler\nGET /api/v1/traces · /spans · /logs · /metrics · /analytics"]

        subgraph QGUARDS["Guard Chain  (in order)"]
            direction TB
            Q1["① AdminAuthorizer\nbearer token → AdminAuth + capability_keys"]
            Q2["② PolicyEnforcer\nread quota check  →  UsageTracker.record_query"]
            Q3["③ QueryEnforcer\nclamp time_range to retention window"]
            Q1 --> Q2 --> Q3
        end
    end

    FL["FileListRepository\nPostgreSQL  file_list\nreturns FileListEntry list"]

    subgraph LOCAL_PATH["Local file path"]
        LDISK[("Local Disk\n.parquet")]
    end

    subgraph S3_PATH["S3 file path"]
        MC["MemoryCache\nLRU in-process RecordBatch cache"]
        DC["DiskCache\nLRU + TTL  local copy of S3 files"]
        S3BS["S3BlockStorage\ndownload on miss"]
        S3B[("Amazon S3")]
        MC -->|"cache miss"| DC
        DC -->|"disk miss"| S3BS --> S3B
        S3B -.->|"populate"| DC
        DC -.->|"populate"| MC
    end

    PFR["ParquetFileReader\nDataFusion ListingTable\nparallel row-group scan across all files"]

    RESP(["PaginatedResponse\nTraceSummary · Span · LogRecord · Metric"])

    RET_CFG["RetentionConfigStore\nloads per-org / per-project policy\n(zradar-retention)"]

    CLIENT -->|"GET /api/v1/traces?start=…&end=…"| QH
    QH --> Q1
    Q3 --> FL
    Q3 --- RET_CFG

    FL -->|"location = local"| LDISK --> PFR
    FL -->|"location = s3"| MC --> PFR
    LDISK -.->|"populate"| MC

    PFR -->|"Arrow RecordBatch\n→ domain structs"| RESP
```

---

## 4. Background Jobs

All background workers, their triggers, and storage interactions.

```mermaid
flowchart TD
    subgraph TRIGGERS["Triggers"]
        TIMER["Timer\n(configurable intervals)"]
        SIZE["Size threshold\n(write_buffer_size_bytes)"]
        STARTUP["Server startup"]
        INTERVAL["Configurable interval\n(storage_snapshot_interval_secs)"]
        POLL["WAL polling"]
    end

    subgraph JOBS["Background Workers"]
        FW["FlushWorker\nDrains WriteBuffer slots to Parquet"]
        FM["FileMover\nMoves local Parquet files to S3"]
        COMP["Compactor\nMerges small files per\ntenant/project/signal/date bucket"]
        CJ["CleanupJob\nDeletes files older than retention_days"]
        SUDJ["StorageUsageDailyJob\nSnapshots compressed bytes per project"]
        WFLUSH["WalFlusher\nDrains WAL records to Parquet"]
        WJAN["WalJanitor\nRemoves old WAL segments"]
        WREP["WalReplayer\nReplays unprocessed WAL on startup"]
        PSR["PolicyStore Refresh\nReloads quota policies every 30s"]
    end

    subgraph READS["Reads From"]
        WB["WriteBuffer\n(in-memory)"]
        WAL_SEG["WAL segments\n(local disk)"]
        LOCAL_FILES["Local .parquet files"]
        S3_FILES["S3 .parquet files"]
        PG_FL["PostgreSQL\nfile_list"]
        PG_POL["PostgreSQL\npolicies table"]
    end

    subgraph WRITES["Writes To"]
        NEW_PAR["New .parquet file\n(local disk)"]
        S3_UPLOAD["S3 bucket"]
        PG_UPD["PostgreSQL\nfile_list updates"]
        PG_USAGE["PostgreSQL\nstorage_cleanup_daily"]
        PG_MERGED["PostgreSQL\nfile_list (merged entry)"]
    end

    TIMER --> FW & FM & COMP & CJ & WJAN & PSR
    SIZE  --> FW
    INTERVAL --> SUDJ
    POLL  --> WFLUSH
    STARTUP --> WREP

    WB --> FW
    FW --> NEW_PAR & PG_UPD

    LOCAL_FILES --> FM
    FM --> S3_UPLOAD & PG_UPD

    PG_FL --> COMP
    LOCAL_FILES --> COMP
    COMP --> NEW_PAR & PG_MERGED

    PG_FL --> CJ
    LOCAL_FILES --> CJ
    S3_FILES --> CJ
    CJ --> PG_UPD

    PG_FL --> SUDJ
    SUDJ --> PG_USAGE

    WAL_SEG --> WFLUSH & WJAN & WREP
    WFLUSH --> NEW_PAR & PG_UPD
    WREP --> NEW_PAR

    PG_POL --> PSR
```

---

## 5. Write Path — Sequence Diagram

Step-by-step call order from OTLP export to durable Parquet file.

```mermaid
sequenceDiagram
    autonumber
    participant Client as OTLP Client
    participant OtlpSvc as OtlpService<br/>(api-optel)
    participant Auth as Authenticator
    participant CB as CircuitBreaker
    participant PE as PolicyEnforcer
    participant RL as ProjectRateLimiter<br/>+ SettingsRepository
    participant UT as UsageTracker
    participant Conv as OtlpConverter
    participant TW as TelemetryWriter<br/>(trait)
    participant WB as WriteBuffer
    participant FW as FlushWorker
    participant PFW as ParquetFileWriter
    participant FL as FileListRepository<br/>(PostgreSQL)
    participant FM as FileMover
    participant S3 as S3BlockStorage

    Client->>OtlpSvc: ExportSpans / ExportMetrics / ExportLogs (gRPC)

    OtlpSvc->>Auth: authenticate(bearer_token)
    Auth-->>OtlpSvc: RequestContext { tenant_id, project_id }

    OtlpSvc->>CB: check_status()
    alt threshold exceeded (disk / memory / queue)
        CB-->>OtlpSvc: RESOURCE_EXHAUSTED
        OtlpSvc-->>Client: gRPC RESOURCE_EXHAUSTED
    else OK
        CB-->>OtlpSvc: OK
    end

    OtlpSvc->>PE: check_ingest(tenant_id, project_id, signal, bytes)
    alt hard block threshold crossed
        PE-->>OtlpSvc: Decision::Block
        OtlpSvc-->>Client: gRPC RESOURCE_EXHAUSTED
    else allowed
        PE-->>OtlpSvc: Decision::Allow
        PE->>UT: record_write(WriteSample)
    end

    OtlpSvc->>RL: check_and_record(project_id, limit_per_second, records)
    alt project rate limit exceeded
        RL-->>OtlpSvc: Denied
        OtlpSvc-->>Client: gRPC RESOURCE_EXHAUSTED
    else allowed
        RL-->>OtlpSvc: Allowed
    end

    OtlpSvc->>Conv: convert(ResourceSpans / ResourceMetrics / ResourceLogs)
    Conv-->>OtlpSvc: Vec<Span> / Vec<Metric> / Vec<LogRecord>

    OtlpSvc->>TW: insert_spans / insert_metrics / insert_logs(records)
    Note over TW: Concrete type is ParquetTelemetryWriter (buffered/direct)<br/>or WalTelemetryWriter (when wal.enabled = true)

    alt write_buffer enabled (normal mode)
        TW->>WB: push(tenant_id, project_id, signal_type, stream_name, records)
        Note over WB: DashMap keyed by (tenant, project, signal, stream, hour)
        loop Every flush_interval_secs OR slot size > write_buffer_size_bytes
            FW->>WB: drain eligible slots
            WB-->>FW: Vec<RecordSlot>
            FW->>PFW: write_spans / write_metrics / write_logs(records)
        end
    else WAL mode (wal.enabled = true)
        TW->>TW: Wal.append(record) + handle.durable() — fsync before returning
        Note over TW: gRPC OK is held until WAL fsync completes
        loop WalFlusher polls WAL
            TW->>PFW: flush WAL records to Parquet
        end
    end

    PFW->>PFW: Arrow RecordBatch → .par temp file → rename → .parquet
    PFW->>FL: register_file(FileListEntry { location=local, min_ts, max_ts, … })
    PFW->>FL: upsert_stream_stats(tenant_id, project_id, signal_type, stream_name)
    FL-->>PFW: file_id

    OtlpSvc-->>Client: gRPC OK

    loop Every file_push_interval_secs (background)
        FM->>FL: query_files(location=local, deleted=false)
        FL-->>FM: Vec<FileListEntry>
        FM->>FM: filter client-side: created_at ≤ now − file_push_delay_secs
        FM->>S3: upload(file_path, parquet_bytes)
        S3-->>FM: s3_url
        FM->>FL: update_location(file_id, location=s3, file_path=s3_url)
    end
```

---

## 6. Read Path — Sequence Diagram

Step-by-step call order from admin HTTP request to paginated response.

```mermaid
sequenceDiagram
    autonumber
    participant Client as Admin Client
    participant Handler as QueryHandler<br/>(api)
    participant AAuth as AdminAuthorizer
    participant PE as PolicyEnforcer<br/>(zradar-policy)
    participant UT as UsageTracker
    participant QE as QueryEnforcer<br/>(zradar-retention)
    participant FL as FileListRepository<br/>(PostgreSQL)
    participant MC as MemoryCache
    participant DC as DiskCache
    participant S3 as S3BlockStorage
    participant Reader as ParquetFileReader<br/>(DataFusion)

    Client->>Handler: GET /api/v1/traces?start_time=…&end_time=…&service_name=…

    Handler->>AAuth: authorize(request_headers)
    AAuth-->>Handler: AdminAuth { context: RequestContext, capability_keys }

    Handler->>PE: check_query(tenant_id, project_id, signal)
    alt read quota exceeded
        PE-->>Handler: Decision::Block
        Handler-->>Client: 429 Too Many Requests
    else allowed
        PE-->>Handler: Decision::Allow
    end

    Handler->>QE: enforce(tenant_id, project_id, time_range)
    Note over QE: Loads RetentionPolicy from PostgreSQL<br/>Clamps time_range.start to max(start, now − retention_days)
    QE-->>Handler: clamped TimeRange

    Handler->>FL: query_files(tenant_id, project_id, signal_type, clamped_time_range)
    FL-->>Handler: Vec<FileListEntry> { file_path, location, min_ts, max_ts }

    loop For each FileListEntry
        alt location = local
            Handler->>Reader: resolve(local_file_path)
        else location = s3
            Handler->>MC: get(s3_key)
            alt memory cache hit
                MC-->>Handler: RecordBatch (in-process)
            else memory cache miss
                Handler->>DC: get(s3_key)
                alt disk cache hit
                    DC-->>Handler: local_cache_path
                else disk cache miss
                    DC->>S3: download(s3_key)
                    S3-->>DC: parquet_bytes
                    DC->>DC: write to cache_dir, update LRU index
                    DC-->>Handler: local_cache_path
                end
            end
            Handler->>Reader: resolve(local_cache_path)
        end
    end

    Reader->>Reader: Register all resolved paths as single DataFusion ListingTable
    Reader->>Reader: Execute SQL with filter pushdown<br/>(time_range, service_name, status, trace_id, …)
    Reader->>Reader: Parallel row-group scan across all files
    Reader-->>Handler: Vec<Arrow RecordBatch>

    Handler->>UT: record_query(QuerySample { tenant_id, project_id, signal, … })
    Handler->>Handler: Map RecordBatch → domain structs<br/>(TraceSummary / Span / LogRecord / Metric)
    Handler-->>Client: PaginatedResponse<T> { data, total, limit, offset }
```

---

## Summary

| Path | Entry | Guard order | Storage |
|------|-------|------------|---------|
| **Write** | gRPC `:4317` | Authenticator → CircuitBreaker → PolicyEnforcer → ProjectRateLimiter | WriteBuffer → FlushWorker → Parquet (local) → S3 |
| **Read** | HTTP `:8081` | AdminAuthorizer → PolicyEnforcer → QueryEnforcer | PostgreSQL file_list → MemoryCache / DiskCache → DataFusion |
| **Background** | Internal timers | — | FileMover, Compactor, CleanupJob, WalFlusher |

**PostgreSQL** holds only metadata (file registry, settings, policies, audit). All telemetry data lives in Parquet files organized as `tenant/project/signal_type/YYYY/MM/DD/HH/*.parquet`.
