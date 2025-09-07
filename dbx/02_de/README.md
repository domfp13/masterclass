# Oracle → Databricks CDC Overview

This document outlines licensing considerations and CDC (Change Data Capture) support when integrating Oracle with other targets such as Azure Databricks or via Azure Data Factory (ADF), as well as the possibility and complexity of building CDC logic directly in PySpark.

---

## 1. Oracle GoldenGate Free vs. Commercial/OCI

- **Oracle GoldenGate Free** is provided at no cost and is well-suited for learning, development, small-scale production, or testing environments with Oracle databases **≤ 20 GB** (including container DBs with all PDBs).
- It **only supports replication between Oracle databases** and **does not integrate with licensed GoldenGate or third-party tools**.
- It lacks advanced features such as Active Data Guard, XStream, downstream capture, and does not include Oracle Support entitlements (support is via community forums only).

### Summary:
- **Oracle → Oracle** replication using GoldenGate Free = **Free**
- **Oracle → non-Oracle** target (e.g., Databricks, Kafka) requires **paid GoldenGate modules** or OCI GoldenGate.

---

## 2. Azure Data Factory: CDC Capabilities & Limitations

- **ADF supports CDC via**:
  - **Native CDC** in mapping data flows—but **only** for sources that support it (e.g., Azure SQL, Cosmos DB), **not Oracle**
  - **Auto-increment extraction** using timestamp or numeric columns (not true CDC)
- Oracle has **no native CDC in ADF**, meaning ADF cannot detect redo-log level changes. You must rely on incremental columns or full snapshots—thus **ADF doesn’t support full CDC for Oracle** out of the box.

---

## 3. Implementing CDC Logic Directly in PySpark

It is indeed possible—and many teams do—to build CDC pipelines using PySpark in Databricks. It’s entirely free and flexible, but you’ll need to manage several non-trivial challenges yourself.

###  Patterns in PySpark-based CDC

####  A) Batch Incremental Load via JDBC + MERGE

1. Read only records changed since your last watermark (e.g., `last_update_ts`)
2. Append to a **bronze** Delta table
3. Use `MERGE INTO` on a **silver** Delta table to apply changes (upsert/delete)

####  B) File-based Streaming + Auto Loader + MERGE

1. Land change files (e.g., with `op` = insert/update/delete indicators) into a storage path
2. Use **Auto Loader** to ingest incrementally
3. Use **structured streaming + `foreachBatch` + MERGE** to apply changes to a Delta table

**Pros**:
- Full control, adaptable, no additional licensing
- Strong integration with Delta Lake features like Change Data Feed (CDF)

**Cons / Challenges**:
- You must handle ordering, deduplication, deletes, late/out-of-order data
- JDBC for Oracle isn’t log-based—either rely on `last_update_ts` or external mechanisms
- Need proper partitioning & tuning for performance (e.g., `fetchsize`, `numPartitions`)
- Manage idempotency (use checkpoints + MERGE idempotently)
- Handle schema changes manually or via DLT
- Monitor streaming jobs and handle backfills and recovery

---

## 4. Quick Comparison Table

| Scenario                                      | Licensing / Cost                     | Notes                                                                                       |
|-----------------------------------------------|--------------------------------------|---------------------------------------------------------------------------------------------|
| Oracle → Oracle (GoldenGate Free)              | **Free**                            | Limited to ≤ 20 GB, Oracle-only targets, limited features and community support only        |
| Oracle → non-Oracle target (e.g., Databricks)  | **Paid GoldenGate**                 | Requires licensed modules or OCI GoldenGate                                                  |
| Oracle → Databricks via ADF                   | **ADF costs** without Oracle CDC    | Only supports batch/incremental loads based on columns—not log-based CDC                    |
| CDC via PySpark (Batch/File + MERGE)           | **Free (run-time costs only)**      | Fully custom; manage tricky parts manually (ordering, deletes, batching, schema drift)      |

---

###  Recommendations

- For **initial exploration or smaller use cases**, try building a basic PySpark CDC flow with JDBC or file ingestion and Delta MERGE.
- For **production-grade, rock-solid CDC**, evaluate licensed GoldenGate or tool-based approaches (e.g., Debezium + Kafka, managed CDC).
- **ADF** can orchestrate movement but doesn’t solve Oracle CDC by itself.
- Once you’re stable, layer in **Delta CDF** to allow downstream users to consume fine-grained change data efficiently.

---

Let me know if you'd like a fully annotated example notebook or `docker-compose.yml` + DLT starter script to implement the PySpark CDC pipeline!