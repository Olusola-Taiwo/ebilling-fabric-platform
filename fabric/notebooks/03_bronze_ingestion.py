# %% Cell 1: Control configuration
# ============================================================
# BRONZE CONTROL CONFIGURATION
# ============================================================

BRONZE_ROOT = "Tables/bronze"

CONTROL_PATH = f"{BRONZE_ROOT}/_control"
AUDIT_PATH = f"{BRONZE_ROOT}/_audit"

CONTROL_TABLE = "bronze_ingestion_control"
AUDIT_TABLE = "bronze_ingestion_audit"

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# %% Cell 2: Create ingestion control table
# ============================================================
# INGESTION CONTROL TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.{CONTROL_TABLE}
(
    batch_id STRING,
    source_system STRING,
    source_object STRING,
    source_path STRING,
    ingestion_timestamp TIMESTAMP,
    status STRING,
    record_count BIGINT,
    error_message STRING
)
USING DELTA
LOCATION '{CONTROL_PATH}'
""")

# %% Cell 3: Create audit table
# ============================================================
# AUDIT TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.{AUDIT_TABLE}
(
    batch_id STRING,
    source_system STRING,
    source_object STRING,
    ingestion_timestamp TIMESTAMP,
    records_read BIGINT,
    records_written BIGINT,
    records_rejected BIGINT,
    status STRING,
    error_message STRING
)
USING DELTA
LOCATION '{AUDIT_PATH}'
""")

# %% Cell 4: Generate a batch ID
# ============================================================
# BATCH ID
# ============================================================

from datetime import datetime
import uuid

RUN_TS = datetime.now()

BATCH_ID = (
    f"batch_"
    f"{RUN_TS.strftime('%Y%m%d%H%M%S')}_"
    f"{uuid.uuid4().hex[:8]}"
)

print("BATCH_ID:", BATCH_ID)

# %% Cell 5: Idempotency check
# ============================================================
# IDEMPOTENCY CHECK
# ============================================================

def batch_already_processed(
    source_system,
    source_object,
    source_path
):

    result = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM bronze.{CONTROL_TABLE}
        WHERE source_system = '{source_system}'
          AND source_object = '{source_object}'
          AND source_path = '{source_path}'
          AND status = 'SUCCESS'
    """).collect()[0]["cnt"]

    return result > 0

# %% Cell 6: Generic Bronze ingestion function
# ============================================================
# GENERIC BRONZE INGESTION
# ============================================================

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def ingest_to_bronze(
    source_path,
    bronze_path,
    source_system,
    source_object,
    table_name
):

    print("=" * 70)
    print(f"Processing: {source_system} / {source_object}")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. Idempotency check
    # --------------------------------------------------------

    if batch_already_processed(
        source_system,
        source_object,
        source_path
    ):

        print("Batch already processed. SKIPPING.")
        return {
            "status": "SKIPPED",
            "records_read": 0,
            "records_written": 0
        }

    # --------------------------------------------------------
    # 2. Read RAW
    # --------------------------------------------------------

    df = spark.read.parquet(source_path)

    records_read = df.count()

    # --------------------------------------------------------
    # 3. Add ingestion metadata
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "_batch_id",
            F.lit(BATCH_ID)
        )
        .withColumn(
            "_ingestion_timestamp",
            F.lit(RUN_TS)
        )
        .withColumn(
            "_source_system",
            F.lit(source_system)
        )
        .withColumn(
            "_source_object",
            F.lit(source_object)
        )
    )

    # --------------------------------------------------------
    # 4. Generate record hash
    # --------------------------------------------------------

    business_columns = [
        c for c in df.columns
        if not c.startswith("_")
        and c not in [
            "run_id",
            "run_date",
            "source_system"
        ]
    ]

    df = df.withColumn(
        "_record_hash",
        F.sha2(
            F.to_json(
                F.struct(*[
                    F.col(c)
                    for c in business_columns
                ])
            ),
            256
        )
    )

    # --------------------------------------------------------
    # 5. Remove duplicate records within batch
    # --------------------------------------------------------

    df = df.dropDuplicates([
        "_source_system",
        "_source_object",
        "_record_hash"
    ])

    records_written = df.count()

    # --------------------------------------------------------
    # 6. Write Delta
    # --------------------------------------------------------

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )

    # --------------------------------------------------------
    # 7. Register table
    # --------------------------------------------------------

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.{table_name}
        USING DELTA
        LOCATION '{bronze_path}'
    """)

    # --------------------------------------------------------
    # 8. Control record
    # --------------------------------------------------------

    control_row = [(
        BATCH_ID,
        source_system,
        source_object,
        source_path,
        RUN_TS,
        "SUCCESS",
        records_written,
        None
    )]

    control_df = spark.createDataFrame(
        control_row,
        [
            "batch_id",
            "source_system",
            "source_object",
            "source_path",
            "ingestion_timestamp",
            "status",
            "record_count",
            "error_message"
        ]
    )

    control_df.write.format("delta").mode("append").save(
        CONTROL_PATH
    )

    # --------------------------------------------------------
    # 9. Audit record
    # --------------------------------------------------------

    audit_row = [(
        BATCH_ID,
        source_system,
        source_object,
        RUN_TS,
        records_read,
        records_written,
        records_read - records_written,
        "SUCCESS",
        None
    )]

    audit_df = spark.createDataFrame(
        audit_row,
        [
            "batch_id",
            "source_system",
            "source_object",
            "ingestion_timestamp",
            "records_read",
            "records_written",
            "records_rejected",
            "status",
            "error_message"
        ]
    )

    audit_df.write.format("delta").mode("append").save(
        AUDIT_PATH
    )

    print(f"Records read    : {records_read}")
    print(f"Records written : {records_written}")
    print("Status          : SUCCESS")

    return {
        "status": "SUCCESS",
        "records_read": records_read,
        "records_written": records_written
    }

# %% Cell 7: Ingest CUSTOMER
result_customer = ingest_to_bronze(
    source_path="Files/raw/peoplesoft/customer",
    bronze_path="Files/bronze/peoplesoft/customer",
    source_system="PEOPLESOFT",
    source_object="CUSTOMER",
    table_name="customer"
)

# %% Cell 8: Ingest ORDER_HDR
result_order = ingest_to_bronze(
    source_path="Files/raw/peoplesoft/order_hdr",
    bronze_path="Files/bronze/peoplesoft/order_hdr",
    source_system="PEOPLESOFT",
    source_object="ORDER_HDR",
    table_name="order_hdr"
)

# %% Cell 9: Ingest Dataverse ACCOUNT
result_account = ingest_to_bronze(
    source_path="Files/raw/dataverse/account",
    bronze_path="Files/bronze/dataverse/account",
    source_system="DATAVERSE",
    source_object="ACCOUNT",
    table_name="account"
)

# %% Cell 10: Ingest DDP
result_ddp = ingest_to_bronze(
    source_path="Files/raw/dataverse/ddp",
    bronze_path="Files/bronze/dataverse/ddp",
    source_system="DATAVERSE",
    source_object="DDP",
    table_name="ddp"
)

# %% Cell 11: Ingest invoice files
result_invoice = ingest_to_bronze(
    source_path="Files/raw/sftp/invoices",
    bronze_path="Files/bronze/sftp/invoices",
    source_system="SFTP",
    source_object="INVOICE",
    table_name="invoice_files"
)

# %% Cell 12: Confirmation notices
result_confirmation = ingest_to_bronze(
    source_path="Files/raw/sftp/confirmation_notice",
    bronze_path="Files/bronze/sftp/confirmation_notice",
    source_system="SFTP",
    source_object="CONFIRMATION_NOTICE",
    table_name="confirmation_files"
)

# %% Cell 13: Statements
result_statement = ingest_to_bronze(
    source_path="Files/raw/sftp/statements",
    bronze_path="Files/bronze/sftp/statements",
    source_system="SFTP",
    source_object="STATEMENT",
    table_name="statement_files"
)

# %% Cell 14: Check the control table
display(
    spark.sql("""
        SELECT
            batch_id,
            source_system,
            source_object,
            ingestion_timestamp,
            status,
            record_count
        FROM bronze.bronze_ingestion_control
        ORDER BY ingestion_timestamp DESC
    """)
)