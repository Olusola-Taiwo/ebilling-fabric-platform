# %% Cell 1: SYNTHETIC SOURCE DATA GENERATOR
# ============================================================
# NOTEBOOK 01 — SYNTHETIC SOURCE DATA GENERATOR
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import uuid
import hashlib

spark = SparkSession.builder.getOrCreate()

# Reproducibility
SEED = 42
random.seed(SEED)

# ------------------------------------------------------------
# RUN METADATA
# ------------------------------------------------------------

run_row = spark.sql("""
    SELECT
        current_timestamp() AS run_timestamp,
        date_format(current_timestamp(), 'yyyy-MM-dd') AS run_date
""").collect()[0]

RUN_TS = run_row["run_timestamp"]
RUN_DATE = run_row["run_date"]

RUN_ID = (
    f"run_{RUN_DATE.replace('-', '')}_"
    f"{uuid.uuid4().hex[:8]}"
)

print(f"RUN_ID   : {RUN_ID}")
print(f"RUN_DATE : {RUN_DATE}")
print(f"RUN_TS   : {RUN_TS}")

# %% Cell 2: Define source paths
# ============================================================
# SOURCE LANDING STRUCTURE
# ============================================================

RAW_ROOT = "Files/raw"

PATH_CUSTOMER = f"{RAW_ROOT}/peoplesoft/customer"
PATH_ORDER_HDR = f"{RAW_ROOT}/peoplesoft/order_hdr"

PATH_ACCOUNT = f"{RAW_ROOT}/dataverse/account"
PATH_DDP = f"{RAW_ROOT}/dataverse/ddp"

PATH_INVOICES = f"{RAW_ROOT}/sftp/invoices"
PATH_CONFIRMATIONS = f"{RAW_ROOT}/sftp/confirmation_notice"
PATH_STATEMENTS = f"{RAW_ROOT}/sftp/statements"

print("Raw source paths configured.")

# %% Cell 3: Generate CUSTOMER
# ============================================================
# CUSTOMER MASTER
# ============================================================

CUSTOMER_COUNT = 10000

cities = [
    "CALGARY",
    "WINNIPEG",
    "LONDON",
    "EDMONTON",
    "VANCOUVER",
    "TORONTO"
]

customers = []

for i in range(CUSTOMER_COUNT):

    account = f"ACC{100000 + i}"

    customers.append((
        account,
        f"Customer_{i:05d}",
        f"{100 + i} Main Street",
        random.choice(cities),
        f"{random.choice(['T1','T2','M1','N1','V1'])}{random.randint(0,9)} "
        f"{random.choice(['A','B','C'])}{random.randint(0,9)}",
        "CAN",
        random.choice(["ACTIVE"] * 9 + ["INACTIVE"]),
        RUN_TS - timedelta(days=random.randint(30, 1000)),
        RUN_TS - timedelta(minutes=random.randint(0, 1440))
    ))

customer_schema = [
    "EXTERNAL_CUST_ID",
    "CUSTOMER_NAME",
    "ADDRESS_LINE1",
    "ADDRESS_CITY",
    "POSTCODE",
    "COUNTRY",
    "CUSTOMER_STATUS",
    "created_timestamp",
    "updated_timestamp"
]

df_customer = spark.createDataFrame(
    customers,
    customer_schema
)

df_customer = (
    df_customer
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("run_date", F.lit(RUN_DATE))
    .withColumn("source_system", F.lit("PEOPLESOFT_SIM"))
)

print(f"Customers generated: {df_customer.count()}")

# %% Cell 4: Generate Dataverse Account
# ============================================================
# DATAVERSE ACCOUNT
# ============================================================

account_rows = []

for row in customers:

    account_number = row[0]
    account_name = row[1]

    account_rows.append((
        str(uuid.uuid4()),
        account_number,
        account_name,
        random.choice(["ACTIVE"] * 9 + ["INACTIVE"]),
        row[2],
        row[3],
        row[4],
        RUN_TS - timedelta(days=random.randint(30, 1000)),
        RUN_TS - timedelta(minutes=random.randint(0, 1440))
    ))

account_schema = [
    "dataverse_record_id",
    "account_number",
    "account_name",
    "account_status",
    "address_line1",
    "address_city",
    "postcode",
    "created_on",
    "modified_on"
]

df_account = spark.createDataFrame(
    account_rows,
    account_schema
)

df_account = (
    df_account
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("run_date", F.lit(RUN_DATE))
    .withColumn("source_system", F.lit("DATAVERSE_SIM"))
)

print(f"Accounts generated: {df_account.count()}")

# %% Cell 5: Generate DDP
# ============================================================
# DATAVERSE DOCUMENT DELIVERY PREFERENCES
# ============================================================

ddp_rows = []

doc_types = [
    "invoice",
    "confirmation",
    "statement"
]

customer_controls = [
    "Key",
    "Sub",
    "Both"
]

delivery_modes = [
    "email",
    "print",
    "ignore"
]

for account in customers:

    account_number = account[0]

    for doc_type in doc_types:

        # Mostly valid records
        customer_control = random.choices(
            customer_controls + [None],
            weights=[35, 30, 25, 10]
        )[0]

        delivery_mode = random.choices(
            delivery_modes + [None],
            weights=[40, 35, 15, 10]
        )[0]

        ddp_rows.append((
            str(uuid.uuid4()),
            account_number,
            doc_type,
            customer_control,
            delivery_mode,
            True,
            RUN_TS - timedelta(days=random.randint(1, 365)),
            RUN_TS - timedelta(minutes=random.randint(0, 1440))
        ))

ddp_schema = [
    "dataverse_record_id",
    "account_number",
    "doc_type",
    "customer_control",
    "delivery_mode",
    "is_active",
    "effective_from",
    "modified_on"
]

df_ddp = spark.createDataFrame(
    ddp_rows,
    ddp_schema
)

df_ddp = (
    df_ddp
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("run_date", F.lit(RUN_DATE))
    .withColumn("source_system", F.lit("DATAVERSE_SIM"))
)

print(f"DDP records generated: {df_ddp.count()}")

# %% Cell 6: Generate ORDER_HDR
# ============================================================
# PEOPLESoft ORDER HEADER
# ============================================================

ORDER_COUNT = 30000

customer_ids = [row[0] for row in customers]

order_rows = []

for i in range(ORDER_COUNT):

    invoice_number = f"INV-{100000 + i}"
    order_id = f"ORD-{200000 + i}"

    bill_to = random.choice(customer_ids)
    sold_to = random.choice(customer_ids)

    # 20% of transactions have same Bill-To and Sold-To
    if random.random() < 0.20:
        sold_to = bill_to

    order_rows.append((
        invoice_number,
        order_id,
        bill_to,
        sold_to,
        round(random.uniform(100, 10000), 2),
        random.choice(["PAID", "UNPAID"]),
        RUN_TS - timedelta(days=random.randint(0, 30)),
        RUN_TS - timedelta(minutes=random.randint(0, 1440))
    ))

order_schema = [
    "EXTERNAL_REF_ID",
    "ORDER_ID",
    "CUSTOMER_ID",
    "ALT2_CUSTOMER_ID",
    "ORDER_TOTAL",
    "PAYMENT_STATUS",
    "created_timestamp",
    "updated_timestamp"
]

df_order_hdr = spark.createDataFrame(
    order_rows,
    order_schema
)

df_order_hdr = (
    df_order_hdr
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("run_date", F.lit(RUN_DATE))
    .withColumn("source_system", F.lit("PEOPLESOFT_SIM"))
)

print(f"Orders generated: {df_order_hdr.count()}")

# %% Cell 7: Generate invoice file metadata
# ============================================================
# INVOICE FILES
# ============================================================

invoice_rows = []

for row in order_rows:

    invoice_number = row[0]

    file_name = (
        f"KTINVOICE_"
        f"{random.randint(100000000, 999999999)}_"
        f"{invoice_number}.pdf"
    )

    file_path = f"/outgoing/invoices/{file_name}"

    content_seed = (
        f"{file_name}|{invoice_number}|{RUN_DATE}"
    )

    file_hash = hashlib.sha256(
        content_seed.encode()
    ).hexdigest()

    invoice_rows.append((
        file_name,
        file_path,
        invoice_number,
        random.randint(50000, 500000),
        RUN_TS - timedelta(minutes=random.randint(0, 1440)),
        file_hash
    ))

invoice_schema = [
    "file_name",
    "file_path",
    "invoice_number",
    "file_size_bytes",
    "file_arrival_timestamp",
    "source_file_hash"
]

df_invoice = spark.createDataFrame(
    invoice_rows,
    invoice_schema
)

df_invoice = (
    df_invoice
    .withColumn("doc_type", F.lit("invoice"))
    .withColumn("source_system", F.lit("SFTP_INVOICES_SIM"))
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("run_date", F.lit(RUN_DATE))
)

print(f"Invoice files generated: {df_invoice.count()}")

# %% Cell 8: Generate confirmation files
# ============================================================
# CONFIRMATION NOTICE FILES
# ============================================================

confirmation_rows = []

for row in order_rows:

    invoice_number = row[0]

    confirmation_number = (
        invoice_number.replace("INV", "CFRM")
    )

    file_name = (
        f"KTCFRM_"
        f"{random.randint(100000000, 999999999)}_"
        f"{confirmation_number}.pdf"
    )

    file_path = (
        f"/outgoing/order_conf/{file_name}"
    )

    content_seed = (
        f"{file_name}|"
        f"{confirmation_number}|"
        f"{RUN_DATE}"
    )

    file_hash = hashlib.sha256(
        content_seed.encode()
    ).hexdigest()

    confirmation_rows.append((
        file_name,
        file_path,
        confirmation_number,
        invoice_number,
        random.randint(50000, 500000),
        RUN_TS - timedelta(minutes=random.randint(0, 1440)),
        file_hash
    ))

confirmation_schema = [
    "file_name",
    "file_path",
    "confirmation_number",
    "invoice_number",
    "file_size_bytes",
    "file_arrival_timestamp",
    "source_file_hash"
]

df_confirmation = spark.createDataFrame(
    confirmation_rows,
    confirmation_schema
)

df_confirmation = (
    df_confirmation
    .withColumn("doc_type", F.lit("confirmation"))
    .withColumn("source_system", F.lit("SFTP_CONFIRM_SIM"))
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("run_date", F.lit(RUN_DATE))
)

print(
    f"Confirmation files generated: "
    f"{df_confirmation.count()}"
)

# %% Cell 9: Generate statements
# ============================================================
# STATEMENT FILES
# ============================================================

STATEMENT_COUNT = 1500

statement_rows = []

for i in range(STATEMENT_COUNT):

    account = random.choice(customer_ids)

    statement_date = (
        RUN_TS.date() -
        timedelta(days=random.randint(0, 30))
    )

    file_name = (
        f"KTSTATEMENT_"
        f"{statement_date.strftime('%Y%m%d')}_"
        f"{account}.pdf"
    )

    file_path = (
        f"/outgoing/statements/{file_name}"
    )

    content_seed = (
        f"{file_name}|{account}|{statement_date}"
    )

    file_hash = hashlib.sha256(
        content_seed.encode()
    ).hexdigest()

    statement_rows.append((
        file_name,
        file_path,
        account,
        statement_date,
        random.randint(50000, 500000),
        RUN_TS,
        file_hash
    ))

statement_schema = [
    "file_name",
    "file_path",
    "external_cust_id",
    "statement_date",
    "file_size_bytes",
    "file_arrival_timestamp",
    "source_file_hash"
]

df_statement = spark.createDataFrame(
    statement_rows,
    statement_schema
)

df_statement = (
    df_statement
    .withColumn("doc_type", F.lit("statement"))
    .withColumn("source_system", F.lit("SFTP_STATEMENT_SIM"))
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("run_date", F.lit(RUN_DATE))
)

print(
    f"Statement files generated: "
    f"{df_statement.count()}"
)

# %% Cell 10: Inject controlled bad data
# ============================================================
# CONTROLLED DATA QUALITY DEFECTS
# ============================================================

# ------------------------------------------------------------
# 1. Missing CUSTOMER_ID
# ------------------------------------------------------------

bad_order = (
    df_order_hdr
    .limit(1)
    .withColumn("CUSTOMER_ID", F.lit(None).cast("string"))
)

# ------------------------------------------------------------
# 2. Orphan Sold-To
# ------------------------------------------------------------

orphan_order = (
    df_order_hdr
    .limit(1)
    .withColumn(
        "ALT2_CUSTOMER_ID",
        F.lit("ACC99999999")
    )
)

# ------------------------------------------------------------
# 3. Invalid DDP delivery mode
# ------------------------------------------------------------

bad_ddp = (
    df_ddp
    .limit(1)
    .withColumn(
        "delivery_mode",
        F.lit("INVALID")
    )
)

# ------------------------------------------------------------
# 4. Invalid customer control
# ------------------------------------------------------------

bad_control = (
    df_ddp
    .limit(1)
    .withColumn(
        "customer_control",
        F.lit("INVALID")
    )
)

print("Controlled data-quality defects prepared.")

# %% Cell 11: Write RAW
# ============================================================
# WRITE SOURCE DATA TO RAW
# ============================================================

(
    df_customer
    .write
    .mode("overwrite")
    .parquet(PATH_CUSTOMER)
)

(
    df_order_hdr
    .write
    .mode("overwrite")
    .parquet(PATH_ORDER_HDR)
)

(
    df_account
    .write
    .mode("overwrite")
    .parquet(PATH_ACCOUNT)
)

(
    df_ddp
    .write
    .mode("overwrite")
    .parquet(PATH_DDP)
)

(
    df_invoice
    .write
    .mode("overwrite")
    .parquet(PATH_INVOICES)
)

(
    df_confirmation
    .write
    .mode("overwrite")
    .parquet(PATH_CONFIRMATIONS)
)

(
    df_statement
    .write
    .mode("overwrite")
    .parquet(PATH_STATEMENTS)
)

print("================================================")
print("RAW SOURCE GENERATION COMPLETED")
print("================================================")
print(f"RUN_ID: {RUN_ID}")
print(f"RAW ROOT: {RAW_ROOT}")


# %% Cell 12: Source reconciliation
# ============================================================
# SOURCE RECONCILIATION
# ============================================================

customer_count = df_customer.count()
account_count = df_account.count()
order_count = df_order_hdr.count()
invoice_count = df_invoice.count()
confirmation_count = df_confirmation.count()
statement_count = df_statement.count()

print(f"Customer records       : {customer_count}")
print(f"Account records        : {account_count}")
print(f"Order records          : {order_count}")
print(f"Invoice files         : {invoice_count}")
print(f"Confirmation files    : {confirmation_count}")
print(f"Statement files       : {statement_count}")

# Validate Bill-To relationships

orphan_bill_to = (
    df_order_hdr.alias("o")
    .join(
        df_customer.alias("c"),
        F.col("o.CUSTOMER_ID") ==
        F.col("c.EXTERNAL_CUST_ID"),
        "left"
    )
    .filter(F.col("c.EXTERNAL_CUST_ID").isNull())
    .count()
)

# Validate Sold-To relationships

orphan_sold_to = (
    df_order_hdr.alias("o")
    .join(
        df_customer.alias("c"),
        F.col("o.ALT2_CUSTOMER_ID") ==
        F.col("c.EXTERNAL_CUST_ID"),
        "left"
    )
    .filter(F.col("c.EXTERNAL_CUST_ID").isNull())
    .count()
)

print(f"Orphan Bill-To records : {orphan_bill_to}")
print(f"Orphan Sold-To records : {orphan_sold_to}")

# %% Cell 13: Source execution audit
# ============================================================
# SOURCE GENERATION AUDIT
# ============================================================

audit_data = [(
    RUN_ID,
    RUN_TS,
    RUN_DATE,
    customer_count,
    account_count,
    order_count,
    invoice_count,
    confirmation_count,
    statement_count,
    "SUCCESS"
)]

audit_schema = [
    "run_id",
    "run_timestamp",
    "run_date",
    "customer_count",
    "account_count",
    "order_count",
    "invoice_count",
    "confirmation_count",
    "statement_count",
    "status"
]

df_source_audit = spark.createDataFrame(
    audit_data,
    audit_schema
)

display(df_source_audit)