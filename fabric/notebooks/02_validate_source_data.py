# %% Cell 1: Common configuration
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from functools import reduce

spark = SparkSession.builder.getOrCreate()

RAW_ROOT = "Files/raw"

PATH_CUSTOMER = f"{RAW_ROOT}/peoplesoft/customer"
PATH_ORDER_HDR = f"{RAW_ROOT}/peoplesoft/order_hdr"
PATH_ACCOUNT = f"{RAW_ROOT}/dataverse/account"
PATH_DDP = f"{RAW_ROOT}/dataverse/ddp"
PATH_INVOICES = f"{RAW_ROOT}/sftp/invoices"
PATH_CONFIRMATIONS = f"{RAW_ROOT}/sftp/confirmation_notice"
PATH_STATEMENTS = f"{RAW_ROOT}/sftp/statements"

print("Source validation configuration loaded.")

# %% Cell 2: Load all source datasets
df_customer = spark.read.parquet(PATH_CUSTOMER)
df_order_hdr = spark.read.parquet(PATH_ORDER_HDR)
df_account = spark.read.parquet(PATH_ACCOUNT)
df_ddp = spark.read.parquet(PATH_DDP)
df_invoice = spark.read.parquet(PATH_INVOICES)
df_confirmation = spark.read.parquet(PATH_CONFIRMATIONS)
df_statement = spark.read.parquet(PATH_STATEMENTS)

print("All raw source datasets loaded successfully.")

# %% Cell 3: Source inventory
source_inventory = [
    ("PEOPLESOFT", "CUSTOMER", df_customer),
    ("PEOPLESOFT", "ORDER_HDR", df_order_hdr),
    ("DATAVERSE", "ACCOUNT", df_account),
    ("DATAVERSE", "DDP", df_ddp),
    ("SFTP", "INVOICE", df_invoice),
    ("SFTP", "CONFIRMATION_NOTICE", df_confirmation),
    ("SFTP", "STATEMENT", df_statement)
]

inventory_rows = []

for system, object_name, df in source_inventory:
    inventory_rows.append((
        system,
        object_name,
        df.count(),
        len(df.columns)
    ))

df_inventory = spark.createDataFrame(
    inventory_rows,
    [
        "source_system",
        "object_name",
        "record_count",
        "column_count"
    ]
)

display(df_inventory)

# %% Cell 4: Schema validation
expected_customer_columns = {
    "EXTERNAL_CUST_ID",
    "CUSTOMER_NAME",
    "ADDRESS_LINE1",
    "ADDRESS_CITY",
    "POSTCODE",
    "COUNTRY",
    "CUSTOMER_STATUS",
    "created_timestamp",
    "updated_timestamp",
    "run_id",
    "run_date",
    "source_system"
}

actual_customer_columns = set(df_customer.columns)

missing_customer = expected_customer_columns - actual_customer_columns
unexpected_customer = actual_customer_columns - expected_customer_columns

print("CUSTOMER missing:", missing_customer)
print("CUSTOMER unexpected:", unexpected_customer)

expected_order_columns = {
    "EXTERNAL_REF_ID",
    "ORDER_ID",
    "CUSTOMER_ID",
    "ALT2_CUSTOMER_ID",
    "ORDER_TOTAL",
    "PAYMENT_STATUS",
    "created_timestamp",
    "updated_timestamp",
    "run_id",
    "run_date",
    "source_system"
}

actual_order_columns = set(df_order_hdr.columns)

print(
    "ORDER_HDR missing:",
    expected_order_columns - actual_order_columns
)

print(
    "ORDER_HDR unexpected:",
    actual_order_columns - expected_order_columns
)

expected_account_columns = {
    "dataverse_record_id",
    "account_number",
    "account_name",
    "account_status",
    "address_line1",
    "address_city",
    "postcode",
    "created_on",
    "modified_on",
    "run_id",
    "run_date",
    "source_system"
}

print(
    "ACCOUNT missing:",
    expected_account_columns - set(df_account.columns)
)

expected_ddp_columns = {
    "dataverse_record_id",
    "account_number",
    "doc_type",
    "customer_control",
    "delivery_mode",
    "is_active",
    "effective_from",
    "modified_on",
    "run_id",
    "run_date",
    "source_system"
}

print(
    "DDP missing:",
    expected_ddp_columns - set(df_ddp.columns)
)

# %% Cell 5: Nullability checks
def null_count(df, column):
    return df.filter(
        F.col(column).isNull()
    ).count()


null_checks = []

for column in [
    "EXTERNAL_CUST_ID",
    "CUSTOMER_NAME",
    "COUNTRY",
    "updated_timestamp"
]:
    null_checks.append((
        "CUSTOMER",
        column,
        null_count(df_customer, column)
    ))

for column in [
    "EXTERNAL_REF_ID",
    "CUSTOMER_ID",
    "ALT2_CUSTOMER_ID",
    "updated_timestamp"
]:
    null_checks.append((
        "ORDER_HDR",
        column,
        null_count(df_order_hdr, column)
    ))

df_null_checks = spark.createDataFrame(
    null_checks,
    ["object_name", "column_name", "null_count"]
)

display(df_null_checks)

# %% Cell 6: Duplicate checks
customer_duplicates = (
    df_customer
    .groupBy("EXTERNAL_CUST_ID")
    .count()
    .filter(F.col("count") > 1)
)

print(
    "Duplicate CUSTOMER account numbers:",
    customer_duplicates.count()
)

order_duplicates = (
    df_order_hdr
    .groupBy("EXTERNAL_REF_ID")
    .count()
    .filter(F.col("count") > 1)
)

print(
    "Duplicate invoice numbers:",
    order_duplicates.count()
)

ddp_duplicates = (
    df_ddp
    .groupBy("account_number", "doc_type")
    .count()
    .filter(F.col("count") > 1)
)

print(
    "Duplicate DDP account/doc_type combinations:",
    ddp_duplicates.count()
)

# %% Cell 7: Referential integrity
orphan_bill_to = (
    df_order_hdr.alias("o")
    .join(
        df_customer.alias("c"),
        F.col("o.CUSTOMER_ID") ==
        F.col("c.EXTERNAL_CUST_ID"),
        "left"
    )
    .filter(
        F.col("c.EXTERNAL_CUST_ID").isNull()
    )
)

print(
    "Orphan Bill-To records:",
    orphan_bill_to.count()
)

orphan_sold_to = (
    df_order_hdr.alias("o")
    .join(
        df_customer.alias("c"),
        F.col("o.ALT2_CUSTOMER_ID") ==
        F.col("c.EXTERNAL_CUST_ID"),
        "left"
    )
    .filter(
        F.col("c.EXTERNAL_CUST_ID").isNull()
    )
)

print(
    "Orphan Sold-To records:",
    orphan_sold_to.count()
)

orphan_ddp_accounts = (
    df_ddp.alias("d")
    .join(
        df_account.alias("a"),
        F.col("d.account_number") ==
        F.col("a.account_number"),
        "left"
    )
    .filter(
        F.col("a.account_number").isNull()
    )
)

print(
    "DDP records without Account:",
    orphan_ddp_accounts.count()
)

# %% Cell 8: Invoice → ORDER_HDR validation
orphan_invoices = (
    df_invoice.alias("i")
    .join(
        df_order_hdr.alias("o"),
        F.col("i.invoice_number") ==
        F.col("o.EXTERNAL_REF_ID"),
        "left"
    )
    .filter(
        F.col("o.EXTERNAL_REF_ID").isNull()
    )
)

print(
    "Invoices without ORDER_HDR:",
    orphan_invoices.count()
)

# %% Cell 9: Confirmation → Order validation
orphan_confirmations = (
    df_confirmation.alias("c")
    .join(
        df_order_hdr.alias("o"),
        F.col("c.invoice_number") ==
        F.col("o.EXTERNAL_REF_ID"),
        "left"
    )
    .filter(
        F.col("o.EXTERNAL_REF_ID").isNull()
    )
)

print(
    "Confirmation notices without ORDER_HDR:",
    orphan_confirmations.count()
)

# %% Cell 10: Domain validation
invalid_customer_control = (
    df_ddp
    .filter(
        ~F.col("customer_control").isin(
            "Key",
            "Sub",
            "Both"
        )
        &
        F.col("customer_control").isNotNull()
    )
)

print(
    "Invalid Customer Control:",
    invalid_customer_control.count()
)

invalid_delivery_mode = (
    df_ddp
    .filter(
        ~F.col("delivery_mode").isin(
            "email",
            "print",
            "ignore"
        )
        &
        F.col("delivery_mode").isNotNull()
    )
)

print(
    "Invalid delivery mode:",
    invalid_delivery_mode.count()
)

# %% Cell 11: File validation
invalid_invoice_files = (
    df_invoice
    .filter(
        ~F.lower(F.col("file_name")).endswith(".pdf")
    )
)

invalid_confirmation_files = (
    df_confirmation
    .filter(
        ~F.lower(F.col("file_name")).endswith(".pdf")
    )
)

invalid_statement_files = (
    df_statement
    .filter(
        ~F.lower(F.col("file_name")).endswith(".pdf")
    )
)

print(
    "Invalid invoice files:",
    invalid_invoice_files.count()
)

print(
    "Invalid confirmation files:",
    invalid_confirmation_files.count()
)

print(
    "Invalid statement files:",
    invalid_statement_files.count()
)

# %% Cell 12: Source validation summary
validation_results = [
    ("CUSTOMER_NOT_NULL", null_count(df_customer, "EXTERNAL_CUST_ID")),
    ("CUSTOMER_DUPLICATES", customer_duplicates.count()),
    ("ORDER_DUPLICATES", order_duplicates.count()),
    ("ORPHAN_BILL_TO", orphan_bill_to.count()),
    ("ORPHAN_SOLD_TO", orphan_sold_to.count()),
    ("ORPHAN_DDP_ACCOUNT", orphan_ddp_accounts.count()),
    ("ORPHAN_INVOICE", orphan_invoices.count()),
    ("ORPHAN_CONFIRMATION", orphan_confirmations.count()),
    ("INVALID_CUSTOMER_CONTROL", invalid_customer_control.count()),
    ("INVALID_DELIVERY_MODE", invalid_delivery_mode.count()),
    ("INVALID_INVOICE_FILE", invalid_invoice_files.count()),
    ("INVALID_CONFIRMATION_FILE", invalid_confirmation_files.count()),
    ("INVALID_STATEMENT_FILE", invalid_statement_files.count())
]

df_validation = spark.createDataFrame(
    validation_results,
    ["rule_name", "failure_count"]
)

df_validation = df_validation.withColumn(
    "status",
    F.when(
        F.col("failure_count") == 0,
        "PASS"
    ).otherwise("FAIL")
)

display(df_validation)

# %% Cell 13: Overall source gate
critical_failures = (
    df_validation
    .filter(
        (F.col("failure_count") > 0) &
        (
            F.col("rule_name").isin(
                "CUSTOMER_NOT_NULL",
                "CUSTOMER_DUPLICATES",
                "ORDER_DUPLICATES",
                "ORPHAN_BILL_TO",
                "ORPHAN_SOLD_TO",
                "ORPHAN_DDP_ACCOUNT",
                "ORPHAN_INVOICE",
                "ORPHAN_CONFIRMATION"
            )
        )
    )
    .count()
)

if critical_failures > 0:
    SOURCE_STATUS = "FAILED"
else:
    SOURCE_STATUS = "PASSED"

print(f"SOURCE VALIDATION STATUS: {SOURCE_STATUS}")