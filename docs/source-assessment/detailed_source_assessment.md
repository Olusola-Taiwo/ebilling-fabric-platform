# Detailed Source Assessment

---

## 1. PeopleSoft CUSTOMER

| Attribute        | Assessment                                   |
|------------------|-----------------------------------------------|
| Source ID        | PSFT-CUST                                     |
| System           | PeopleSoft / BI Data Lake                     |
| Object           | CUSTOMER                                      |
| Domain           | Customer                                      |
| Source owner     | ERP/Data Platform Team (simulated)            |
| Business owner   | Customer/Accounts Team (simulated)            |
| Format           | Parquet                                       |
| Frequency        | Daily                                         |
| Expected volume  | 5,000–20,000 records initially                |
| SLA              | Available before 07:00                        |
| Business key     | EXTERNAL_CUST_ID                              |
| Natural key      | External customer/account number              |
| Surrogate key    | None initially                                |
| Timestamp        | updated_timestamp                             |
| Watermark        | updated_timestamp                             |
| CDC              | Simulated using timestamp                     |
| Deletes          | Soft delete preferred                         |
| Late arrivals    | Accepted based on event/update timestamp      |
| PII              | Customer name/address information             |
| Classification   | Confidential                                  |
| Downstream       | Silver Customer / Invoice processing          |

---

## 2. PeopleSoft ORDER_HDR

| Attribute        | Assessment                                   |
|------------------|-----------------------------------------------|
| Source ID        | PSFT-ORD                                      |
| System           | PeopleSoft / BI Data Lake                     |
| Object           | ORDER_HDR                                     |
| Domain           | Billing/Order                                 |
| Source owner     | ERP/Data Platform Team (simulated)            |
| Business owner   | Billing Team (simulated)                      |
| Format           | Parquet                                       |
| Frequency        | Daily                                         |
| Expected volume  | 10,000–50,000 initially                        |
| SLA              | Available before 07:00                        |
| Business key     | EXTERNAL_REF_ID                               |
| Technical key    | ORDER_ID                                      |
| Bill-To          | CUSTOMER_ID                                   |
| Sold-To          | ALT2_CUSTOMER_ID                              |
| Timestamp        | updated_timestamp                             |
| Watermark        | updated_timestamp                             |
| CDC              | Simulated                                     |
| Deletes          | No physical deletes expected                  |
| Late arrivals    | Possible                                      |
| PII              | Indirect customer reference                   |
| Classification   | Confidential                                  |
| Downstream       | Silver Invoice / Confirmation processing      |

---

## 3. Dataverse Account

| Attribute        | Assessment                                   |
|------------------|-----------------------------------------------|
| Source ID        | DV-ACC                                        |
| System           | Microsoft Dataverse                           |
| Object           | Account                                       |
| Domain           | Customer                                      |
| Source owner     | CRM/Application Team (simulated)              |
| Business owner   | Customer Management (simulated)               |
| Format           | Table/API                                     |
| Frequency        | Daily snapshot (simulation)                   |
| Expected volume  | 5,000–20,000                                  |
| SLA              | Available before processing                   |
| Business key     | Account number                                |
| Technical key    | Dataverse record ID                           |
| Timestamp        | modified_on                                   |
| Watermark        | modified_on                                   |
| CDC              | Simulated                                     |
| Deletes          | Soft delete/inactive                          |
| Late arrivals    | Possible                                      |
| PII              | Account/customer information                  |
| Classification   | Confidential                                  |
| Downstream       | Silver Account / DDP processing               |

---

## 4. Dataverse Document Delivery Preference (DDP)

This is one of the most important sources in the entire project.

| Attribute        | Assessment                                   |
|------------------|-----------------------------------------------|
| Source ID        | DV-DDP                                        |
| System           | Microsoft Dataverse                           |
| Object           | mnpd_documentdeliverypreference               |
| Domain           | eBilling                                      |
| Source owner     | eBilling/Application Team (simulated)         |
| Business owner   | Billing Operations (simulated)                |
| Format           | Table/API                                     |
| Frequency        | Daily/incremental                             |
| Business key     | Account + Document Type                       |
| Technical key    | Dataverse record ID                           |
| Timestamp        | modified_on                                   |
| Watermark        | modified_on                                   |
| CDC              | Simulated                                     |
| Deletes          | Soft delete/inactive                          |
| Late arrivals    | Possible                                      |
| PII              | Customer/account reference                    |
| Classification   | Confidential                                  |
| Downstream       | Invoice/Confirmation business processing      |

---

## 5. SFTP Invoice PDFs

| Attribute        | Assessment                                   |
|------------------|-----------------------------------------------|
| Source ID        | SFTP-INV                                      |
| Source           | SFTP                                          |
| Folder           | /outgoing/invoices                            |
| Domain           | Billing                                       |
| Format           | PDF                                           |
| Frequency        | Daily                                         |
| Expected volume  | 5,000–20,000 files                            |
| SLA              | Files available before 07:00                  |
| Business key     | Invoice number                                |
| Technical key    | File name/path                                |
| Timestamp        | File modified/arrival timestamp               |
| Watermark        | Arrival timestamp                             |
| CDC              | Not applicable                                |
| Deletes          | Source file removed/archived after pickup     |
| Late arrivals    | Yes                                           |
| PII              | Invoice/customer information                  |
| Classification   | Confidential                                  |
| Downstream       | Invoice integration                           |

---

## 6. SFTP Confirmation Notices

| Attribute        | Assessment                                   |
|------------------|-----------------------------------------------|
| Source ID        | SFTP-CFRM                                     |
| Source           | SFTP                                          |
| Folder           | /outgoing/order_conf                          |
| Domain           | Order                                         |
| Format           | PDF                                           |
| Frequency        | Daily                                         |
| Expected volume  | 3,000–10,000 files                            |
| SLA              | Files available before 07:00                  |
| Business key     | Confirmation number                           |
| Technical key    | File name/path                                |
| Timestamp        | File arrival timestamp                        |
| Watermark        | Arrival timestamp                             |
| CDC              | Not applicable                                |
| Deletes          | Source file archived/removed after pickup     |
| Late arrivals    | Yes                                           |
| PII              | Customer/order information                    |
| Classification   | Confidential                                  |
| Downstream       | Confirmation integration                      |

---

## 7. SFTP Statements

| Attribute        | Assessment                                   |
|------------------|-----------------------------------------------|
| Source ID        | SFTP-STMT                                     |
| Source           | SFTP                                          |
| Folder           | /outgoing/statements                          |
| Domain           | Billing                                       |
| Format           | PDF                                           |
| Frequency        | Daily                                         |
| Expected volume  | 500–2,000                                     |
| SLA              | Before 07:00                                  |
| Business key     | Customer account + statement date             |
| Technical key    | File name/path                                |
| Timestamp        | File arrival timestamp                        |
| Watermark        | Arrival timestamp                             |
| CDC              | Not applicable                                |
| Deletes          | Archive after pickup                          |
| Late arrivals    | Yes                                           |
| PII              | Customer/account information                  |
| Classification   | Confidential                                  |
| Downstream       | Statement processing                          |

