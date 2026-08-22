# Source Registry

| Source ID | Source System | Source Object                | Source Type                | Format             | Frequency | Primary Domain |
|-----------|---------------|------------------------------|----------------------------|--------------------|-----------|----------------|
| PSFT-CUST | PeopleSoft    | CUSTOMER                     | Database/Data Lake extract | Parquet            | Daily     | Customer       |
| PSFT-ORD  | PeopleSoft    | ORDER_HDR                    | Database/Data Lake extract | Parquet            | Daily     | Billing/Order  |
| DV-ACC    | Dataverse     | Account                      | API/Table                  | Parquet simulation | Daily     | Customer       |
| DV-DDP    | Dataverse     | Document Delivery Preference | API/Table                  | Parquet simulation | Daily     | eBilling       |
| SFTP-INV  | SFTP          | Invoice PDFs                 | File                       | PDF                | Daily     | Billing        |
| SFTP-CFRM | SFTP          | Confirmation Notices         | File                       | PDF                | Daily     | Order          |
| SFTP-STMT | SFTP          | Statements                   | File                       | PDF                | Daily     | Billing        |
