This sample project aims to provide performance of Contoso VMI (Vendor Management Inventory) Clients with dedicated measures related to inventory, promotion and order management.

Source Data - BlueYonder and SAP ECC( test data)

Activities in Scope:
1) Data Reporting Platform
   Daily/Weekly sync of reporting with source system
   Power BI dashboards to monitor the metrics
   Data Modeliing
   Integration of Contoso identified data source for Blue Yonder
2) Data Platform:
   Extraction of Promo , Inventory Orders and Sellout Interfaces

Project data source files shared by SAP ECC and BY on datalake:
  sabicontosodev -- DEV environment

Overview :
Development tools are ADF, Databricks and PBI
ADF naming convention : pl_0309_01_*
On Databricks inside workspace 0309_VMI_Reporting there are notebooks created for ingestion and business rules

On PowerBI VMIReporting there is the related reporting

Data Factory Pipeline:
Orchestrator is Azure Data Factory with 3 environments
  DEV - Contoso-ADF-0309-DEV
ADF copies data from local database and BY server, imports Master and Transactional data

Data coming from BY server :
  pl_0309_01_MoveFromSFTPToRaw
