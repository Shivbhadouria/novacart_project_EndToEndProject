# 🛒 NovaCart — End-to-End Data Engineering on Azure Databricks

> A complete, incremental **Lakehouse pipeline** that takes messy e-commerce data from **Azure SQL**, cleans it through a **Bronze → Silver → Gold** medallion architecture, and serves business insights in a **Databricks dashboard**.

![Azure](https://img.shields.io/badge/Azure-0078D4?logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-governance-blue)
![GitHub](https://img.shields.io/badge/GitHub-181717?logo=github&logoColor=white)

---

## 📖 Table of Contents
1. [What is this project?](#-what-is-this-project)
2. [Architecture](#-architecture)
3. [Source data](#-source-data)
4. [How the pipeline works](#-how-the-pipeline-works)
5. [Dashboard](#-dashboard)
6. [Project structure](#-project-structure)
7. [How to run it](#-how-to-run-it)
8. [Key concepts demonstrated](#-key-concepts-demonstrated)
9. [Author](#-author)

---

## 🎯 What is this project?

**NovaCart** is a fictional online store with three kinds of data: **products**, **orders** and **payments**. The raw data is intentionally *messy* (typos, `$` signs, commas in prices, nulls, blanks), just like real-world data.

This project shows how to:

- 📥 **Ingest only new data** each run (incremental loading, no full reloads)
- 🧹 **Clean and validate** messy data, and **quarantine** bad rows for review
- 🏆 **Build business-ready tables** with joins, KPIs and history tracking (SCD Type 2)
- 📊 **Visualize** sales and payment health in a dashboard

---

## 🏗️ Architecture

![Architecture]
<p align="center">
  <img src="End-to-End Azure Databricks Architecture.png" alt="End-to-End Azure Databricks Architecture" width="100%">
</p>

**Flow in one line:**
`Azure SQL` → `Lakehouse Federation (Unity Catalog)` → `Bronze` → `Silver` → `Gold` → `Dashboard`

| Layer | Purpose | What happens here |
|---|---|---|
| 🗄️ **Source** | Transactional data | Azure SQL Database with `products`, `orders`, `payments` |
| 🔗 **Federation** | Secure access | Unity Catalog reads Azure SQL directly from Databricks |
| 🥉 **Bronze** | Raw | Incremental ingestion, data landed as-is, audit columns added |
| 🥈 **Silver** | Cleansed | Cleaning, standardisation, deduplication, validation, quarantine |
| 🥇 **Gold** | Curated | Joined order-level table, KPIs, SCD Type 2 history |
| 📊 **BI** | Insights | Databricks dashboard on top of Gold tables |

---

## 🗃️ Source data

Three related tables live in Azure SQL (`dbo` schema):

```
products ──< orders ──< payments
(product_id)  (order_id)   (payment_id)
```

| Table | Key columns |
|---|---|
| `products` | `product_id`, `product_name`, `category`, `price`, `updated_at` |
| `orders` | `order_id`, `customer_id`, `product_id`, `order_status`, `order_amount`, `created_at`, `updated_at` |
| `payments` | `payment_id`, `order_id`, `payment_status`, `paid_amount`, `processed_at` |

### 🧪 The "messy data" I deliberately created
The initial load script injects realistic data-quality problems so the Silver layer has real work to do:

| Problem | Example |
|---|---|
| Typos | `ELECTRNICS` instead of `ELECTRONICS` |
| Mixed casing | `lifestyle`, `electronics`, `shipped` |
| Extra spaces | `'   Product 15   '` |
| Different naming styles | `Prod_9`, `PRODUCT-11`, `Product 1` |
| Currency symbols / commas | `$50.00`, `50,00` |
| Invalid values | `??`, `N/A`, `0.00` |
| Nulls and blanks | missing `customer_id`, empty `order_status` |

### 🔄 Incremental data
Two extra scripts simulate new data arriving later (10 new products, orders and payments each), so the pipeline can be tested for incremental behaviour.

---

## ⚙️ How the pipeline works

All three notebooks follow the same pattern: **a control table remembers where the last run stopped**, so every run only processes new data and is safe to re-run.

### 🥉 Bronze — `BronzeWork.ipynb`
*Goal: copy new rows from Azure SQL into Delta tables.*

1. Read the last **watermark** (timestamp + primary key) from `bronze_schema.ingstion_control`
2. Read the source table and keep only **new or changed rows**
3. Add audit columns: `bronze_ingested_at`, `bronze_run_id`, `bronze_source_name`
4. **Append** to `orders_raw`, `products_raw`, `payments_raw`
5. Save the new watermark → re-running with no new data writes `0` rows

### 🥈 Silver — `SilverWork.ipynb`
*Goal: clean, standardise and validate.*

1. Read only Bronze rows newer than the last Silver run
2. **Clean**
   - Trim and uppercase text (`order_status`, `category`, `payment_status`)
   - Fix product names (`Prod_9`, `PRODUCT-11` → `PRODUCT 9`, `PRODUCT 11`)
   - Fix typo `ELECTRNICS` → `ELECTRONICS`
   - Remove `$`, convert decimal commas, cast amounts to `double`
   - Turn `N/A`, `??`, blanks into `NULL`
3. **Deduplicate** — keep the latest version of each ID using a window function
4. **Validate** business rules (e.g. missing customer, missing status, amount ≤ 0)
5. **Quarantine** bad rows with a reason such as `verify_order_amount`, so the owning team can fix them
6. **Merge (upsert)** good rows into Silver Delta tables

### 🥇 Gold — `GoldWork.ipynb`
*Goal: business-ready tables.*

1. Find only the **changed** Silver rows since the last Gold run
2. Find the **impacted `order_id`s** (a change in an order, product or payment affects its order)
3. Rebuild only those orders by joining orders + products + payments
4. Calculate `payment_completion_ratio` and `payment_state` (`paid`, `Overpaid`, `unpaid`, `Invalid_order_amount`)
5. Merge into **`orders_information`** (latest state of every order)
6. Maintain **`orders_information_scd2`**: old version is closed (`is_current = false`, `valid_to_ts`), new version is inserted (`is_current = true`). Full history is preserved
7. Recalculate **`category_performance`** KPIs only for impacted categories
8. Publish **Parquet snapshots** (latest + timestamped history) to a Databricks Volume
9. Update the Gold control table

### 📌 Gold tables at a glance

| Table | Description |
|---|---|
| `orders_information` | One row per order, with product and payment details |
| `orders_information_scd2` | Same data with full change history (SCD Type 2) |
| `category_performance` | Orders, GMV, paid amount, completion ratio, failure rate per category |

---

## 📊 Dashboard

![Dashboard](docs/NovaCart_Dashboard.png)

The **NovaCart Sales & Payments Overview** dashboard includes:

- **KPIs:** Total Orders, Gross Merchandise Value (GMV), Total Paid Amount
- **Charts:** Monthly GMV trend, Orders by status, GMV by category, Payment failure rate by category, Orders by payment state
- **Table:** Category performance summary
- **Filters:** Category, order status, order date

📄 A PDF export is available in the repo: [`NovaCart_Dashboard.pdf`](docs/NovaCart_Dashboard.pdf)

---

## 📁 Project structure

```
novacart_project_EndToEndProject/
│
├── notebooks/
│   ├── BronzeWork.ipynb        # Raw incremental ingestion
│   ├── SilverWork.ipynb        # Cleaning, validation, quarantine
│   └── GoldWork.ipynb          # Joins, KPIs, SCD Type 2, snapshots
│
├── sql/
│   ├── sql-tables-creation.sql          # Create tables in Azure SQL
│   ├── initial-load-for-mess-data.sql   # Initial load (clean + messy data)
│   ├── IncrementalLoad-1.sql            # New data batch 1
│   └── IncrementalLoad-2.sql            # New data batch 2
│
├── docs/
│   ├── End-to-End_Azure_Databricks_Architecture.png
│   ├── NovaCart_Dashboard.png
│   └── NovaCart_Dashboard.pdf
│
└── README.md
```

---

## 🚀 How to run it

**Prerequisites:** Azure subscription, Azure SQL Database, Azure Databricks workspace with Unity Catalog.

1. **Create the source tables** in Azure SQL by running `sql/sql-tables-creation.sql`
2. **Load the initial data** with `sql/initial-load-for-mess-data.sql`
3. **Connect Databricks to Azure SQL** using Lakehouse Federation (a Unity Catalog connection and foreign catalog, named `dbxnovacart_catalog` in the notebooks)
4. **Create the target catalog** `novacartdbx` in Databricks
5. **Run the notebooks in order:** `BronzeWork` → `SilverWork` → `GoldWork`
6. **Test incremental loading:** run `IncrementalLoad-1.sql` in Azure SQL, then re-run the three notebooks. Repeat with `IncrementalLoad-2.sql`
7. **Build or open the dashboard** on the Gold tables
8. *(Optional)* Schedule the notebooks with **Databricks Jobs & Workflows** and add failure alerts

---

## 💡 Key concepts demonstrated

- ✅ Medallion architecture (Bronze / Silver / Gold) on Delta Lake
- ✅ Incremental loading with **watermarks** and **control tables**
- ✅ **Idempotent** pipelines (safe to re-run without duplicates)
- ✅ Data cleaning, standardisation and **deduplication** with window functions
- ✅ **Data quality checks** and a quarantine pattern for bad records
- ✅ Delta Lake **MERGE (upsert)** operations
- ✅ **SCD Type 2** for historical tracking
- ✅ Impact-based incremental refresh (rebuild only affected orders and categories)
- ✅ Governance with **Unity Catalog** and Lakehouse Federation
- ✅ Version control with GitHub

---

## 👤 Author

**Shiv Bhadouria** — Data Engineer (Azure Databricks, Data Factory, PySpark, Delta Lake)

- 💼 LinkedIn: [linkedin.com/in/shivbhadouria](https://www.linkedin.com/in/shivbhadouria/)
- 🐙 GitHub: [@Shivbhadouria](https://github.com/Shivbhadouria)

⭐ If you found this project useful, consider giving the repo a star!
