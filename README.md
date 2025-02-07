Flight Data Pipeline: End-to-End ETL Project ✈️


**Project Overview**

This project focuses on building a scalable data pipeline to collect, clean, and structure historical flight data for future analysis. The goal is to process air traffic data efficiently, ensuring structured storage and optimized querying. This project does not include dashboards or visualizations—it is primarily a backend-focused ETL pipeline.

**Tech Stack**

- Database: AWS RDS (MySQL)
- ETL & Automation: Python (requests, pandas, MySQL Connector)
- Data Storage: Staging & production tables in MySQL
- Data Modeling: Entity-relationship design, indexing, referential integrity

**Pipeline Workflow**
- Extract: Collected flight data from multiple APIs.
- Transform: Cleaned and structured raw data in staging tables.
- Load: Moved processed data into production tables in AWS RDS.
- Optimize: Indexed key columns for faster querying.
- Automate: ETL pipeline scheduled for continuous updates.

**Key Features & Challenges**
- Handled API Rate Limits: Implemented request batching to manage restrictions.
- Query Optimization: Indexed key columns before migration for efficient retrieval.
- Data Integrity: Enforced constraints to maintain referential integrity across tables.

**Next Steps**

- Analyze the structured data to uncover air traffic trends.
- Explore predictive modeling (e.g., flight delay prediction).
- Optimize further with partitioning or additional indexing.

**Project Resources**
- [ERD Diagram](https://docs.google.com/document/d/15mu3wYjVOsDJsSmy1sr6LBxHk6JQU-H-pG_eQWCkBCQ/edit?tab=t.0)
- [Project Documentation](https://docs.google.com/document/d/1By9qU9mv3TmIXb8S8HxTPkfGFXD4YDrjSif3z9VlWww/edit?tab=t.0)
- [Sample Dataset](https://docs.google.com/spreadsheets/d/1cMR6E5lVDfqacRWloLz4NRQW2BhX7_JQ20vTR-HbnLA/edit?gid=0#gid=0)

**Contributing**
- Pull requests are welcome! If you find any issues or have suggestions, feel free to open an issue or submit a PR. 😊

**License**
- This project is licensed under the MIT License.