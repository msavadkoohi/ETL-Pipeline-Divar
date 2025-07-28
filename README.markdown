# ETL-Pipeline-Divar

## Overview
This repository contains a collection of ETL (Extract, Transform, Load) pipelines developed during my tenure as a Business Data Analyst at Divar from May 2023 to May 2024. These pipelines are organized by verticals (e.g., Trust, Review) and are designed to generate SQL tables and metrics accessible via Metabase, supporting various teams across the company. The project was last updated on July 28, 2025.

## Project Structure
The repository is structured to accommodate multiple verticals, each containing relevant ETL pipelines:

- `verticals/Trust/`
  - `Trust-Blacklist-Divar`: Pipeline for consolidating blacklist and unblacklist data into a historical store (Trust team).
  - `Trust-Blacklist-Appeal-Funnel-Divar`: Pipeline for analyzing blacklist appeal funnels (Trust team).
  - `Trust-GetContact-Usage-Divar`: Pipeline for summarizing and analyzing get_contact action usage (Trust team).
  - `Trust-Fraud-Metrics-Divar`: Pipeline for calculating fraud-related metrics (Trust team).

- `verticals/Review/`
  - `Review-YellowWords-Metrics-Divar`: Pipeline for tracking yellow word usage in ads and preventing publication (Review team).

- `verticals/[OtherVerticals]/` (Future): Placeholder for additional verticals like CallCenter, Marketplace, Operations, etc.

## Tools Used
- Python, Django, PySpark, Airflow

## How to Contribute
1. Clone the repository: `git clone https://github.com/msavadkoohi/ETL-Pipeline-Divar.git`
2. Navigate to the project directory: `cd ETL-Pipeline-Divar`
3. Install dependencies: `pip install pyspark airflow django`
4. Create a new vertical folder under `verticals/` (e.g., `verticals/NewVertical/`).
5. Add a new project folder with the structure: `/src/data_model/`, `/src/data_project/`, `/src/airflow/`, `/queries/`, `/data/`, and `README.md`.
6. Commit and push changes: `git add .`, `git commit -m "Added new project"`, `git push`.

## How to Run
- For each project, navigate to its directory (e.g., `cd verticals/Trust/Trust-Blacklist-Divar`) and follow the specific `README.md` instructions.
- Ensure Airflow is configured and Spark is set up for running DAGs and scripts.
- Example: `python src/data_project/main.py` or configure Airflow to schedule DAGs.

## Data
- Sample data is included in the `/data/` folder of each project for testing.
- Queries are stored in the `/queries/` folder where applicable.

## Results
- Successfully generated SQL tables for various metrics, supporting Trust and Review teams.
- Enabled data-driven decisions with centralized access via Metabase.

## Future Improvements
- Add support for additional verticals (e.g., CallCenter, PostPublish).
- Enhance documentation with detailed API or database integration guides.
- Automate project setup with reusable templates.
- Integrate real-time data processing capabilities.

## Last Updated
July 28, 2025, 05:19 PM CEST