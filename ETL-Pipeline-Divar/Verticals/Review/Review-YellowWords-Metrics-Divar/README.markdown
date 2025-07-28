# Review-YellowWords-Metrics-Divar

## Overview
This ETL pipeline, developed during my tenure as a Business Data Analyst at Divar (May 2023 - May 2024), focuses on generating metrics for yellow words in advertisements. It creates a `YellowWordsMetrics` table in the database to track ads that should not be published due to containing specific words, supporting the Review vertical.

## Problem Statement
- **Issue**: Ads with yellow words (prohibited terms) needed monitoring to prevent publication.
- **Goal**: Generate a table with metrics (e.g., total count, rejection rates) for analysis and action.

## Features
- **Data Model**: Defines `YellowWordsMetrics` with fields for date, yellow words, and various counts/shares.
- **Data Processing**: Calculates metrics based on ad data and inserts into the database.
- **Airflow Automation**: Schedules daily metric generation with dependencies.

## Tools Used
- Python, Django, PySpark, Airflow

## How to Run
1. Clone the repository: `git clone https://github.com/msavadkoohi/ETL-Pipeline-Divar.git`
2. Navigate to the project: `cd verticals/Review/Review-YellowWords-Metrics-Divar`
3. Install dependencies: `pip install pyspark airflow`
4. Set up Airflow: Configure DAGs in `/src/airflow/`
5. Run script: `python src/data_project/yellow_words_metrics.py`

## Data
- Sample data is simulated in the `yellow_words_metrics.py` script.
- Queries are in `queries/yellow_words_queries.sql`.

## Results
- Created a `YellowWordsMetrics` table for tracking yellow word usage and rejection rates.
- Supported Review team in filtering prohibited ads.

## Future Improvements
- Add real yellow word lists from a configuration file.
- Enhance Airflow with more sensors for data dependencies.
- Integrate with a reporting dashboard.