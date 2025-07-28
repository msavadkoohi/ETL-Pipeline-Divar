# ETL Blacklist Pipeline - Divar

## Overview
This directory contains a sample ETL pipeline developed during my tenure as a Business Data Analyst at Divar (May 2023 - May 2024). The pipeline addresses a critical data management challenge: the previous separation of blacklist and unblacklist data, which made it difficult to track how many times users were blacklisted or unblacklisted. This project transforms and consolidates this data into a historical store, providing a comprehensive view of user blacklist status over time.

## Problem Statement
- **Issue**: Prior to this pipeline, blacklist and unblacklist data were stored separately, and the system only displayed the latest update, obscuring the full history of user blacklist events.
- **Impact**: This limited the ability to analyze repeat blacklist/unblacklist patterns or understand user behavior trends effectively.

## Features
- **Data Consolidation**: Merges blacklist and unblacklist datasets using PySpark.
- **Historical Tracking**: Stores data as a history, enabling analysis of multiple blacklist/unblacklist events per user.
- **Data Transformation**: Applies timezone conversion (UTC to Asia/Tehran) and handles missing values.
- **Scalability**: Designed to process large datasets efficiently.

## Tools Used
- Python, PySpark

## How to Run
1. Clone the repository: `git clone https://github.com/msavadkoohi/ETL-Pipeline-Divar.git`
2. Install dependencies: `pip install pyspark`
3. Navigate to the project directory: `cd ETL-Blacklist-Divar`
4. Run the script: `python src/main.py`

## Data
- Sample data is provided in `data/sample_data.csv` to demonstrate the pipeline's functionality.

## Results
- Successfully created a historical dataset that tracks blacklist and unblacklist events, enabling better analysis of user behavior.
- Improved data visibility, allowing for insights into repeat blacklist patterns and user retention strategies.

## Future Improvements
- Add visualization scripts (e.g., using Matplotlib or Power BI) to display blacklist trends.
- Extend the pipeline to include additional data sources (e.g., appeal data).