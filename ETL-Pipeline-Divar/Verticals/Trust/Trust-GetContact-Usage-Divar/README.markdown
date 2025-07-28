# ETL Get Contact Usage - Divar

## Overview
This directory contains an ETL pipeline developed during my tenure as a Business Data Analyst at Divar (May 2023 - May 2024). The pipeline was designed to process and summarize data from the `get_contact` action, which was previously stored in an unreadable, messy JSON format. The goal was to identify users excessively using this action (e.g., via bots) to crawl phone numbers for advertising or other unauthorized purposes.

## Problem Statement
- **Issue**: The `get_contact` action data was unstructured and stored as complex JSON, making it inaccessible for analysis.
- **Goal**: Transform this data into a readable format and detect abnormal usage patterns, such as excessive requests from specific IPs or phones, to prevent data scraping and protect user privacy.

## Features
- **Data Transformation**: Converts messy JSON data into structured summaries.
- **Usage Analysis**: Aggregates request counts, unique phones, and IPs by date and status.
- **Type Separation**: Distinguishes between backend and user-initiated requests.
- **Detect Anomalies**: Enables identification of potential bot activity through high request volumes.

## Tools Used
- Python, PySpark, Pendulum

## How to Run
1. Clone the repository: `git clone https://github.com/msavadkoohi/ETL-Pipeline-Divar.git`
2. Navigate to the project directory: `cd ETL-GetContact-Usage-Divar`
3. Install dependencies: `pip install pyspark pendulum`
4. Run the script: `python src/main.py`

## Data
- Sample data is provided in `data/sample_data.csv` to demonstrate the pipeline's functionality.

## Results
- Successfully transformed unstructured `get_contact` data into actionable summaries.
- Enabled detection of potential bot activity, supporting efforts to mitigate data crawling for advertising.

## Future Improvements
- Add anomaly detection algorithms (e.g., using statistical thresholds).
- Integrate real-time monitoring for immediate bot detection.
- Enhance with visualization of request trends.