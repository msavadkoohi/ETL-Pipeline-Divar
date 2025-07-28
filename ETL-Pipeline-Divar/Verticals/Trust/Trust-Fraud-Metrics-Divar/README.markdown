# ETL Fraud Metrics - Divar

## Overview
This directory contains an ETL pipeline developed during my tenure as a Business Data Analyst in the Trust and Fraud Prevention team at Divar (May 2023 - May 2024). The pipeline calculates fraud-related metrics by aggregating data from various sources, including active users, fraud reports, and CRM data, to assess exposure to fraudulent activities.

## Problem Statement
- **Issue**: Data on user interactions, fraud reports, and blacklisted tokens was fragmented, making it challenging to quantify fraud exposure and events accurately.
- **Goal**: Compute fraud metrics (e.g., fraud event counts, conversion rates) to support the Trust team's efforts in preventing fraud, while also aiding review and operations teams with actionable insights.

## Features
- **Metric Calculation**: Estimates fraud events based on conversion rates and exposure types.
- **Data Aggregation**: Combines data from active users, reported tokens, and blacklisted interactions.
- **Historical Analysis**: Processes data monthly with Jalali calendar support.
- **Integration**: Incorporates CRM-reported fraud events for precision.

## Tools Used
- Python, PySpark, Pendulum

## How to Run
1. Clone the repository: `git clone https://github.com/msavadkoohi/ETL-Pipeline-Divar.git`
2. Navigate to the project directory: `cd ETL-Fraud-Metrics-Divar`
3. Install dependencies: `pip install pyspark pendulum`
4. Run the script: `python src/main.py`

## Data
- Sample data is provided in `data/sample_data.csv` to demonstrate the pipeline's functionality.

## Results
- Provided accurate fraud event metrics, supporting the Trust team's fraud prevention strategies.
- Enabled cross-team collaboration (Trust, Review, Operations) with standardized fraud exposure data.

## Future Improvements
- Add statistical confidence intervals for fraud rate calculations.
- Integrate real-time fraud detection alerts.
- Enhance with visualizations of fraud trends.