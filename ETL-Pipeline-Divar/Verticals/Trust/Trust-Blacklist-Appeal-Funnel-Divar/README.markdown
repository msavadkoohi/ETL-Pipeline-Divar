# ETL Blacklist Appeal Funnel - Divar

## Overview
This directory contains an ETL pipeline developed during my tenure as a Business Data Analyst at Divar (May 2023 - May 2024). The pipeline focuses on creating a funnel analysis for blacklist appeal processes, consolidating data from blacklist, unblacklist, judgements, and user events to track the appeal journey historically.

## Problem Statement
- **Issue**: Data on blacklist appeals, user events, and related judgements were fragmented, making it hard to analyze the appeal funnel or track user interactions effectively.
- **Goal**: Provide a historical view of appeal processes to identify patterns, such as repeat appeals or user engagement with the appellant page.

## Features
- **Funnel Tracking**: Combines blacklist, unblacklist, and appeal data to create a comprehensive appeal funnel.
- **Historical Storage**: Stores data with timestamps to enable time-based analysis.
- **Data Transformation**: Applies timezone conversion (UTC to Asia/Tehran) and ranks appeal events.
- **User Event Integration**: Incorporates user interactions (e.g., viewing appellant pages) into the analysis.

## Tools Used
- Python, PySpark

## How to Run
1. Clone the repository: `git clone https://github.com/msavadkoohi/ETL-Pipeline-Divar.git`
2. Navigate to the project directory: `cd ETL-Blacklist-Appeal-Funnel-Divar`
3. Install dependencies: `pip install pyspark`
4. Run the script: `python src/main.py`

## Data
- Sample data is provided in `data/sample_data.csv` to demonstrate the pipeline's functionality.

## Results
- Enabled tracking of blacklist appeal funnels, revealing user engagement and appeal outcomes.
- Facilitated historical analysis of appeal processes for improved decision-making.

## Future Improvements
- Add visualizations to highlight funnel stages (e.g., using Matplotlib).
- Integrate additional user event types for deeper insights.