# StreamWatch ETL Pipeline

## 📌 Project Overview
This project is an **ETL (Extract, Transform, Load) pipeline** designed to process large-scale JSON log data (10GB+) from TV program viewership. The pipeline extracts raw data, transforms it into meaningful insights, and stores the results in **CSV format** and a **MySQL database** for further analysis.

## 🚀 Features
- **Scalable ETL Pipeline**: Handles large JSON log data efficiently using **PySpark**.
- **Data Processing & Transformation**:
  - Selects relevant fields for analysis.
  - Categorizes TV programs by content type (Movies, Sports, Kids, etc.).
  - Computes key statistics such as total watch time per user.
  - Counts the number of unique devices per user.
- **Batch Processing**: Supports dynamic date range selection for bulk data processing.
- **Optimized Performance**: Configured **SparkSession** for better memory management and processing speed.
- **Multiple Data Storage Options**:
  - Saves processed data in **CSV format**.
  - Loads cleaned data into a **MySQL database** for querying and reporting.

## 🛠️ Tech Stack
- **Programming Language**: Python (PySpark, Pandas)
- **Big Data Processing**: Apache Spark
- **Database**: MySQL
- **File Formats**: JSON (input), CSV (output)
- **Infrastructure**: Local file system, Hadoop configurations
