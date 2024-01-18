# 🚀 Airflow API Data Processing DAG 🚀

Welcome to the **Save API Response Hourly Main DAG**! This Airflow DAG is designed to fetch data from an API, process it, and then upload it to Google Cloud Storage (GCS) and BigQuery. 📊

## 📅 Schedule
This DAG runs hourly, ensuring that your data is up-to-date and processed regularly.

## 📁 Directory Structure
Data fetched from the API is organized in a date-specific directory structure within the `Data` folder. This ensures a systematic arrangement for easy management.

## 🕒 Time Intervals
The DAG fetches data for the past hour (`begin_interval` to `end_interval`) to keep your data current.

## 📂 Variables
Sensitive information such as API credentials, key file paths, GCS bucket names, and BigQuery table IDs are securely stored in Airflow Admin Variables.

## 🤖 Task 1: Fetch Data
The first task, named `fetch_data`, involves fetching data from the API, converting it to JSONL, and saving it locally. The data is organized in date-specific directories.

## ☁️ Task 2: Upload to GCS
The second task, named `upload_to_gcs`, takes the locally saved JSONL files, uploads them to Google Cloud Storage, and processes them using PySpark. The processed data is saved in Parquet format within a date-specific directory structure in GCS. Simultaneously, the data is loaded into a BigQuery external table.

## 🛠️ Technologies Used
- **Airflow**: Orchestrating the entire workflow.
- **PySpark**: Transforming and processing the JSONL files.
- **Google Cloud Storage (GCS)**: Storing and managing the raw and processed data.
- **BigQuery**: Storing the processed data in an external table.

## 🚨 Exception Handling
Robust exception handling ensures that any API request failure is caught, preventing the DAG from failing silently.

## 🧑‍💻 How to Run
1. Ensure all required variables are set in the Airflow Admin.
2. Schedule the DAG to run at your preferred frequency.
3. Sit back and watch your data processing magic happen!

Feel free to customize the DAG according to your needs. Happy Airflowing! 🌐✨
