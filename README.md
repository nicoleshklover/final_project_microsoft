# Anomaly Detection Tool

## Introduction

This project focuses on creating an accessible tool for detecting and analyzing anomalies, seasonality and trends within time series data for Microsoft’s Excel for the Web team. The tool is a Python based solution that dynamically generates Grafana dashboards from Kusto Query Language (KQL) inputs.

Given a KQL query representing a time series table, the tool creates a Grafana dashboard with visualizations that allow the exploration of the time series in terms of anomaly detection, trends, and seasonality.

---

## Features

- **Dynamic Time Series Analysis**: Automatically detect anomalies, trends and seasonality in time series data using KQL functions such as `series_decompose` and `series_decompose_anomalies`.
- **Flexible Input**: Supports various KQL inputs, including filters or specific dimensions.
- **Grafana Integration**: Creates visually appealing and interactive dashboards for comprehensive analysis.
- **Error Handling**: Robust error detection for issues such as authentication failures.
- **Threshold Tuning**: Allows users to dynamically adjust anomaly thresholds directly in Grafana.

---

## Key Scripts

### `config.py`

Handles environment variable loading and configuration setup. Key variables include:

- Azure authentication (`APP_ID`, `APP_KEY`, `AUTHORITY_ID`)
- Cluster and database details (`QUERY_CLUSTER`, `DATABASE`)
- Grafana API details (`API_TOKEN`, `GRAFANA_URL`)
- Base KQL query (`BASE_QUERY`)

### `kusto_connection.py`

Manages the connection to ADX and provides helper functions:

- `get_query_client()`: Authenticates and returns a Kusto client.
- `get_all_table_names()`: Retrieves all table names from the specified database.
- `extract_table_name()`: Extracts the table name from a given KQL query.
- `extract_dt()`: Calculates time differences between consecutive rows.

### `anomaly_detection.py`

Generates KQL queries for various analyses:

- `generate_series_decomposition_query`: Decomposes series into trend and seasonal components.
- `generate_series_decompose_anomalies_query`: Detects anomalies based on decomposed components.
- `generate_anomaly_count_per_segment_query`: Summarizes anomalies by dimension.
- `generate_anomaly_count_bar_chart`: Visualizes anomalies for specific dimensions.

### `dashboard.py`

Handles the creation and configuration of Grafana dashboards:

- `create_dashboard`: Sends API requests to Grafana to create dashboards.
- `add_row`: Adds organizational rows to the dashboard layout.

### `main.py`

Orchestrates the entire process:

- Validates configuration.
- Extracts columns and dimensions from the KQL base query.
- Generates Grafana variables and queries.
- Invokes `create_dashboard` to build and deploy the dashboard.

---

## How to Use

1. **Set Up Environment Variables**:

   - Create a `.env` file with the required variables (e.g., Azure credentials, Grafana details).
   - Note: The input query currently has to be ordered: time stamp column, value column, other dimensions.

2. **Install Dependencies**:

   - Use `pip` to install required libraries, such as `azure-kusto-data` and `requests`.

3. **Run the Tool**:

   - Execute `main.py` to generate the dashboard. Ensure the `.env` file and Python scripts are in the same directory.

4. **Access the Dashboard**:

   - Log in to your Grafana instance and view the newly created dashboard.

---

## Future Improvements

- **Real Time Alerts**: Enable notifications for detected anomalies.
- **Dynamic Column Detection**: Support tables with varying column arrangements.
- **Enhanced UI**: Develop a graphical interface for configuring inputs and thresholds.
- **Dimension Prioritization**: Automatically filter irrelevant dimensions for better focus.
