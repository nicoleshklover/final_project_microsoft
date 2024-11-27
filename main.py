# main.py

import config
from anomaly_detection import (
    generate_series_decomposition_query,
    generate_series_decompose_anomalies_query,
    generate_anomaly_count_per_segment_query,
    generate_anomaly_count_bar_chart,
    generate_dimension_anomaly_barchart,
)
from dashboard import (
    create_dashboard,
      add_row
)
from kusto_connection import (
    get_query_columns_from_query,
    get_all_table_names,
    extract_table_name
)


def main():
    """
    Orchestrates the creation of dashboards in Grafana based on ADX data.

    Steps:
    - Validates required environment variables.
    - Sets up logging.
    - Retrieves columns from the base query.
    - Generates variable definitions and queries for panels.
    - Calls the function to create a dashboard in Grafana.
    """
    try:
        # Ensure all necessary environment variables are set
        required_env_vars = [
            "APP_ID", "APP_KEY", "AUTHORITY_ID",
            "API_TOKEN", "GRAFANA_URL", "DATASOURCE_NAME", "BASE_QUERY"
        ]
        for var in required_env_vars:
            if not getattr(config, var, None):
                raise EnvironmentError(f"Environment variable {var} not set.")

        # Get the datasource name and base query from config
        datasource_name = config.DATASOURCE_NAME
        base_query = config.BASE_QUERY

        try:
            # Get all table names from the Kusto database
            table_names = get_all_table_names()

            # Extract table name from base_query
            dashboard_title = extract_table_name(base_query, table_names)

            # Get columns from the base_query
            all_columns = get_query_columns_from_query(base_query)

            # Ensure there are at least two columns: time and value
            if len(all_columns) < 2:
                print("Not enough columns in query result.")
                return

            # Extract dimension columns
            dimension_columns = all_columns[2:] if len(all_columns) > 2 else []
            columns = all_columns[:2]

            # Initialize lists for variable definitions and queries
            variable_definitions = []
            queries_and_titles = []


            # Define the AnomalyThreshold variable as a Textbox
            anomaly_threshold_definition = {
                "type": "textbox",
                "name": "AnomalyThreshold",
                "label": "Anomaly Threshold",
                "hide": 0,
                "query": "",
                "refresh": 2,
                "current": {
                    "text": "1.5",
                    "value": "1.5"
                },
                "skipUrlSync": False
            }

            # Append the AnomalyThreshold variable to the variable_definitions list
            variable_definitions.append(anomaly_threshold_definition)

            # Add 'Time Series Plot' row
            add_row(queries_and_titles, 'Time Series Plot')

            # Generate time series plots queries without dimension filtering
            query = generate_series_decomposition_query(base_query, columns)
            title = f'Series Decomposition'
            queries_and_titles.append({
                'query': query,
                'title': title,
                'type': 'timeseries',
                'repeat': None
            })

            # Series Decompose Anomalies Panel
            query_anomalies = generate_series_decompose_anomalies_query(base_query, columns)
            title_anomalies = f'Anomalies'
            queries_and_titles.append({
                'query': query_anomalies,
                'title': title_anomalies,
                'type': 'timeseries',
                'repeat': None
            })

            # Add 'Anomalies Count Per Dimension' row
            add_row(queries_and_titles, 'Anomalies Count Per Dimension')

            # Generate Anomalies Count Per Dimension bar chart panel
            query_anomalies_per_dim = generate_dimension_anomaly_barchart(base_query, columns, dimension_columns=dimension_columns)

            if query_anomalies_per_dim:
                title_anomalies_per_dim = f'Anomalies Per Dimension'
                queries_and_titles.append({
                    'query': query_anomalies_per_dim,
                    'title': title_anomalies_per_dim,
                    'type': 'barchart',
                    'repeat': None
                })

            # Add 'Anomalies Count' row
            add_row(queries_and_titles, 'Anomalies Count')

            # Create variables for each dimension column
            for dimension_column in dimension_columns:
                variable_name = dimension_column

                variable_definition = {
                    "type": "query",
                    "name": variable_name,
                    "hide": 0,
                    "datasource": {"type": "grafana-azure-data-explorer-datasource", "uid": datasource_name},
                    "refresh": "",
                    "multi": True,  # Allow multiple selections
                    "includeAll": True,  # Include 'All' option
                    "query": f"{base_query}\n| project {dimension_column} = tostring({dimension_column})\n| distinct {dimension_column}",
                    "sort": 0,
                    "current": {},
                    "definition": f"{base_query}\n| project {dimension_column} = tostring({dimension_column})\n| distinct {dimension_column}",
                    "label": variable_name,
                    "skipUrlSync": False,
                    "multiFormat": "regex",
                    "allValue": ".*"
                }

                variable_definitions.append(variable_definition)

                # Anomalies Count Bar Chart Panel
                query_bar_chart = generate_anomaly_count_bar_chart(
                    base_query, columns, dimension_column=dimension_column
                )
                title_bar_chart = f'Anomalies Count by {variable_name}'
                queries_and_titles.append({
                    'query': query_bar_chart,
                    'title': title_bar_chart,
                    'type': 'barchart',
                    'repeat': None
                })

            # Add 'Anomalies Count Per Segment' row
            add_row(queries_and_titles, 'Anomalies Count Per Segment')

            # Anomaly Count per Segment Panel
            anomaly_count_query = generate_anomaly_count_per_segment_query(
                base_query, columns, dimension_columns=dimension_columns
            )
            if anomaly_count_query:
                if len(dimension_columns) >= 2:
                    title_anomaly_count = f'Anomaly Count per Segment by {", ".join(dimension_columns)}'
                else: 
                    # one dimension
                    title_anomaly_count = f'Anomaly Count by {dimension_columns[0]}'
                queries_and_titles.append({
                    'query': anomaly_count_query,
                    'title': title_anomaly_count,
                    'type': 'table',
                    'repeat': None 
                })

            # Add 'Anomalies Score' row
            add_row(queries_and_titles, 'Anomalies Score')

            for dimension_column in dimension_columns:
                variable_name = dimension_column

                # Series Decompose Anomalies Panel
                query_anomalies = generate_series_decompose_anomalies_query(
                    base_query, columns, dimension_column=dimension_column
                )
                title_anomalies = f'{variable_name} - ${{{variable_name}}}'
                queries_and_titles.append({
                    'query': query_anomalies,
                    'title': title_anomalies,
                    'type': 'timeseries',
                    'repeat': variable_name
                })

            # Add 'Series Decomposition' row
            add_row(queries_and_titles, 'Series Decomposition')

            for dimension_column in dimension_columns:
                variable_name = dimension_column

                # Series Decomposition Panel
                query = generate_series_decomposition_query(
                    base_query, columns, dimension_column=dimension_column
                )
                title = f'{variable_name} - ${{{variable_name}}}'
                queries_and_titles.append({
                    'query': query,
                    'title': title,
                    'type': 'timeseries',
                    'repeat': variable_name
                })

            # Call create_dashboard function to generate the dashboard in Grafana
            create_dashboard(
                queries_and_titles,
                variable_definitions=variable_definitions,
                dashboard_title=dashboard_title,
                datasource_name=datasource_name,
                database=config.DATABASE
            )

        except Exception as e:
            # Logs the error during processing of the query
            print(f"An error occurred while processing the query: {e}")

    except Exception as e:
        # Handles errors occurring during the initial setup or outside query processing.
        print(f"An exception occurred: {e}")

if __name__ == "__main__":
    main()