# dashboard.py

import json
import requests
import config
import re


def add_row(queries_and_titles, title):
    """
    Adds a row to the dashboard configuration.

    Parameters:
    - queries_and_titles (list): The list of queries and titles for the dashboard panels.
    - title (str): The title of the row to add.
    """

    queries_and_titles.append({
        'title': title,
        'type': 'row',
        'collapsed': False,
        'panels': []
    })


def create_dashboard(queries_and_titles, variable_definitions, dashboard_title, datasource_name, database):
    """
    Creates a Grafana dashboard (JSON format) based on the provided queries and configurations, and sends it to the Grafana API.

    Parameters:
    - queries_and_titles (list): A list of dictionaries containing queries and titles for the panels.
    - variable_definitions (list): A list of variable definitions for Grafana templating.
    - dashboard_title (str): The title of the dashboard to be created.
    - datasource_name (str): The UID of the Grafana data source to use.
    - database (str): The name of the database to query.
    """
    # Initialize the dashboard structure
    dashboard_uid = re.sub(r'\W+', '-', dashboard_title.lower()).strip('-')

    dashboard = {
        "uid": dashboard_uid,
        "title": dashboard_title,
        "panels": [],
        "templating": {
            "list": variable_definitions
        },
        "timezone": "browser",
        "schemaVersion": 40,
        "version": 1,
        "refresh": "",
        "time": {
            "from": "now-7d",
            "to": "now"
        },
        "timepicker": {},
        "weekStart": ""
    }

    # Initialize panel ID and grid position counters
    panel_id = 1
    y_position = 0 # Used to position panels vertically

    for item in queries_and_titles:
        if item['type'] == 'row':
            # Handle row panel
            row_panel = {
                "type": "row",
                "title": item['title'],
                "collapsed": item.get('collapsed', False),
                "gridPos": {
                    "h": 1, # Height of the row
                    "w": 24, # Full width
                    "x": 0, # Start at left
                    "y": y_position
                },
                "panels": [],
                "id": panel_id
            }
            dashboard['panels'].append(row_panel)
            panel_id += 1
            y_position += 1  # Rows typically have a height of 1
        else:
            # Handle regular panels
            panel = {
                "type": item['type'],
                "title": item['title'],
                "id": panel_id,
                "gridPos": {
                    "h": 8,  # Standard height for panels
                    "w": 24, 
                    "x": 0, 
                    "y": y_position
                },
                "datasource": {
                    "type": "grafana-azure-data-explorer-datasource",
                    "uid": datasource_name
                },
                "targets": [
                    {
                        "refId": "A",
                        "datasource": {
                            "type": "grafana-azure-data-explorer-datasource",
                            "uid": datasource_name
                        },
                        "database": database,
                        "queryType": "KQL",
                        "querySource": "raw",
                        "rawMode": True,
                        "resultFormat": "time_series" if item['type'] == 'timeseries' else "table",
                        "query": item['query']
                    }
                ],
                "fieldConfig": {
                    "defaults": {},
                    "overrides": []
                },
                "options": {
                    "legend": {
                        "displayMode": "list",
                        "placement": "bottom",
                        "showLegend": True
                    },
                    "tooltip": {
                        "mode": "single",
                        "sort": "none"
                    }
                },
                "pluginVersion": "5.0.7"
            }

            # If the panel should be repeated (for variables)
            if item.get('repeat'):
                panel['repeat'] = item['repeat']
                panel['repeatDirection'] = 'h' # Horizontal repetition
                panel['maxPerRow'] = 6  # Maximum panels per row (adjustable)

            # Add specific options based on panel type
            if item['type'] == 'timeseries':
                # Configuration for timeseries panels
                panel['fieldConfig']['defaults']['custom'] = {
                    "drawStyle": "line",
                    "lineInterpolation": "linear",
                    "lineWidth": 1,
                    "fillOpacity": 0,
                    "pointSize": 5,
                    "showPoints": "auto",
                    "barWidthFactor": 0.6,
                    "gradientMode": "none"
                }
            elif item['type'] == 'barchart':
                # Configuration for bar chart panels
                panel['fieldConfig']['defaults']['custom'] = {
                    "drawStyle": "bar",
                    "barAlignment": 0,
                    "barWidthFactor": 0.97,
                    "fillOpacity": 80
                }
                panel['options']['stacking'] = "none"
                panel['options']['orientation'] = "auto"
            elif item['type'] == 'table':
                # Configuration for table panels
                panel['options'] = {
                    "showHeader": True,
                    "fontSize": "100%",
                    "sortBy": []
                }
                panel['fieldConfig']['defaults']['align'] = "auto"

            # Add the configured panel to the dashboard
            dashboard['panels'].append(panel)
            panel_id += 1
            y_position += 8  # Increment y_position based on panel height

    # Prepare the dashboard JSON
    dashboard_json = {
        "dashboard": dashboard,
        "folderId": 0,
        "overwrite": True
    }

    # Convert the dashboard dictionary to JSON
    dashboard_json_str = json.dumps(dashboard_json)

    # Get Grafana API details from environment variables
    grafana_url = config.GRAFANA_URL
    api_token = config.API_TOKEN

    # Set up the HTTP headers with the API token for authentication
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }

    # Send the dashboard JSON to Grafana via the API
    response = requests.post(
        f"{grafana_url}/api/dashboards/db",
        headers=headers,
        data=dashboard_json_str
    )

    # Check the response
    if response.status_code == 200:
        print(f"Dashboard '{dashboard_title}' created successfully")
    else:
        print(f"Failed to create dashboard '{dashboard_title}': {response.text}")