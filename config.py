# config.py

import os
from dotenv import load_dotenv

# Load variables from .env file into environment
load_dotenv(override=True)

# Database configurations
DATABASE = os.getenv("DATABASE")
QUERY_CLUSTER = os.getenv("QUERY_CLUSTER")

# Authentication credentials
APP_ID = os.getenv("APP_ID")
APP_KEY = os.getenv("APP_KEY")
AUTHORITY_ID = os.getenv("AUTHORITY_ID")

# Grafana API configurations
API_TOKEN = os.getenv('API_TOKEN')
GRAFANA_URL = os.getenv('GRAFANA_URL')
DATASOURCE_NAME = os.getenv('DATASOURCE_NAME')

# Base query to be used for the dashboard
BASE_QUERY = os.getenv("BASE_QUERY")
