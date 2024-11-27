from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
import config
import re
import pandas as pd


def get_query_client():
    """
    Creates and returns a `KustoClient` instance for querying the Kusto cluster.

    The client is authenticated using Azure Active Directory (AAD) application key authentication,
    utilizing credentials provided in the `config` module.

    Returns:
    KustoClient: An authenticated KustoClient connected to the specified cluster.
    """
    
    query_kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        config.QUERY_CLUSTER, config.APP_ID, config.APP_KEY, config.AUTHORITY_ID
    )
    return KustoClient(query_kcsb)


def get_all_table_names():
    """
    Retrieves all table names from a Kusto database.

    Returns:
    table_names (list): A list of all table names in the database.
    """

    # Get the names of the tables
    query = ".show tables | project TableName"
    client = get_query_client()
    response = client.execute(config.DATABASE, query)

    # Extract and return the list of table names
    table = response.primary_results[0]
    table_names = [row['TableName'] for row in table.rows]
    
    return table_names


def get_query_columns_from_query(base_query):
    """
    Retrieves the column names resulting from a given Kusto query.

    Parameters:
    base_query (str): The Kusto query to execute.

    Returns:
    columns (list): A list of the column names from the query result.
    """

    client = get_query_client()
    response = client.execute(config.DATABASE, base_query)

    # Extract column names from the response
    table = response.primary_results[0]
    columns = [column.column_name for column in table.columns]

    return columns


def extract_dt(base_query, timestamp_column):
    """
    Extracts the time difference between the two most recent entries from a base query using KQL.
    Parameters:
    - base_query (str): The base KQL query to execute.
    - timestamp_column (str): The name of the timestamp column.
    Returns:
    - str: The time difference between the two most recent timestamps.
    """
    client = get_query_client()
    # Construct the KQL query to calculate the time difference
    query = f"""
    let data = ({base_query});
    data
    | sort by {timestamp_column} desc
    | extend NextTimestamp = next({timestamp_column})
    | where isnotnull(NextTimestamp)
    | take 1
    | project TimeDifference = {timestamp_column} - NextTimestamp
    """
    # Execute the query
    response = client.execute(config.DATABASE, query)

    # Check if the response contains data
    if not response.primary_results or len(response.primary_results) == 0:
        return None  # Or handle the case as needed

    result_table = response.primary_results[0]
    result_df = dataframe_from_result_table(result_table)

    if result_df.empty:
        return None  # Or handle the case as needed

    # Get the TimeDifference value
    time_diff_str = result_df['TimeDifference'][0]

    # Convert the time difference string to a pandas Timedelta object
    time_difference = pd.Timedelta(time_diff_str)

    # Decompose the time delta into days, hours, minutes, and seconds
    days = time_difference.days
    seconds = time_difference.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    # Build the output string
    time_components = []
    if days > 0:
        time_components.append(f"{days}d")
    if hours > 0:
        time_components.append(f"{hours}h")
    if minutes > 0:
        time_components.append(f"{minutes}m")
    if secs > 0:
        time_components.append(f"{secs}s")

    if not time_components:
        return "1d"

    return ' '.join(time_components)



def extract_table_name(base_query, table_names):
    """
    Attempts to extract the table name from the base query.

    Parameters:
    - base_query (str): The KQL query provided by the user.
    - table_names (list): List of all table names in the database.

    Returns:
    - table_name (str): The extracted table name or 'Custom Query' if not found.
    """

    # Remove comments and extra spaces
    query = re.sub(r'//.*', '', base_query).strip()

    # Split the query into tokens
    tokens = re.split(r'\s+|\n+|\|', query)

    # Attempt to find a token that matches a table name
    for token in tokens:
        token = token.strip()
        if token in table_names:
            return token

    # Raise an error if no table name is found in the query
    raise ValueError("The given query does not reference any table in the provided database.")
