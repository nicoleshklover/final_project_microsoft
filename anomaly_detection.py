# anomaly_detection.py
from kusto_connection import extract_dt


def generate_series_decomposition_query(base_query, columns, dimension_column=None):
    """
    Generates a KQL query for time series decomposition using series_decompose.
    Decomposes a series to seasonal and trend components.

    Parameters:
    - base_query (str): The base table or query to operate on.
    - columns (list): A list containing [time_column, value_column].
    - dimension_column (str, optional): The name of the dimension column to group by and filter on. If provided, the decomposition will be performed separately for each group defined by this column.

    Returns:
    - decomposition_query (str): A KQL query of the decomposed series.

    Notes:
    - When `dimension_column` is provided, the query includes a parameter placeholder `${dimension_column}` for dynamic substitution at execution time.
    """

    # Extract the time and value columns
    time_column = columns[0]
    value_column = columns[1]

    # Construct the where clause to filter the data based on the dimension column's value
    where_clause = ''
    if dimension_column:
        variable_placeholder = f'${{{dimension_column}}}'
        where_clause = f'| where tostring({dimension_column}) == "{variable_placeholder}"'

    decomposition_query = f"""
    let dt = {extract_dt(base_query, time_column)};
    let min_t = toscalar({base_query} | summarize min({time_column}));
    let max_t = toscalar({base_query} | summarize max({time_column}));
    let decomposed_data = {base_query}
    | make-series num=avg(todouble({value_column})) on {time_column} from min_t to max_t step dt
    | extend (Baseline, Seasonal, Trend, Residual) = series_decompose(num, -1, 'linefit')
    | mv-expand {time_column} to typeof(datetime), num to typeof(real), Seasonal to typeof(real), Trend to typeof(real)
    | project {time_column}, Trend, Seasonal;
    let joined_data = {base_query}
    | join kind=leftouter decomposed_data on {time_column};
    let decomposed_by_{dimension_column} = joined_data
    | project {time_column}, {value_column}, Seasonal, Trend{', tostring(' + dimension_column + ')' if dimension_column else ''}
    {where_clause};
    decomposed_by_{dimension_column}
    """.strip()

    return decomposition_query



def generate_series_decompose_anomalies_query(base_query, columns, dimension_column=None):
    """
    Generates a KQL query for detecting anomalies using series_decompose_anomalies.
    Creates a query that performs time series decomposition to identify anomalies.

    Parameters:
    - base_query (str): The base table or query to operate on.
    - columns (list): A list containing [time_column, value_column].
    - dimension_column (str, optional): The name of the dimension column to group by and filter on. If provided, the decomposition and anomaly detection will be performed separately for each group defined by this column.

    Returns:
    - anomalies_query (str): A KQL query that performs anomaly detection.
    
    Notes:
    - When `dimension_column` is provided, the query includes a parameter placeholder `${dimension_column}` for dynamic substitution at execution time.
    """

    time_column = columns[0]
    value_column = columns[1]

    # Construct the where clause to filter the data based on the dimension column's value
    where_clause = ''
    if dimension_column:
        variable_placeholder = f'${{{dimension_column}}}'
        where_clause = f'| where tostring({dimension_column}) == "{variable_placeholder}"'

    anomalies_query = f"""
    let dt = {extract_dt(base_query, time_column)};
    let min_t = toscalar({base_query} | summarize min({time_column}));
    let max_t = toscalar({base_query} | summarize max({time_column}));
    let anomalies_data = {base_query}
    | make-series num=avg(todouble({value_column})) on {time_column} from min_t to max_t step dt
    | extend (Anomalies, AnomalyScore) = series_decompose_anomalies(num, todouble("${{AnomalyThreshold}}"), -1, 'linefit')
    | mv-expand {time_column} to typeof(datetime), Anomalies to typeof(real), AnomalyScore to typeof(real)
    | project {time_column}, Anomalies, AnomalyScore;
    let joined_data = {base_query}
    | join kind=leftouter anomalies_data on {time_column};
    let anomalies_by_{dimension_column} = joined_data
    | project {time_column}, {value_column}, Anomalies, AnomalyScore{', tostring(' + dimension_column + ')' if dimension_column else ''}
    {where_clause};
    anomalies_by_{dimension_column}
    """.strip()

    return anomalies_query


def generate_anomaly_count_per_segment_query(base_query, columns, dimension_columns=None):
    """
    Generates a KQL query that counts anomalies per segment (columns combinations).
    The function handles different numbers of dimensions and returns a query that can be used to identify segments with the highest number of anomalies.

    Parameters:
    - base_query (str): The base table or query to operate on.
    - columns (list): A list containing [time_column, value_column].
    - dimension_columns (str, optional): A list of dimension column names to group by. Anomalies will be counted for each combination of dimension values.

    Returns:
    - anomaly_count_query (str or None): The generated KQL query string that counts anomalies per segment. Returns None if no dimensions are provided, indicating that the panel should be skipped.
    """

    time_column = columns[0]
    value_column = columns[1]

    # Ensure dimension_columns is a list
    dimension_columns = dimension_columns or []

    # If there are no dimensions, return None to indicate the panel should be skipped
    if not dimension_columns:
        return None

    # Prepare the 'by' clause for grouping
    by_clause = ', '.join(dimension_columns)

    anomaly_count_query = f"""
    let dt = {extract_dt(base_query, time_column)};
    let min_t = toscalar({base_query} | summarize min({time_column}));
    let max_t = toscalar({base_query} | summarize max({time_column}));
    let anomaly_scores = {base_query}
    | make-series num=avg(todouble({value_column})) on {time_column} from min_t to max_t step dt
    | extend Anomalies = series_decompose_anomalies(num, todouble("${{AnomalyThreshold}}"), -1, 'linefit')
    | mv-expand {time_column} to typeof(datetime), Anomalies to typeof(real)
    | project {time_column}, Anomalies;
    {base_query}
    | join kind=leftouter anomaly_scores on {time_column}
    | where Anomalies == 1
    | summarize AnomalyCount = count() by Anomalies, {by_clause}
    | project AnomalyCount, {by_clause}
    | sort by AnomalyCount desc""".strip()

    return anomaly_count_query


def generate_anomaly_count_bar_chart(base_query, columns, dimension_column):
    """
    Generates a KQL query for counting anomalies per a single dimension.

    Parameters:
    - base_query (str): The base table or query to operate on.
    - columns (list): A list containing [time_column, value_column].
    - dimension_column (str, optional): The name of the dimension column to group by. Anomalies will be counted for each unique value in this column.

    Returns:
    - barplot_query (str): A KQL query string that counts anomalies for each category per a specified dimension.
    """

    time_column = columns[0]
    value_column = columns[1]

    barplot_query = f"""
    let dt = {extract_dt(base_query, time_column)};
    let min_t = toscalar({base_query} | summarize min({time_column}));
    let max_t = toscalar({base_query} | summarize max({time_column}));
    let anomalies_data = {base_query}
    | make-series num=avg(todouble({value_column})) on {time_column} from min_t to max_t step dt
    | extend Anomalies = series_decompose_anomalies(num, todouble("${{AnomalyThreshold}}"), -1, 'linefit')
    | mv-expand {time_column} to typeof(datetime), Anomalies to typeof(real)
    | project {time_column}, Anomalies;
    let joined_data = {base_query}
    | join kind=leftouter anomalies_data on {time_column}
    | where Anomalies == 1;
    let anomalies_by_{dimension_column} = joined_data
    | summarize AnomalyCount = count() by tostring({dimension_column})
    | extend Category = tostring({dimension_column});
    anomalies_by_{dimension_column}
    """.strip()

    return barplot_query


def generate_dimension_anomaly_barchart(base_query, columns, dimension_columns=None):
    """
    Generates a KQL query for counting anomalies per dimension.

    Parameters:
    - base_query (str): The base table or query to operate on.
    - columns (list): A list containing [time_column, value_column].
    - dimension_columns (list, optional): A list of dimension column names. Anomalies will be counted for each dimension.

    Returns:
    - barchart_query (str): A KQL query string that counts anomalies for each dimension.
    """

    time_column = columns[0]
    value_column = columns[1]

    # Ensure dimension_columns is a list
    dimension_columns = dimension_columns or []

    # If there are no dimensions, return None to indicate the panel should be skipped
    if not dimension_columns:
        return None

    # Construct the projection of dimension columns
    dimension_projection = ''.join([f", {col}" for col in dimension_columns])

    barchart_query = f"""
    let dt = {extract_dt(base_query, time_column)};
    let min_t = toscalar({base_query} | summarize min({time_column}));
    let max_t = toscalar({base_query} | summarize max({time_column}));
    let anomalies_data = {base_query}
    | make-series num=avg(todouble({value_column})) on {time_column} from min_t to max_t step dt
    | extend Anomalies = series_decompose_anomalies(num, todouble("${{AnomalyThreshold}}"), -1, 'linefit')
    | mv-expand {time_column} to typeof(datetime), Anomalies to typeof(real)
    | project {time_column}, Anomalies;
    let joined_data = {base_query}
    | join kind=leftouter anomalies_data on {time_column}
    | where Anomalies == 1
    | project {time_column}, Anomalies{dimension_projection};
    """

    # Add anomalies summarization for each dimension
    for dimension in dimension_columns:
        barchart_query += f"""
    let anomalies_by_{dimension} = joined_data
    | summarize AnomalyCount = count() by Dimension = '{dimension}';
    """

    # Combine the results using union
    union_queries = ' | union '.join([f'anomalies_by_{dimension}' for dimension in dimension_columns])
    barchart_query += f"""
    {union_queries}
    """

    return barchart_query.strip()
