import pandas as pd

def join_dataframes(df1, df2, join_column, how='inner'):
    """
    Joins two DataFrames on a specified column.

    Parameters:
    df1 (pd.DataFrame): The first DataFrame.
    df2 (pd.DataFrame): The second DataFrame.
    join_column (str): The column to join on.
    how (str): The type of join to perform. Default is 'inner'. Other options are 'left', 'right', 'outer'.

    Returns:
    pd.DataFrame: The joined DataFrame.
    """
    return pd.merge(df1, df2, on=join_column, how=how)