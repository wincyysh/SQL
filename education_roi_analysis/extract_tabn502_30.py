import pandas as pd
import re

def explore_dataframe(file_path):
  pd.set_option('display.max_columns', None)
  pd.set_option('display.max_rows', None)
  df = pd.read_excel(file_path, skiprows=2)
  df.drop(3, axis=0, inplace=True)
  df = df[[col for col in df.columns if not isinstance(col, float)]]
  df = df[[col for col in df.columns if '.' not in col]]
  df = df.replace('‡', pd.NA)

  return df

def split_dataframe_by_nan_rows(df):
  """
  Split DataFrame into multiple tables based on NaN rows
  A row is considered a split point if all columns from index 2 onwards are NaN
  """
  tables = []
  current_table = []
  for idx, row in df.iterrows():
    is_split_row = row[2:].isna().all()
    
    if is_split_row:
      if len(current_table)>10:
        tables.append(pd.DataFrame(current_table))
        current_table = []
      current_table = []
      current_table.append(row)
    else:
      current_table.append(row)
  
  if len(current_table)>10:
    tables.append(pd.DataFrame(current_table))
  
  for i, table in enumerate(tables):
    table.reset_index(drop=True, inplace=True)

    table.iat[0, 0] = re.sub(r'[^a-zA-Z0-9_]', '_', str(table.iat[0, 0]))

  return tables


def split_tables_by_marker(tables):
  """
  Split tables based on the 'Percent, all education levels' marker.
  Returns two lists: earnings tables and attainment tables.
  
  Parameters:
    tables (list): List of pandas DataFrames
  
  Returns:
    tuple: (earnings_tables, attainment_tables)
  """
  earnings_tables = []
  attainment_tables = []
  
  for table in tables:
    if len(table) < 2:
      continue

    marker_idx = None
    for idx in range(len(table)):
      cell_value = str(table.iloc[idx, 0]).strip() if not pd.isna(table.iloc[idx, 0]) else ""
      if "percent, all education levels" in cell_value.lower():
        marker_idx = idx
        break
    
    if marker_idx is not None:
      earnings_part = table.iloc[:marker_idx].copy().reset_index(drop=True)
      attainment_part = table.iloc[marker_idx:].copy().reset_index(drop=True)
      
      if len(earnings_part) > 1:
        earnings_tables.append(earnings_part)
      if len(attainment_part) > 1:
        attainment_tables.append(attainment_part)
    else:
      header_text = ' '.join([str(val).lower() for val in table.iloc[0:3, 0] if not pd.isna(val)])
      
      if "median annual earnings" in header_text:
        earnings_tables.append(table)
      elif "percent" in header_text:
        attainment_tables.append(table)
      else:
        earnings_tables.append(table)
  
  return earnings_tables, attainment_tables


def explore_and_split_excel(df):
  """
  Read Excel file and split into categorized tables
  Returns:
  tuple: (earnings_tables, attainment_tables)
  """
  tables = split_dataframe_by_nan_rows(df)
  earnings_tables, attainment_tables = split_tables_by_marker(tables)
  return earnings_tables, attainment_tables
