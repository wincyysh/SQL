import pandas as pd

def explore_cost_dataframe(file_path):
  pd.set_option('display.max_columns', None)
  pd.set_option('display.max_rows', None)
  df = pd.read_excel(file_path)  
  df = df.iloc[90:132, :2]
  return df

def get_education_level(value):
  """Map education level text to its order number"""
  value_lower = value.lower()
  id = 0
  if 'all' in value_lower:
    id = 5
  elif '4-year' in value_lower:
    id = 6
  elif '2-year' in value_lower:
    id = 4
  return id

def split_dataframe_by_nan(df):
  """
  Split DataFrame into multiple tables based on NaN 
  """
  new_df = pd.DataFrame(columns=['educational_level_id','year', 'cost'])
  for row in range(df.shape[0]):
    if pd.isna(df.iloc[row,1]):
      id = int(get_education_level(df.iloc[row,0]))
    if not pd.isna(df.iloc[row,1]):
      new_df.loc[len(new_df)] = [id, int(df.iloc[row,0][:2])*100+int(df.iloc[row,0][-2:]), df.iloc[row, 1]]
  return new_df
