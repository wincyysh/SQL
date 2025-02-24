import psycopg2
from psycopg2.extras import execute_values
from extract_tabn334_10 import explore_cost_dataframe, split_dataframe_by_nan, get_education_level

class CostDataLoader:
  def __init__(self, db_params):
    self.db_params = db_params
    self.conn = None
    self.cur = None

  def connect(self):
    try:
      self.conn = psycopg2.connect(**self.db_params)
      self.cur = self.conn.cursor()
      print("Database connection established")
    except Exception as e:
      print(f"Error connecting to database: {str(e)}")
      raise

  def disconnect(self):
    if self.cur:
      self.cur.close()
    if self.conn:
      self.conn.close()
      print("Database connection closed")

  def create_schema(self):
    """Create the complete database schema"""
    try:
      self.cur.execute("""
        DROP TABLE IF EXISTS expenditure_per_full_time_student CASCADE;
      """)

      self.cur.execute("""                 
        CREATE TABLE IF NOT EXISTS expenditure_per_full_time_student (
            expenditure_id SERIAL PRIMARY KEY,
            educational_level_id INT REFERENCES dim_educational_level(educational_level_id),
            year_id INT REFERENCES dim_year(year_id),
            cost FLOAT
        );
      """)

      self.conn.commit()
      print("Database schema created successfully")
    except Exception as e:
      self.conn.rollback()
      print(f"Error creating schema: {str(e)}")
      raise

 
  def load_data(self, file_path):
    """Load data from Excel file into database tables using optimized row/column mapping"""
    df = explore_cost_dataframe(file_path)
    table = split_dataframe_by_nan(df)
    for row_idx in range(0, len(table)):
      education_level_id = table.iloc[row_idx,0]
      year_value = table.iloc[row_idx,1]
      self.cur.execute(
        "INSERT INTO dim_year (year) VALUES (%s) ON CONFLICT (year) DO NOTHING",
        (int(year_value),)
      )
      self.cur.execute(
        "SELECT year_id FROM dim_year WHERE year = %s",
        (int(year_value),)
      )
      year_id = self.cur.fetchone()[0]
      cost = table.iloc[row_idx,2]
      self.cur.execute("""
          INSERT INTO Expenditure_per_full_time_student 
          (educational_level_id, year_id, cost)
          VALUES (%s, %s, %s)
      """, (int(education_level_id), year_id, float(cost)))

    self.conn.commit()
    print("Data loaded successfully")


def main():
  db_params = {
    'dbname': 'your_dbname',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'your_host',
    'port': 'your_port'
  }
  file_path = 'tabn334_10.xlsx'
  loader = CostDataLoader(db_params)
  try:
    loader.connect()
    loader.create_schema()
    loader.load_data(file_path)
  except Exception as e:
    print(f"Error in main execution: {str(e)}")
  finally:
    loader.disconnect()

if __name__ == "__main__":
  main()