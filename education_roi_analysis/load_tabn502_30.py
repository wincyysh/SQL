import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from extract_tabn502_30 import explore_dataframe, explore_and_split_excel

class EducationDataLoader:
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
          DROP TABLE IF EXISTS fact_educational_attainment CASCADE;
          DROP TABLE IF EXISTS fact_education_completion CASCADE;
          DROP TABLE IF EXISTS dim_demographic CASCADE;
          DROP TABLE IF EXISTS dim_educational_level CASCADE;
          DROP TABLE IF EXISTS dim_year CASCADE;
          DROP TABLE IF EXISTS Median_annual_earnings CASCADE;
          DROP TABLE IF EXISTS gender_table CASCADE;
          DROP TABLE IF EXISTS race_ethnicity CASCADE;
          DROP TABLE IF EXISTS educational_attainment CASCADE;
      """)

      self.cur.execute("""
          -- Dimensions tables
          CREATE TABLE IF NOT EXISTS dim_year (
              year_id SERIAL PRIMARY KEY,
              year INTEGER NOT NULL UNIQUE,
              CONSTRAINT year_range CHECK (year >= 2005 AND year <= 2022)
          );

          CREATE TABLE IF NOT EXISTS dim_educational_level (
              educational_level_id SERIAL PRIMARY KEY,
              education_level_name VARCHAR(100) NOT NULL UNIQUE,
              education_level_order INTEGER NOT NULL
          );
                      
          CREATE TABLE IF NOT EXISTS gender_table (
              gender_id SERIAL PRIMARY KEY,
              gender_name VARCHAR(10),
              gender_code CHAR(1) UNIQUE CHECK (gender_code IN ('F', 'M', 'A'))
          );
                      
          CREATE TABLE IF NOT EXISTS race_ethnicity (
              race_ethnicity_id SERIAL PRIMARY KEY,
              race_ethnicity_name VARCHAR(10),
              race_ethnicity_code CHAR(1) UNIQUE CHECK (race_ethnicity_code IN ('A', 'B', 'H', 'W', 'M', 'U'))
          );

          CREATE TABLE IF NOT EXISTS dim_demographic (
              demographics_id SERIAL PRIMARY KEY,
              gender_id INT REFERENCES gender_table(gender_id),
              race_ethnicity_id INT REFERENCES race_ethnicity(race_ethnicity_id),
              UNIQUE(gender_id, race_ethnicity_id)
          );
                      
          CREATE TABLE IF NOT EXISTS educational_attainment (
              educational_attainment_id SERIAL PRIMARY KEY,
              educational_level_id INT REFERENCES dim_educational_level(educational_level_id),
              demographic_id INT REFERENCES dim_demographic(demographics_id),
              year_id INT REFERENCES dim_year(year_id),
              percentage FLOAT
          );
                                      
          CREATE TABLE IF NOT EXISTS Median_annual_earnings (
              median_annual_earnings_id SERIAL PRIMARY KEY,
              educational_level_id INT REFERENCES dim_educational_level(educational_level_id),
              demographic_id INT REFERENCES dim_demographic(demographics_id),
              year_id INT REFERENCES dim_year(year_id),
              annual_earnings FLOAT
              -- No UNIQUE constraint
          );
      """)

      self.conn.commit()
      print("Database schema created successfully")
    except Exception as e:
      self.conn.rollback()
      print(f"Error creating schema: {str(e)}")
      raise

  def insert_year_data(self, df):
    """Insert years from DataFrame into dim_year table"""
    try:
      years = [int(col) for col in df.columns[1:] if str(col).isdigit()]
      year_values = [(year,) for year in years]
      
      execute_values(
        self.cur,
        "INSERT INTO dim_year (year) VALUES %s ON CONFLICT (year) DO NOTHING",
        year_values
      )
      
      print("Years inserted successfully")
      return years
    except Exception as e:
      print(f"Error inserting years: {str(e)}")
      raise

  def insert_demographic_combinations(self):
    """Insert demographic combinations by cross joining gender and race"""
    try:
      self.cur.execute("""
          WITH gender_race AS (
              SELECT g.gender_id, r.race_ethnicity_id
              FROM gender_table g
              CROSS JOIN race_ethnicity r
          )
          SELECT gender_id, race_ethnicity_id 
          FROM gender_race
      """)
      
      demographic_values = [(g_id, r_id) for g_id, r_id in self.cur.fetchall()]
      
      execute_values(
        self.cur,
        "INSERT INTO dim_demographic (gender_id, race_ethnicity_id) VALUES %s ON CONFLICT (gender_id, race_ethnicity_id) DO NOTHING",
        demographic_values
      )

      self.conn.commit()
      print("Demographic combinations inserted successfully")

    except Exception as e:
      self.conn.rollback()
      print(f"Error inserting demographic combinations: {str(e)}")
      raise

  def insert_education_level_data(self):
    """Insert education levels into dim_educational_level table"""
    try:
      education_levels = [
        ('Less than high school completion', 1),
        ('High school completion', 2),
        ('Some college, no degree', 3),
        ('Associate degree', 4),
        ('Median annual earnings, all education levels', 5),
        ("Bachelor's degree", 6),
        ("Bachelor's degree or higher", 7),
        ("Master's or higher degree", 8)
      ]
      
      execute_values(
        self.cur,
        "INSERT INTO dim_educational_level (education_level_name, education_level_order) VALUES %s",
        education_levels
      )
      
      print("Education levels inserted successfully")
    except Exception as e:
      print(f"Error inserting education levels: {str(e)}")
      raise

  def insert_race_ethnicity_data(self):
    """Insert race and ethnicity data into race_ethnicity table"""
    try:
      race_ethnicity_values = [
        ('Asian', 'A'),
        ('Black', 'B'),
        ('Hispanic', 'H'),
        ('White', 'W'),
        ('Union', 'U')
      ]
      
      execute_values(
        self.cur,
        "INSERT INTO race_ethnicity (race_ethnicity_name, race_ethnicity_code) VALUES %s",
        race_ethnicity_values
      )
      
      print("Race ethnicity data inserted successfully")
    except Exception as e:
      print(f"Error inserting race ethnicity data: {str(e)}")
      raise

  def insert_gender_data(self):
    """Insert gender data into gender_table"""
    try:
      gender_values = [
        ('Female', 'F'),
        ('Male', 'M'),
        ('All', 'A')
      ]
      
      execute_values(
        self.cur,
        "INSERT INTO gender_table (gender_name, gender_code) VALUES %s",
        gender_values
      )
      
      print("Gender data inserted successfully")
    except Exception as e:
      print(f"Error inserting gender data: {str(e)}")
      raise

  def insert_dimension_data(self):
    """Insert initial dimension data"""
    try:
      self.insert_education_level_data()
      self.insert_race_ethnicity_data()
      self.insert_gender_data()
      self.insert_demographic_combinations()
      self.conn.commit()
      print("Dimension data inserted successfully")

    except Exception as e:
      self.conn.rollback()
      print(f"Error inserting dimension data: {str(e)}")
      raise

  def get_demographic_id(self, gender_code, race_code):
    self.cur.execute("""
        SELECT d.demographics_id
        FROM dim_demographic d
        JOIN gender_table g ON d.gender_id = g.gender_id
        JOIN race_ethnicity r ON d.race_ethnicity_id = r.race_ethnicity_id
        WHERE g.gender_code = %s AND r.race_ethnicity_code = %s
    """, (gender_code, race_code))
    
    result = self.cur.fetchone()
    if result:
      return result[0]
    else:
      raise ValueError(f"No demographic ID found for gender '{gender_code}' and race '{race_code}'")

  def map_education_level(self, value):
    if pd.isna(value) or not isinstance(value, str):
      return None

    value_lower = value.lower()
    id = 0
    if 'all education levels' in value_lower:
      id = 5
      return id
    elif 'less than' in value_lower:
      id = 1
      return id
    elif 'high school' in value_lower:
      id = 2
      return id
    elif 'no degree' in value_lower:
      id = 3
      return id
    elif 'associate' in value_lower:
      id = 4
      return id
    elif "bachelor's degree" in value_lower:
      id = 6
      return id
    elif "bachelor's or higher" in value_lower:
      id = 7
      return id
    elif "master's" in value_lower or 'master' in value_lower:
      id = 8
      return id

    return None

  def parse_demographic_info(self, demographic_info):
    gender_code = 'A'  
    race_code = 'U'    
    
    if pd.notna(demographic_info):
      if 'Total' in demographic_info:
        gender_code = 'A'
        race_code = 'U'
      elif 'white' in demographic_info.lower():
        race_code = 'W'
        gender_code = 'A'
      elif 'black' in demographic_info.lower():
        race_code = 'B'
        gender_code = 'A'
      elif 'asian' in demographic_info.lower():
        race_code = 'A'
        gender_code = 'A'
      elif 'hispanic' in demographic_info.lower():
        race_code = 'H'
        gender_code = 'A'
      elif 'female' in demographic_info.lower():
        gender_code = 'F' 
        race_code = 'U'
      else:
        gender_code = 'M'
        race_code = 'U'
    
    return gender_code, race_code


  def load_data(self, file_path):
    df = explore_dataframe(file_path)
    earnings_tables, attainment_tables = explore_and_split_excel(df)
    all_years = self.insert_year_data(df)
    year_mapping = {}
    for col_idx in range(0, len(all_years)):
      year_value = all_years[col_idx]
      self.cur.execute(
        "SELECT year_id FROM dim_year WHERE year = %s",
        (int(year_value),)
      )
      year_id = self.cur.fetchone()[0]
      year_mapping[col_idx] = year_id
    for table_idx in range(0,len(attainment_tables)):
      earnings_table = earnings_tables[table_idx]
      attainment_table = attainment_tables[table_idx]
      demographic_info = earnings_table.iloc[0, 0]
      gender_code, race_code = self.parse_demographic_info(demographic_info)
      demographic_id = self.get_demographic_id(gender_code, race_code)
      for row_idx in range(1,len(earnings_table)):
        value = earnings_table.iloc[row_idx, 0]  
        education_level_id = self.map_education_level(value)
        earnings_row = earnings_table.iloc[row_idx, 1:]        
        for col_idx in range(len(earnings_row)):
          year_id = year_mapping[col_idx]
          earnings_value = earnings_row.iloc[col_idx]
          if pd.notna(earnings_value):
            earnings_value = float(earnings_value)
          else:
            earnings_value = 0
          self.cur.execute("""
              INSERT INTO Median_annual_earnings 
              (educational_level_id, demographic_id, year_id, annual_earnings)
              VALUES (%s, %s, %s, %s)
          """, (education_level_id, demographic_id, year_id, earnings_value))
      for row_idx in range(len(attainment_table)):
        value = attainment_table.iloc[row_idx, 0]
        education_level_id = self.map_education_level(value)
        attainment_row = None
        attainment_row = attainment_table.iloc[row_idx,1:]
        for col_idx in range(len(attainment_row)):
          year_id = year_mapping[col_idx]
          attainment_value = attainment_row.iloc[col_idx]
          if pd.notna(attainment_value):
            attainment_value = float(attainment_value)
          else:
            attainment_value = 0
          self.cur.execute("""
              INSERT INTO educational_attainment 
              (educational_level_id, demographic_id, year_id, percentage)
              VALUES (%s, %s, %s, %s)
          """, (education_level_id, demographic_id, year_id, attainment_value))

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

  file_path = 'tabn502_30.xlsx'

  loader = EducationDataLoader(db_params)

  try:
    loader.connect()
    loader.create_schema()
    loader.insert_dimension_data()
    loader.load_data(file_path)

  except Exception as e:
    print(f"Error in main execution: {str(e)}")
  finally:
    loader.disconnect()

if __name__ == "__main__":
  main()