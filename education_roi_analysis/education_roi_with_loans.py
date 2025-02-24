import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np

class LoanROICalculator:
  def __init__(self, db_params):
    self.db_params = db_params
    self.conn = None
    self.cur = None
    self.interest_rate = round(0.0668, 4)  # 6.68% in decimal form
    self.loan_term_years = 10
    self.loan_coverage = round(0.70, 2)  # 70% of education cost is financed

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

  def calculate_monthly_loan_payment(self, principal):
    """Calculate monthly loan payment with full precision"""
    r = self.interest_rate / 12  # Monthly interest rate
    n = self.loan_term_years * 12  # Total number of payments
    if r == 0:
      return principal / n
    
    return principal * (r * (1 + r)**n) / ((1 + r)**n - 1)

  def calculate_total_loan_cost(self, principal):
    """Calculate total cost of loan including interest with 2 decimal precision"""
    monthly_payment = self.calculate_monthly_loan_payment(principal)
    total_payments = monthly_payment * self.loan_term_years * 12
    return round(total_payments, 2)

  def create_roi_loan_table(self):
    """Create the ROI calculation table"""
    try:
      self.cur.execute("""
        DROP TABLE IF EXISTS education_roi_with_loans CASCADE;
        
        CREATE TABLE education_roi_with_loans (
          roi_id SERIAL PRIMARY KEY,
          educational_level_id INT REFERENCES dim_educational_level(educational_level_id),
          year_id INT REFERENCES dim_year(year_id),
          demographic_id INT REFERENCES dim_demographic(demographics_id),
          
          -- Cost Metrics (all NUMERIC(10,2) for 2 decimal places)
          total_education_cost NUMERIC(10,2),
          loan_amount NUMERIC(10,2),
          total_loan_cost NUMERIC(10,2),
          monthly_loan_payment NUMERIC(10,2),
          
          -- Earnings Metrics
          annual_earnings NUMERIC(10,2),
          baseline_earnings NUMERIC(10,2),
          net_monthly_earnings NUMERIC(10,2),
          
          -- ROI Metrics
          total_investment NUMERIC(10,2),
          earnings_premium_monthly NUMERIC(10,2),
          net_roi_after_loans_10yr NUMERIC(10,2),
          roi_percentage_after_loans NUMERIC(10,2),
          debt_to_income_ratio NUMERIC(10,2),
          years_to_break_even NUMERIC(10,2),
          
          UNIQUE(educational_level_id, year_id, demographic_id)
        );
      """)
      self.conn.commit()
      print("ROI table created successfully")
    except Exception as e:
      self.conn.rollback()
      print(f"Error creating ROI table: {str(e)}")
      raise

  def calculate_roi_with_loans(self):
    """Calculate ROI metrics with 2 decimal precision"""
    try:
      self.cur.execute("""
        WITH BaselineEarnings AS (
          SELECT 
            year_id,
            demographic_id,
            ROUND(annual_earnings::numeric, 2) as hs_annual_earnings
          FROM Median_annual_earnings
          WHERE educational_level_id = 2
        ),
        CostData AS (
          SELECT 
            educational_level_id,
            year_id,
            ROUND(CASE 
              WHEN educational_level_id = 4 THEN cost * 2  -- 2 years for Associate's
              WHEN educational_level_id = 6 THEN cost * 4  -- 4 years for Bachelor's
              WHEN educational_level_id = 8 THEN cost * 6  -- 6 years for Master's
              ELSE cost
            END::numeric, 2) as total_education_cost
          FROM expenditure_per_full_time_student
        )
        SELECT 
          e.educational_level_id,
          e.year_id,
          e.demographic_id,
          ROUND(e.annual_earnings::numeric, 2) as annual_earnings,
          ROUND(b.hs_annual_earnings::numeric, 2) as baseline_earnings,
          ROUND(c.total_education_cost::numeric, 2) as total_education_cost
        FROM Median_annual_earnings e
        LEFT JOIN BaselineEarnings b 
          ON e.year_id = b.year_id 
          AND e.demographic_id = b.demographic_id
        LEFT JOIN CostData c 
          ON e.educational_level_id = c.educational_level_id 
          AND e.year_id = c.year_id
        WHERE e.annual_earnings > 0;
      """)
      
      rows = self.cur.fetchall()
      
      for row in rows:
        educational_level_id, year_id, demographic_id, annual_earnings, baseline_earnings, total_education_cost = row
        
        total_education_cost = float(total_education_cost if total_education_cost else 0)
        annual_earnings = float(annual_earnings if annual_earnings else 0)
        baseline_earnings = float(baseline_earnings if baseline_earnings else 0)
        
        loan_amount = total_education_cost * self.loan_coverage
        monthly_loan_payment = self.calculate_monthly_loan_payment(loan_amount)
        total_loan_cost = self.calculate_total_loan_cost(loan_amount)
        
        net_monthly_earnings = (annual_earnings / 12) - monthly_loan_payment
        earnings_premium_monthly = (annual_earnings - baseline_earnings) / 12
        
        total_investment = total_education_cost + (total_loan_cost - loan_amount)
        net_roi_after_loans_10yr = (annual_earnings * 10) - total_investment
        
        roi_percentage_after_loans = (
          ((net_roi_after_loans_10yr / total_investment) * 100) if total_investment > 0 else 0
        )
        debt_to_income_ratio = (
          (monthly_loan_payment * 12 / annual_earnings) if annual_earnings > 0 else 0
        )
        years_to_break_even = (
          total_investment / (annual_earnings - baseline_earnings) 
          if baseline_earnings and (annual_earnings > baseline_earnings) else 0
        )

        self.cur.execute("""
          INSERT INTO education_roi_with_loans (
            educational_level_id, year_id, demographic_id,
            total_education_cost, loan_amount, total_loan_cost, monthly_loan_payment,
            annual_earnings, baseline_earnings, net_monthly_earnings,
            total_investment, earnings_premium_monthly,
            net_roi_after_loans_10yr, roi_percentage_after_loans,
            debt_to_income_ratio, years_to_break_even
          ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          ON CONFLICT (educational_level_id, year_id, demographic_id) DO UPDATE
          SET 
            total_education_cost = EXCLUDED.total_education_cost,
            loan_amount = EXCLUDED.loan_amount,
            total_loan_cost = EXCLUDED.total_loan_cost,
            monthly_loan_payment = EXCLUDED.monthly_loan_payment,
            annual_earnings = EXCLUDED.annual_earnings,
            baseline_earnings = EXCLUDED.baseline_earnings,
            net_monthly_earnings = EXCLUDED.net_monthly_earnings,
            total_investment = EXCLUDED.total_investment,
            earnings_premium_monthly = EXCLUDED.earnings_premium_monthly,
            net_roi_after_loans_10yr = EXCLUDED.net_roi_after_loans_10yr,
            roi_percentage_after_loans = EXCLUDED.roi_percentage_after_loans,
            debt_to_income_ratio = EXCLUDED.debt_to_income_ratio,
            years_to_break_even = EXCLUDED.years_to_break_even;
        """, (
          educational_level_id, year_id, demographic_id,
          total_education_cost, loan_amount, total_loan_cost, monthly_loan_payment,
          annual_earnings, baseline_earnings, net_monthly_earnings,
          total_investment, earnings_premium_monthly,
          net_roi_after_loans_10yr, roi_percentage_after_loans,
          debt_to_income_ratio, years_to_break_even
        ))
        self.cur.execute("DELETE FROM education_roi_with_loans WHERE total_education_cost = 0")

      self.conn.commit()
      print("ROI calculations completed successfully")
    except Exception as e:
      self.conn.rollback()
      print(f"Error calculating ROI: {str(e)}")
      raise

  def get_roi_summary(self):
    """Get summary with all numbers rounded to 2 decimal places"""
    try:
      self.cur.execute("""
        SELECT 
          el.education_level_name,
          ROUND(AVG(r.total_education_cost)::numeric, 2) as avg_education_cost,
          ROUND(AVG(r.loan_amount)::numeric, 2) as avg_loan_amount,
          ROUND(AVG(r.monthly_loan_payment)::numeric, 2) as avg_monthly_payment,
          ROUND(AVG(r.annual_earnings)::numeric, 2) as avg_annual_earnings,
          ROUND(AVG(r.net_monthly_earnings)::numeric, 2) as avg_net_monthly_earnings,
          ROUND(AVG(r.roi_percentage_after_loans)::numeric, 2) as avg_roi_percentage,
          ROUND(AVG(r.debt_to_income_ratio * 100)::numeric, 2) as avg_debt_to_income_percent,
          ROUND(AVG(r.years_to_break_even)::numeric, 2) as avg_years_to_break_even
        FROM education_roi_with_loans r
        JOIN dim_educational_level el 
          ON r.educational_level_id = el.educational_level_id
        GROUP BY 
          el.education_level_name,
          el.education_level_order
        ORDER BY 
          el.education_level_order;
      """)
      results = self.cur.fetchall()
      return results
    except Exception as e:
      print(f"Error getting ROI summary: {str(e)}")
      raise

def main():
  db_params = {
    'dbname': 'your_dbname',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'your_host',
    'port': 'your_port'
  }

  calculator = LoanROICalculator(db_params)

  try:
    calculator.connect()
    calculator.create_roi_loan_table()
    calculator.calculate_roi_with_loans()
    # results = calculator.get_roi_summary()
    # print("\nDetailed ROI Summary Including Student Loans:")
    # print("-------------------------------------------")
    # for row in results:
    #   print(f"\nEducation Level: {row[0]}")
    #   print(f"Average Education Cost: ${row[1]:,.2f}")
    #   print(f"Average Loan Amount: ${row[2]:,.2f}")
    #   print(f"Average Monthly Payment: ${row[3]:,.2f}")
    #   print(f"Average Annual Earnings: ${row[4]:,.2f}")
    #   print(f"Average Net Monthly Earnings: ${row[5]:,.2f}")
    #   print(f"ROI Percentage (After Loans): {row[6]:.2f}%")
    #   print(f"Debt-to-Income Ratio: {row[7]:.2f}%")
    #   print(f"Years to Break Even: {row[8]:.2f}")

  except Exception as e:
    print(f"Error in main execution: {str(e)}")
  finally:
    calculator.disconnect()

if __name__ == "__main__":
  main()