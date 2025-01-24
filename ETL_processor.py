import psycopg2
from datetime import datetime, date
from psycopg2 import sql
import logging

class ETLProcessor:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            database=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.logger = logging.getLogger(__name__)
        self._create_staging_table()
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.logger.info("Database connection closed")
    
    def _execute_query(self, query, params=None, fetch=False):
        """Utility method for executing SQL queries with optional result fetching"""
        cur = None
        try:
            cur = self.conn.cursor()
            cur.execute(query, params or ())
            
            result = None
            if fetch:
                result = cur.fetchall()
            
            self.conn.commit()
            return result
            
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Query failed: {str(e)}")
            raise
        finally:
            if cur:
                cur.close()
    
    def _parse_date(self, date_str, fmt):
        try:
            return datetime.strptime(date_str, fmt).date() if date_str else None
        except ValueError:
            return None

    def _create_staging_table(self):
        """Create staging table if not exists"""
        query = '''
            CREATE TABLE IF NOT EXISTS staging_patients (
                                    Customer_Name varchar(255)  not null,
									Customer_ID varchar(18) not null,
                                    Customer_Open_Date date not null,
                                    Last_Consulted_Date date,
                                    Vaccination_Type char(5),
                                    Doctor_Consulted char(255),
                                    State char(5),
                                    Country char(5),
                                    Post_Code int,
                                    Date_of_Birth date,
                                    Active_Customer char(1),
									PRIMARY KEY (Customer_ID)
            );
        '''
        self._execute_query(query)
        self.logger.info("Staging table created/verified")

    def _create_country_table(self, country_code):
        """Create country-specific table with derived columns"""
        query = sql.SQL('''
            CREATE TABLE IF NOT EXISTS {} (
                Customer_Name VARCHAR(255) NOT NULL,
                Customer_ID VARCHAR(18) NOT NULL,
                Customer_Open_Date DATE NOT NULL,
                Last_Consulted_Date DATE,
                Vaccination_Type CHAR(5),
                Doctor_Consulted VARCHAR(255),
                State CHAR(5),
                Country CHAR(5),
                Post_Code int,
                Date_of_Birth DATE,
                Active_Customer CHAR(1),
                Age INT,
                Days_Since_Last_Consulted_Gt30 BOOLEAN,
                PRIMARY KEY (Customer_ID)
            );
        ''').format(sql.Identifier(f"table_{country_code}"))
        
        self._execute_query(query)
        self.logger.info(f"Created/verified table for country: {country_code}")

    def _validate_record(self, fields):
        """Validate record against business rules"""
        # Mandatory fields check
        mandatory = [fields[2], fields[3], fields[4]]  # Name, ID, Open Date
        if not all(mandatory):
            self.logger.warning("Missing mandatory fields")
            return False

        # Date validation
        try:
            parse_date = lambda f, fmt: datetime.strptime(f, fmt).date() if f else None
            parse_date(str(fields[2]), '%Y-%m-%d')  # Open Date
            if fields[4]: parse_date(str(fields[3]), '%Y-%m-%d')  # Last Consulted
            if fields[10]: parse_date(str(fields[9]), '%Y-%m-%d')  # DOB
        except ValueError as e:
            self.logger.warning(f"Invalid date format: {str(e)}")
            return False

        # Field length validation
        length_checks = [
            (fields[1], 18),   # Customer ID
            (fields[4], 5),    # Vaccination Type
            (fields[6], 5),    # State
            (fields[7], 5)     # Country
        ]
        
        for value, max_length in length_checks:
            if value and len(value) > max_length:
                self.logger.warning(f"Field too long: {value}")
                return False

        return True

    def _load_to_staging(self, file_path):
        """Load validated data into staging table"""

        with open(file_path, mode='r') as file:
            content = file.read().strip()
            rows = content.split('\n')
            batch = []
            for fields in rows[1:]:  
                field = fields.split('|')
                print(field)
                Customer_Name = field[2]
                Customer_ID = field[3]
                Customer_Open_Date = self._parse_date(field[4], '%Y%m%d')
                Last_Consulted_Date = self._parse_date(field[5], '%Y%m%d')
                Vaccination_Type = field[6]
                Doctor_Consulted = field[7]
                State = field[8]
                Country = field[9]
                Post_Code = field[10]
                Date_of_Birth = self._parse_date(field[11], '%Y%m%d')
                Active_Customer = field[12]
                
                insert_query = f"""
                            INSERT INTO staging_patients (Customer_Name, Customer_ID, Customer_Open_Date, Last_Consulted_Date, Vaccination_Type, Doctor_Consulted,
                                                    State, Country, Post_Code, Date_of_Birth, Active_Customer)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """
                records = (Customer_Name, Customer_ID, Customer_Open_Date, Last_Consulted_Date, Vaccination_Type, Doctor_Consulted,
                                                    State, Country, Post_Code, Date_of_Birth, Active_Customer)
                
                if not self._validate_record(list(records)):
                    continue

                batch.append(records)

                # Batch insert every 10000 records
            if len(batch) >= 10000:
                self._execute_query(insert_query, batch)
                batch = [] 
            
            if batch:
                self._execute_query(insert_query, batch)
                print("Executd")
            self.logger.info(f"Loaded {file_path} to staging table")

    def _process_country_data(self, country_code):
        """Process data for a specific country"""
        query = sql.SQL('''
            WITH ranked AS (
                SELECT 
                Customer_Name,
                Customer_ID,
                Customer_Open_Date,
                Last_Consulted_Date,
                Vaccination_Type,
                Doctor_Consulted,
                State,
                Country,
                Post_Code,        
                Date_of_Birth,
                Active_Customer,
                ROW_NUMBER() OVER (
                    PARTITION BY Customer_id 
                    ORDER BY last_consulted_date DESC NULLS LAST
                ) AS rn
                FROM staging_patients
                WHERE Country = %s
            )
            INSERT INTO {} 
            SELECT 
                Customer_Name,
                Customer_ID,
                Customer_Open_Date,
                Last_Consulted_Date,
                Vaccination_Type,
                Doctor_Consulted,
                State,
                Country,
                Post_Code,        
                Date_of_Birth,
                Active_Customer,
                EXTRACT(YEAR FROM AGE(Date_of_Birth)) AS Age,
                (CURRENT_DATE - Last_Consulted_Date) > 30 AS Days_Since_Last_Consulted_Gt30
            FROM ranked
            WHERE rn = 1
            ON CONFLICT (Customer_ID) DO UPDATE SET
                Customer_Name = EXCLUDED.Customer_Name,
                Customer_Open_Date = EXCLUDED.Customer_Open_Date,
                Last_Consulted_Date = EXCLUDED.Last_Consulted_Date,
                Vaccination_Type = EXCLUDED.Vaccination_Type,
                Doctor_Consulted = EXCLUDED.Doctor_Consulted,
                State = EXCLUDED.State,
                Country = EXCLUDED.Country,
                Post_Code = EXCLUDED.Post_Code,
                Date_of_Birth = EXCLUDED.Date_of_Birth,
                Active_Customer = EXCLUDED.Active_Customer,
                Age = EXCLUDED.Age,
                days_since_last_consulted_gt30 = EXCLUDED.days_since_last_consulted_gt30
        ''').format(sql.Identifier(f"table_{country_code}"))
        
        self._execute_query(query, (country_code,))
        self.logger.info(f"Processed data for country: {country_code}")

    def process_file(self, file_path):
        """Main ETL processing method"""
        try:
            # Step 1: Load data to staging
            self._load_to_staging(file_path)
            
            # Step 2: Get unique countries
            countries_rows = self._execute_query(
                "SELECT DISTINCT country FROM staging_patients",
                fetch=True
            )
            countries = [row[0] for row in countries_rows if row[0]]

            # Step 3: Process each country
            for country in countries:
                self._create_country_table(country)
                self._process_country_data(country)
            
            # Step 4: Clear staging
            self._execute_query("TRUNCATE staging_patients")
            
        except Exception as e:
            self.logger.error(f"ETL process failed: {str(e)}")
            raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    with ETLProcessor(
        dbname="hospital",
        user="postgres",
        password="postgres",
        host="127.0.0.1",
        port="5432"
    ) as processor:
        processor.process_file('/Users/gnanaprasanna/Documents/Data_Expert/data-engineer-handbook/bootcamp/materials/1-dimensional-data-modeling/hosipital_assignment/vaccination_data.txt')