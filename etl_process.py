#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import csv
from datetime import datetime
import sys

class DataWarehouseETL:
    def __init__(self, 
                 host='localhost', 
                 port=5432, 
                 database='datawarehouse',
                 user='dwuser', 
                 password='dwpassword'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()
            print("✅ Successfully connected to PostgreSQL database")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
        
    def setup_database(self):
        """Setup data warehouse database structure"""
        print("Setting up data warehouse database structure...")
        
        # Execute original table structure creation
        self.execute_sql_file('ressources/stores-and-products.sql')
        
        # Execute data warehouse structure creation
        self.execute_sql_file('datawarehouse_setup.sql')
        
    def execute_sql_file(self, filename):
        """Execute SQL file"""
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                sql_script = file.read()
                
                # PostgreSQL supports direct multi-statement execution
                self.cursor.execute(sql_script)
                self.conn.commit()
                
        except Exception as e:
            print(f"Error executing SQL file {filename}: {e}")
            self.conn.rollback()
            
    def populate_dim_date(self, start_year=2019, end_year=2020):
        """Populate date dimension table"""
        print("Populating date dimension table...")
        
        dates_data = []
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                for day in range(1, 32):
                    try:
                        date_obj = datetime(year, month, day)
                        date_key = int(date_obj.strftime("%Y%m%d"))
                        quarter = (month - 1) // 3 + 1
                        month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                                     'July', 'August', 'September', 'October', 'November', 'December']
                        
                        dates_data.append((
                            date_key,
                            date_obj.strftime("%Y-%m-%d"),
                            year,
                            quarter,
                            month,
                            day,
                            date_obj.weekday() + 1,
                            month_names[month],
                            f"Q{quarter}"
                        ))
                    except ValueError:
                        continue
        
       
        query = '''
            INSERT INTO DimDate 
            (DateKey, FullDate, Year, Quarter, Month, Day, DayOfWeek, MonthName, QuarterName)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (DateKey) DO NOTHING
        '''
        
        self.cursor.executemany(query, dates_data)
        self.conn.commit()
        print(f"✅ Inserted {len(dates_data)} date records")
        
    def populate_dim_shop(self):
        """Populate shop dimension table"""
        print("Populating shop dimension table...")
        
        query = '''
        SELECT DISTINCT 
            s.ShopID,
            s.Name as ShopName,
            c.CityID,
            c.Name as CityName,
            r.RegionID,
            r.Name as RegionName,
            co.CountryID,
            co.Name as CountryName
        FROM Shop s
        JOIN City c ON s.CityID = c.CityID
        JOIN Region r ON c.RegionID = r.RegionID
        JOIN Country co ON r.CountryID = co.CountryID
        '''
        
        self.cursor.execute(query)
        shops = self.cursor.fetchall()
        
        # Insert shop dimension data
        insert_query = '''
            INSERT INTO DimShop 
            (ShopID, ShopName, CityID, CityName, RegionID, RegionName, CountryID, CountryName)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        self.cursor.executemany(insert_query, shops)
        self.conn.commit()
        print(f"✅ Inserted {len(shops)} shop records")
        
    def populate_dim_product(self):
        """Populate product dimension table"""
        print("Populating product dimension table...")
        
        query = '''
        SELECT DISTINCT
            a.ArticleID,
            a.Name as ArticleName,
            a.Price,
            pg.ProductGroupID,
            pg.Name as ProductGroupName,
            pf.ProductFamilyID,
            pf.Name as ProductFamilyName,
            pc.ProductCategoryID,
            pc.Name as ProductCategoryName
        FROM Article a
        JOIN ProductGroup pg ON a.ProductGroupID = pg.ProductGroupID
        JOIN ProductFamily pf ON pg.ProductFamilyID = pf.ProductFamilyID
        JOIN ProductCategory pc ON pf.ProductCategoryID = pc.ProductCategoryID
        '''
        
        self.cursor.execute(query)
        products = self.cursor.fetchall()
        
        # Insert product dimension data
        insert_query = '''
            INSERT INTO DimProduct 
            (ArticleID, ArticleName, Price, ProductGroupID, ProductGroupName, 
             ProductFamilyID, ProductFamilyName, ProductCategoryID, ProductCategoryName)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        self.cursor.executemany(insert_query, products)
        self.conn.commit()
        print(f"✅ Inserted {len(products)} product records")
        
    def load_sales_data(self):
        """Load sales data to fact table"""
        print("Loading sales data to fact table...")
        
        # Create lookup dictionaries for better performance
        shop_lookup = {}
        product_lookup = {}
        
        # Build shop lookup dictionary
        self.cursor.execute("SELECT ShopKey, ShopName FROM DimShop")
        shops = self.cursor.fetchall()
        for shop_key, shop_name in shops:
            shop_lookup[shop_name] = shop_key
            
        # Build product lookup dictionary
        self.cursor.execute("SELECT ProductKey, ArticleName FROM DimProduct")
        products = self.cursor.fetchall()
        for product_key, article_name in products:
            product_lookup[article_name] = product_key
        
        # Read sales data CSV file
        batch_size = 1000
        current_batch = []
        processed_count = 0
        
        with open('ressources/sales.csv', 'r', encoding='iso-8859-1', errors='ignore') as file:
            reader = csv.DictReader(file, delimiter=';')
            
            for row_num, row in enumerate(reader, 1):
                try:
                    # Parse date
                    date_str = row['Date']
                    date_parts = date_str.split('.')
                    date_obj = datetime(int(date_parts[2]), int(date_parts[1]), int(date_parts[0]))
                    date_key = int(date_obj.strftime("%Y%m%d"))
                    
                    # Find dimension keys
                    shop_name = row['Shop']
                    article_name = row['Article']
                    
                    if shop_name in shop_lookup and article_name in product_lookup:
                        shop_key = shop_lookup[shop_name]
                        product_key = product_lookup[article_name]
                        
                        # Process revenue data (German format)
                        revenue_str = row['Revenue'].replace(',', '.')
                        revenue = float(revenue_str)
                        
                        current_batch.append((
                            date_key,
                            shop_key,
                            product_key,
                            int(row['Sold']),
                            revenue
                        ))
                        
                        if len(current_batch) >= batch_size:
                            self.insert_sales_batch(current_batch)
                            processed_count += len(current_batch)
                            current_batch = []
                            
                        if row_num % 10000 == 0:
                            print(f"Processed {row_num} rows...")
                            
                except Exception as e:
                    print(f"Error processing row {row_num}: {e}")
                    continue
            
            # Insert last batch
            if current_batch:
                self.insert_sales_batch(current_batch)
                processed_count += len(current_batch)
                
        print(f"✅ Sales data loading completed, processed {processed_count} valid records")
        
    def insert_sales_batch(self, batch_data):
        """Batch insert sales data"""
        query = '''
            INSERT INTO FactSales (DateKey, ShopKey, ProductKey, QuantitySold, Revenue)
            VALUES (%s, %s, %s, %s, %s)
        '''
        self.cursor.executemany(query, batch_data)
        self.conn.commit()
        
    def run_etl(self):
        """Execute complete ETL process"""
        print("Starting ETL process...")
        
        try:
            self.connect()
            self.setup_database()
            self.populate_dim_date()
            self.populate_dim_shop()
            self.populate_dim_product()
            self.load_sales_data()
            
            print("✅ ETL process completed!")
            self.print_summary()
            
        except Exception as e:
            print(f"❌ Error during ETL process: {e}")
            if self.conn:
                self.conn.rollback()
            
    def print_summary(self):
        """Print data loading summary"""
        print("\n=== Data Warehouse Loading Summary ===")
        
        tables = ['DimDate', 'DimShop', 'DimProduct', 'FactSales']
        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.cursor.fetchone()[0]
            print(f"{table}: {count:,} records")
            
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    etl = DataWarehouseETL()
    try:
        etl.run_etl()
    finally:
        etl.close() 