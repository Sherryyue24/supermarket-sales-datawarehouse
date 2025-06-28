#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OLAP Query Module - Universal Analysis Function
Provides multidimensional data analysis with dynamic granularity control
"""

import psycopg2
import pandas as pd
import sys

class OLAPAnalyzer:
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
        
        # Define dimension hierarchies and corresponding SQL fields
        self.geo_hierarchy = {
            'country': 's.CountryName',
            'region': 's.RegionName', 
            'city': 's.CityName',
            'shop': 's.ShopName'
        }
        
        self.time_hierarchy = {
            'year': 'd.Year',
            'quarter': 'd.Quarter',
            'month': 'd.Month', 
            'day': 'd.Day'
        }
        
        self.product_hierarchy = {
            'productCategory': 'p.ProductCategoryName',
            'productFamily': 'p.ProductFamilyName',
            'productGroup': 'p.ProductGroupName',
            'article': 'p.ArticleName'
        }
        
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
            print("✅ Successfully connected to PostgreSQL database")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
        
    def execute_query(self, query, params=None):
        """Execute query and return DataFrame"""
        if not self.conn:
            self.connect()
            
        try:
            if params:
                return pd.read_sql_query(query, self.conn, params=params)
            else:
                return pd.read_sql_query(query, self.conn)
        except Exception as e:
            print(f"Query execution failed: {e}")
            return pd.DataFrame()
    
    def analysis(self, geo='region', time='quarter', product='productGroup', year=None):
        """
        Universal OLAP analysis function with dynamic granularity control
        
        Parameters:
        - geo: Geographic granularity ('country', 'region', 'city', 'shop')
        - time: Time granularity ('year', 'quarter', 'month', 'day')
        - product: Product granularity ('productCategory', 'productFamily', 'productGroup', 'article')
        - year: Optional year filter
        
        Returns: DataFrame with multidimensional analysis results
        """
        
        # Validate parameters
        if geo not in self.geo_hierarchy:
            raise ValueError(f"Invalid geo parameter. Must be one of: {list(self.geo_hierarchy.keys())}")
        if time not in self.time_hierarchy:
            raise ValueError(f"Invalid time parameter. Must be one of: {list(self.time_hierarchy.keys())}")
        if product not in self.product_hierarchy:
            raise ValueError(f"Invalid product parameter. Must be one of: {list(self.product_hierarchy.keys())}")
        
        # Build SQL query with dynamic granularity
        geo_field = self.geo_hierarchy[geo]
        time_field = self.time_hierarchy[time]
        product_field = self.product_hierarchy[product]
        
        query = f"""
        SELECT 
            COALESCE(CAST({geo_field} AS TEXT), '[Total]') as geo_dimension,
            COALESCE(CAST({time_field} AS TEXT), '[Total]') as time_dimension,
            COALESCE(CAST({product_field} AS TEXT), '[Total]') as product_dimension,
            GROUPING({geo_field}) as geo_grouping,
            GROUPING({time_field}) as time_grouping,
            GROUPING({product_field}) as product_grouping,
            SUM(f.QuantitySold) as total_quantity,
            SUM(f.Revenue) as total_revenue,
            COUNT(*) as transaction_count,
            AVG(f.Revenue / NULLIF(f.QuantitySold, 0)) as avg_unit_price,
            CASE 
                WHEN GROUPING({geo_field}) = 0 AND GROUPING({time_field}) = 0 AND GROUPING({product_field}) = 0 THEN 'Detail Level'
                WHEN GROUPING({geo_field}) = 0 AND GROUPING({time_field}) = 0 AND GROUPING({product_field}) = 1 THEN 'By Geo + Time'
                WHEN GROUPING({geo_field}) = 0 AND GROUPING({time_field}) = 1 AND GROUPING({product_field}) = 0 THEN 'By Geo + Product'
                WHEN GROUPING({geo_field}) = 1 AND GROUPING({time_field}) = 0 AND GROUPING({product_field}) = 0 THEN 'By Time + Product'
                WHEN GROUPING({geo_field}) = 0 AND GROUPING({time_field}) = 1 AND GROUPING({product_field}) = 1 THEN 'By Geography Only'
                WHEN GROUPING({geo_field}) = 1 AND GROUPING({time_field}) = 0 AND GROUPING({product_field}) = 1 THEN 'By Time Only'
                WHEN GROUPING({geo_field}) = 1 AND GROUPING({time_field}) = 1 AND GROUPING({product_field}) = 0 THEN 'By Product Only'
                WHEN GROUPING({geo_field}) = 1 AND GROUPING({time_field}) = 1 AND GROUPING({product_field}) = 1 THEN 'Grand Total'
                ELSE 'Unknown Level'
            END as aggregation_level
        FROM FactSales f
        JOIN DimShop s ON f.ShopKey = s.ShopKey
        JOIN DimDate d ON f.DateKey = d.DateKey
        JOIN DimProduct p ON f.ProductKey = p.ProductKey
        """
        
        params = []
        if year:
            query += " WHERE d.Year = %s"
            params.append(year)
            
        query += f"""
        GROUP BY CUBE({geo_field}, {time_field}, {product_field})
        ORDER BY GROUPING({geo_field}), GROUPING({time_field}), GROUPING({product_field}),
                 COALESCE(CAST({geo_field} AS TEXT), 'ZZZZ'), 
                 COALESCE(CAST({time_field} AS TEXT), 'ZZZZ'), 
                 COALESCE(CAST({product_field} AS TEXT), 'ZZZZ')
        """
        
        result_df = self.execute_query(query, params if params else None)
        
        if not result_df.empty:
            # Add metadata about the analysis
            print(f"\n📊 OLAP Analysis Results (CUBE - All Aggregation Levels)")
            print(f"Geographic Granularity: {geo}")
            print(f"Time Granularity: {time}")
            print(f"Product Granularity: {product}")
            if year:
                print(f"Year Filter: {year}")
            print(f"Total Records: {len(result_df)}")
            print("\nAggregation Levels Generated:")
            for level in result_df['aggregation_level'].unique():
                count = len(result_df[result_df['aggregation_level'] == level])
                print(f"  - {level}: {count} records")
            print("-" * 60)
            
        return result_df
    
    def generate_cross_table(self, geo='region', time='quarter', product='productGroup', year=2019, metric='total_quantity'):
        """
        Generate cross table 
        
        Parameters:
        - geo, time, product: Dimension granularities
        - year: Year filter
        - metric: Metric to display ('total_quantity', 'total_revenue', 'transaction_count')
        """
        
        print(f"\n📊 Cross Table")
        print(f"Rows: {geo} + {time} (multi-level), Columns: {product}")
        print(f"Metric: {metric}, Year: {year}")
        print("=" * 70)
        
        # Get cross table data using GROUPING SETS for efficient aggregation
        data = self.get_cross_table_data(geo=geo, time=time, product=product, year=year)
        
        if data.empty:
            print("No data available for cross table")
            return None
        
        try:
            # Extract detail level data for pivot table generation
            detail_data = data[data['aggregation_type'] == 'Detail'].copy()
            
            if detail_data.empty:
                print("No detail data available for pivot table")
                return data
            
            # Create pivot table with multi-level rows and complete aggregations
            pivot_table = detail_data.pivot_table(
                index=['geo_dimension', 'time_dimension'],
                columns='product_dimension', 
                values=metric,
                fill_value=0,
                aggfunc='sum',
                margins=True,  # Adds automatic totals
                margins_name='TOTAL'
            )
            
            # Fix NaN issue in TOTAL column by manually calculating row totals
            if 'TOTAL' in pivot_table.columns:
                # For each row, calculate the sum of all product columns (excluding TOTAL)
                product_cols = [col for col in pivot_table.columns if col != 'TOTAL']
                pivot_table['TOTAL'] = pivot_table[product_cols].sum(axis=1)
            
            # Fix NaN issue in TOTAL row by manually calculating column totals
            if 'TOTAL' in pivot_table.index.get_level_values(0):
                # For each column, calculate the sum of all non-total rows
                for col in pivot_table.columns:
                    # Get all non-total rows
                    non_total_mask = pivot_table.index.get_level_values(0) != 'TOTAL'
                    if non_total_mask.any():
                        total_value = pivot_table.loc[non_total_mask, col].sum()
                        # Set the total value for this column
                        pivot_table.loc[('TOTAL', 'TOTAL'), col] = total_value
            
            # Display options for better readability
            max_cols = 8
            display_table = pivot_table.copy()
            
            if len(pivot_table.columns) - 1 > max_cols:  # -1 for TOTAL column
                # Keep first few columns, add ..., then TOTAL
                cols_to_keep = list(pivot_table.columns[:max_cols]) + ['TOTAL']
                display_table = pivot_table[cols_to_keep].copy()
                # Insert ... column before TOTAL - fix the position calculation
                insert_position = len(display_table.columns) - 1
                display_table.insert(insert_position, '...', '...')
            
            # Format the multi-level index for better display
            enhanced_rows = []
            
            # Process each geographic region
            geo_regions = [idx for idx in display_table.index.get_level_values(0).unique() if idx != 'TOTAL']
            
            for geo_region in geo_regions:
                try:
                    region_rows = display_table.loc[geo_region]
                    
                    # Handle single row case (when only one time period)
                    if len(region_rows.shape) == 1:
                        time_periods = [region_rows.name]
                        region_rows = pd.DataFrame([region_rows])
                        region_rows.index = time_periods
                    else:
                        time_periods = region_rows.index
                    
                    # Add individual time period rows
                    region_total = None
                    for time_period in time_periods:
                        if time_period != 'TOTAL':
                            row_data = region_rows.loc[time_period] if len(region_rows.shape) > 1 else region_rows.iloc[0]
                            row_dict = row_data.to_dict()
                            
                            # Calculate TOTAL column manually if it's NaN
                            if 'TOTAL' in row_dict and (pd.isna(row_dict['TOTAL']) or row_dict['TOTAL'] is None):
                                total_sum = 0
                                for col, value in row_dict.items():
                                    if col != 'TOTAL' and isinstance(value, (int, float)) and not pd.isna(value):
                                        total_sum += value
                                row_dict['TOTAL'] = total_sum
                            
                            enhanced_rows.append({
                                'geo_region': geo_region,
                                'time_period': f"{time} {time_period}, {year}",
                                **row_dict
                            })
                        else:
                            # Store the regional total for later use
                            region_total = region_rows.loc[time_period] if len(region_rows.shape) > 1 else region_rows.iloc[0]
                    
                    # Add regional subtotal - calculate manually to ensure TOTAL column is correct
                    region_data_only = [row for row in enhanced_rows if row['geo_region'] == geo_region and 'total' not in row['time_period']]
                    if region_data_only:
                        # Sum up all the values for this region
                        subtotal_dict = {'geo_region': geo_region, 'time_period': 'total'}
                        total_sum = 0
                        for col in display_table.columns:
                            if col != 'TOTAL':
                                col_sum = sum(row.get(col, 0) for row in region_data_only if isinstance(row.get(col), (int, float)))
                                subtotal_dict[col] = col_sum
                                total_sum += col_sum
                        # Add the TOTAL column with the correct sum
                        subtotal_dict['TOTAL'] = total_sum
                        enhanced_rows.append(subtotal_dict)
                    
                except Exception as e:
                    print(f"Warning: Could not process region {geo_region}: {e}")
                    continue
            
            # Add grand total row
            if 'TOTAL' in display_table.index:
                try:
                    grand_total_data = display_table.loc['TOTAL']
                    if hasattr(grand_total_data, 'loc'):
                        grand_total_data = grand_total_data.loc['TOTAL'] if 'TOTAL' in grand_total_data.index else grand_total_data.iloc[0]
                    
                    grand_total_dict = grand_total_data.to_dict()
                    
                    # Calculate TOTAL column manually if it's NaN
                    if 'TOTAL' in grand_total_dict and (pd.isna(grand_total_dict['TOTAL']) or grand_total_dict['TOTAL'] is None):
                        total_sum = 0
                        for col, value in grand_total_dict.items():
                            if col != 'TOTAL' and isinstance(value, (int, float)) and not pd.isna(value):
                                total_sum += value
                        grand_total_dict['TOTAL'] = total_sum
                    
                    enhanced_rows.append({
                        'geo_region': 'total',
                        'time_period': '',
                        **grand_total_dict
                    })
                except Exception as e:
                    print(f"Warning: Could not add grand total: {e}")
            
            # Convert to DataFrame
            if enhanced_rows:
                result_df = pd.DataFrame(enhanced_rows)
                result_df = result_df.set_index(['geo_region', 'time_period'])
                
                # Format display
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', None)
                pd.set_option('display.max_colwidth', 12)
                
                print(f"\n📋 Cross Table:")
                print(result_df.to_string())
                
                # Show basic summary
                print(f"\n📊 Table Summary:")
                print(f"  Geographic regions: {len(geo_regions)}")
                print(f"  Time periods: {len(detail_data['time_dimension'].unique())}")
                print(f"  Product categories: {len(pivot_table.columns) - 1}")  # -1 for TOTAL column
                print(f"  Total table size: {len(result_df)} rows × {len(result_df.columns)} columns")
                
                # Show additional insights from GROUPING SETS aggregations
                print(f"\n🔍 Additional Insights from GROUPING SETS:")
                geographic_totals = data[data['aggregation_type'] == 'Geographic Total']
                if not geographic_totals.empty:
                    print(f"  Geographic Totals:")
                    for _, row in geographic_totals.head(3).iterrows():
                        print(f"    {row['geo_dimension']}: {row[metric]:,.0f}")
                
                grand_total = data[data['aggregation_type'] == 'Grand Total']
                if not grand_total.empty:
                    total_value = grand_total.iloc[0][metric]
                    print(f"  Grand Total {metric}: {total_value:,.0f}")
                
            else:
                print("No enhanced data to display")
                return display_table
                
        except Exception as e:
            print(f"Error creating cross table: {e}")
            import traceback
            traceback.print_exc()
            # Fallback to simple display 
            print("\nFallback - Raw Data Sample:")
            fallback_data = data[data['aggregation_type'] == 'Detail'] if 'aggregation_type' in data.columns else data
            if not fallback_data.empty:
                print(fallback_data[['geo_dimension', 'time_dimension', 'product_dimension', metric]].head(10).to_string(index=False))
            else:
                print("No detail data available")
        
        return data
    

    def get_summary_statistics(self):
        """Get summary statistics"""
        if not self.conn:
            self.connect()
            
        queries = {
            'total_revenue': "SELECT SUM(Revenue) FROM FactSales",
            'total_transactions': "SELECT COUNT(*) FROM FactSales",
            'date_range': """
                SELECT MIN(d.FullDate) as StartDate, MAX(d.FullDate) as EndDate 
                FROM FactSales f JOIN DimDate d ON f.DateKey = d.DateKey
            """,
            'unique_products': "SELECT COUNT(DISTINCT ProductKey) FROM FactSales",
            'unique_shops': "SELECT COUNT(DISTINCT ShopKey) FROM FactSales"
        }
        
        results = {}
        for key, query in queries.items():
            try:
                result = self.execute_query(query)
                if not result.empty:
                    if key == 'date_range':
                        start_date = result.iloc[0, 0]
                        end_date = result.iloc[0, 1]
                        results[key] = f"{start_date} to {end_date}"
                    else:
                        results[key] = result.iloc[0, 0]
                else:
                    results[key] = None
            except Exception as e:
                print(f"Error querying {key}: {e}")
                results[key] = None
                
        return results
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    def get_cross_table_data(self, geo='region', time='quarter', product='productGroup', year=None):
        """
        Get cross table data using GROUPING SETS for efficient multi-level aggregation
        
        Parameters:
        - geo: Geographic granularity ('country', 'region', 'city', 'shop')
        - time: Time granularity ('year', 'quarter', 'month', 'day')
        - product: Product granularity ('productCategory', 'productFamily', 'productGroup', 'article')
        - year: Optional year filter
        
        Returns: DataFrame with multi-level aggregation data for cross table
        """
        
        # Validate parameters
        if geo not in self.geo_hierarchy:
            raise ValueError(f"Invalid geo parameter. Must be one of: {list(self.geo_hierarchy.keys())}")
        if time not in self.time_hierarchy:
            raise ValueError(f"Invalid time parameter. Must be one of: {list(self.time_hierarchy.keys())}")
        if product not in self.product_hierarchy:
            raise ValueError(f"Invalid product parameter. Must be one of: {list(self.product_hierarchy.keys())}")
        
        # Build SQL query with GROUPING SETS for cross table aggregation
        geo_field = self.geo_hierarchy[geo]
        time_field = self.time_hierarchy[time]
        product_field = self.product_hierarchy[product]
        
        query = f"""
        SELECT 
            COALESCE(CAST({geo_field} AS TEXT), '[Total]') as geo_dimension,
            COALESCE(CAST({time_field} AS TEXT), '[Total]') as time_dimension,
            COALESCE(CAST({product_field} AS TEXT), '[Total]') as product_dimension,
            GROUPING({geo_field}) as geo_grouping,
            GROUPING({time_field}) as time_grouping,
            GROUPING({product_field}) as product_grouping,
            SUM(f.QuantitySold) as total_quantity,
            SUM(f.Revenue) as total_revenue,
            COUNT(*) as transaction_count,
            AVG(f.Revenue / NULLIF(f.QuantitySold, 0)) as avg_unit_price,
            CASE 
                WHEN GROUPING({geo_field}) = 0 AND GROUPING({time_field}) = 0 AND GROUPING({product_field}) = 0 THEN 'Detail'
                WHEN GROUPING({geo_field}) = 0 AND GROUPING({time_field}) = 0 AND GROUPING({product_field}) = 1 THEN 'Row Subtotal'
                WHEN GROUPING({geo_field}) = 0 AND GROUPING({time_field}) = 1 AND GROUPING({product_field}) = 0 THEN 'Column Subtotal'
                WHEN GROUPING({geo_field}) = 0 AND GROUPING({time_field}) = 1 AND GROUPING({product_field}) = 1 THEN 'Geographic Total'
                WHEN GROUPING({geo_field}) = 1 AND GROUPING({time_field}) = 1 AND GROUPING({product_field}) = 1 THEN 'Grand Total'
                ELSE 'Other'
            END as aggregation_type
        FROM FactSales f
        JOIN DimShop s ON f.ShopKey = s.ShopKey
        JOIN DimDate d ON f.DateKey = d.DateKey
        JOIN DimProduct p ON f.ProductKey = p.ProductKey
        """
        
        params = []
        if year:
            query += " WHERE d.Year = %s"
            params.append(year)
            
        query += f"""
        GROUP BY GROUPING SETS (
            ({geo_field}, {time_field}, {product_field}),  -- Detail level
            ({geo_field}, {time_field}),                   -- Row subtotals (geo + time)
            ({geo_field}, {product_field}),                -- Column subtotals (geo + product)
            ({geo_field}),                                 -- Geographic totals
            ()                                             -- Grand total
        )
        ORDER BY GROUPING({geo_field}), GROUPING({time_field}), GROUPING({product_field}),
                 COALESCE(CAST({geo_field} AS TEXT), 'ZZZZ'), 
                 COALESCE(CAST({time_field} AS TEXT), 'ZZZZ'), 
                 COALESCE(CAST({product_field} AS TEXT), 'ZZZZ')
        """
        
        result_df = self.execute_query(query, params if params else None)
        
        if not result_df.empty:
            print(f"📊 Cross Table Data Generated using GROUPING SETS")
            print(f"Total Records: {len(result_df)}")
            print("Aggregation Types:")
            for agg_type in result_df['aggregation_type'].unique():
                count = len(result_df[result_df['aggregation_type'] == agg_type])
                print(f"  - {agg_type}: {count} records")
            print("-" * 40)
        
        return result_df

    def interactive_drill_navigation(self, year=2019):
        """
        Interactive drill down/roll up navigation through dimension hierarchies
        
        Parameters:
        - year: Year filter for analysis
        
        Demonstrates true OLAP navigation capabilities
        """
        print(f"\n🎯 Interactive Drill Down/Roll Up Navigation")
        print("=" * 60)
        print("Navigate through dimension hierarchies using drill down/roll up operations")
        
        # Define hierarchy levels for each dimension
        geo_hierarchy_levels = ['country', 'region', 'city', 'shop']
        time_hierarchy_levels = ['year', 'quarter', 'month', 'day']  
        product_hierarchy_levels = ['productCategory', 'productFamily', 'productGroup', 'article']
        
        # Current navigation state
        current_geo = 1  # Start at country level (index 1 = region)
        current_time = 1  # Start at quarter level  
        current_product = 2  # Start at productGroup level
        
        while True:
            # Display current position in hierarchies
            print(f"\n📍 Current Navigation Position:")
            print(f"   Geographic: {geo_hierarchy_levels[current_geo]} (Level {current_geo + 1}/4)")
            print(f"   Time: {time_hierarchy_levels[current_time]} (Level {current_time + 1}/4)")
            print(f"   Product: {product_hierarchy_levels[current_product]} (Level {current_product + 1}/4)")
            
            # Get current analysis
            geo = geo_hierarchy_levels[current_geo]
            time = time_hierarchy_levels[current_time] 
            product = product_hierarchy_levels[current_product]
            
            print(f"\n📊 Current Analysis: {geo} × {time} × {product} ({year})")
            
            # Execute analysis with current hierarchy levels
            result = self.analysis(geo=geo, time=time, product=product, year=year)
            
            if not result.empty:
                # Show summary of current level
                detail_data = result[result['aggregation_level'] == 'Detail Level']
                if not detail_data.empty:
                    total_revenue = detail_data['total_revenue'].sum()
                    record_count = len(detail_data)
                    print(f"📈 Summary: {record_count} records, Total Revenue: €{total_revenue:,.2f}")
                    
                    # Show top 10 records as sample
                    print("\n📋 Sample Data (Top 10 records):")
                    sample_data = detail_data.head(10)[['geo_dimension', 'time_dimension', 'product_dimension', 'total_revenue']]
                    print(sample_data.to_string(index=False))
            
            # Navigation menu
            print(f"\n🧭 Navigation Options:")
            print("=" * 40)
            
            # Drill Down options
            print("🔽 DRILL DOWN (Go to more detailed level):")
            if current_geo < len(geo_hierarchy_levels) - 1:
                next_geo = geo_hierarchy_levels[current_geo + 1]
                print(f"   1. Geographic: {geo_hierarchy_levels[current_geo]} → {next_geo}")
            else:
                print(f"   1. Geographic: Already at most detailed level ({geo_hierarchy_levels[current_geo]})")
                
            if current_time < len(time_hierarchy_levels) - 1:
                next_time = time_hierarchy_levels[current_time + 1]
                print(f"   2. Time: {time_hierarchy_levels[current_time]} → {next_time}")
            else:
                print(f"   2. Time: Already at most detailed level ({time_hierarchy_levels[current_time]})")
                
            if current_product < len(product_hierarchy_levels) - 1:
                next_product = product_hierarchy_levels[current_product + 1]
                print(f"   3. Product: {product_hierarchy_levels[current_product]} → {next_product}")
            else:
                print(f"   3. Product: Already at most detailed level ({product_hierarchy_levels[current_product]})")
            
            # Roll Up options  
            print("\n🔼 ROLL UP (Go to more aggregated level):")
            if current_geo > 0:
                prev_geo = geo_hierarchy_levels[current_geo - 1]
                print(f"   4. Geographic: {geo_hierarchy_levels[current_geo]} → {prev_geo}")
            else:
                print(f"   4. Geographic: Already at most aggregated level ({geo_hierarchy_levels[current_geo]})")
                
            if current_time > 0:
                prev_time = time_hierarchy_levels[current_time - 1]
                print(f"   5. Time: {time_hierarchy_levels[current_time]} → {prev_time}")
            else:
                print(f"   5. Time: Already at most aggregated level ({time_hierarchy_levels[current_time]})")
                
            if current_product > 0:
                prev_product = product_hierarchy_levels[current_product - 1]
                print(f"   6. Product: {product_hierarchy_levels[current_product]} → {prev_product}")
            else:
                print(f"   6. Product: Already at most aggregated level ({product_hierarchy_levels[current_product]})")
            
            print(f"\n   7. Show Detailed Data")
            print(f"   8. Return to Main Menu")
            
            # Get user choice
            nav_choice = input("\nSelect navigation option (1-8): ").strip()
            
            if nav_choice == '1' and current_geo < len(geo_hierarchy_levels) - 1:
                current_geo += 1
                print(f"🔽 Drilling down Geographic dimension to {geo_hierarchy_levels[current_geo]}")
            elif nav_choice == '2' and current_time < len(time_hierarchy_levels) - 1:
                current_time += 1
                print(f"🔽 Drilling down Time dimension to {time_hierarchy_levels[current_time]}")
            elif nav_choice == '3' and current_product < len(product_hierarchy_levels) - 1:
                current_product += 1
                print(f"🔽 Drilling down Product dimension to {product_hierarchy_levels[current_product]}")
            elif nav_choice == '4' and current_geo > 0:
                current_geo -= 1
                print(f"🔼 Rolling up Geographic dimension to {geo_hierarchy_levels[current_geo]}")
            elif nav_choice == '5' and current_time > 0:
                current_time -= 1
                print(f"🔼 Rolling up Time dimension to {time_hierarchy_levels[current_time]}")
            elif nav_choice == '6' and current_product > 0:
                current_product -= 1
                print(f"🔼 Rolling up Product dimension to {product_hierarchy_levels[current_product]}")
            elif nav_choice == '7':
                # Show detailed data
                if not result.empty:
                    detail_data = result[result['aggregation_level'] == 'Detail Level']
                    if not detail_data.empty:
                        print_dataframe(detail_data, f"Detailed Analysis: {geo} × {time} × {product} ({year})")
                    else:
                        print("No detail level data available")
            elif nav_choice == '8':
                break
            else:
                print("❌ Invalid choice or operation not available at current level")
                
def print_dataframe(df, title=""):
    """Format and print DataFrame"""
    print(f"\n=== {title} ===")
    if df.empty:
        print("No data available")
    else:
        # Create a display copy of the dataframe
        display_df = df.copy()
        
        # Hide technical columns from display but keep them for functionality
        columns_to_hide = ['geo_grouping', 'time_grouping', 'product_grouping']
        columns_to_show = [col for col in display_df.columns if col not in columns_to_hide]
        display_df = display_df[columns_to_show]
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 30)
        print(display_df.to_string(index=False))
        print(f"Total {len(display_df)} records")

 