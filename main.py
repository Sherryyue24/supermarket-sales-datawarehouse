#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from etl_process import DataWarehouseETL
from olap_queries import OLAPAnalyzer, print_dataframe

def main():
    """Main program entry point"""
    print("🏪 Supermarket Sales Data Warehouse Analysis System (Docker)")
    print("=" * 70)
    
    while True:
        print("\nPlease select an option:")
        print("1. Execute ETL Process (Build Data Warehouse)")
        print("2. Run OLAP Analysis")
        print("3. Database Status Check")
        print("4. Exit")
        
        choice = input("\nEnter option (1-4): ").strip()
        
        if choice == '1':
            run_etl()
        elif choice == '2':
            run_olap_analysis()
        elif choice == '3':
            check_database_status()
        elif choice == '4':
            print("Thank you for using the system!")
            break
        else:
            print("Invalid option, please try again.")

def run_etl():
    """Execute ETL process"""
    print("\n🔄 Starting ETL process...")
    
    etl = DataWarehouseETL()
    try:
        etl.run_etl()
        print("✅ ETL process completed! Data warehouse has been built successfully.")
    except Exception as e:
        print(f"❌ ETL process failed: {e}")
        print("Please ensure PostgreSQL database is running.")
    finally:
        etl.close()

def check_database_status():
    """Check database status"""
    print("\n🔍 Checking database connection status...")
    
    try:
        analyzer = OLAPAnalyzer()
        analyzer.connect()
        
        # Check if tables exist
        stats = analyzer.get_summary_statistics()
        
        print("✅ Database connection is normal")
        if stats['total_transactions'] and stats['total_transactions'] > 0:
            print("📊 Data warehouse status:")
            print(f"   Total transactions: {stats['total_transactions']:,}")
            print(f"   Total revenue: €{stats['total_revenue']:,.2f}")
            print(f"   Product varieties: {stats['unique_products']}")
            print(f"   Number of shops: {stats['unique_shops']}")
            print(f"   Date range: {stats['date_range']}")
        else:
            print("⚠️  Data warehouse is empty, please run ETL process first.")
            
        analyzer.close()
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Please ensure:")
        print("1. Docker is running")
        print("2. You have run './start.sh' startup script")
        print("3. PostgreSQL container is started")

def run_olap_analysis():
    """Run OLAP analysis"""
    print("\n📊 Starting OLAP analysis...")
    
    analyzer = OLAPAnalyzer()
    
    try:
        # Check if data exists first
        stats = analyzer.get_summary_statistics()
        if not stats['total_transactions'] or stats['total_transactions'] == 0:
            print("❌ Data warehouse is empty, please run ETL process first.")
            return
        
        while True:
            print("\nPlease select analysis type:")
            print("1. Detail Table Analysis")
            print("2. Cross Table Analysis")
            print("3. Interactive Drill Down/Roll Up Navigation")
            print("4. Return to Main Menu")
            
            sub_choice = input("\nEnter option (1-4): ").strip()
            
            if sub_choice == '1':
                # Detail table analysis
                print("\n📊 Detail Table Analysis")
                print("Configure analysis parameters...")
                
                # Get parameters from user
                print("\nSelect Geographic Granularity:")
                print("1. Country")
                print("2. Region") 
                print("3. City")
                print("4. Shop")
                geo_choice = input("Enter choice (1-4, default 2): ").strip()
                geo_map = {'1': 'country', '2': 'region', '3': 'city', '4': 'shop'}
                geo = geo_map.get(geo_choice, 'region')
                
                print("\nSelect Time Granularity:")
                print("1. Year")
                print("2. Quarter")
                print("3. Month") 
                print("4. Day")
                time_choice = input("Enter choice (1-4, default 2): ").strip()
                time_map = {'1': 'year', '2': 'quarter', '3': 'month', '4': 'day'}
                time = time_map.get(time_choice, 'quarter')
                
                print("\nSelect Product Granularity:")
                print("1. Product Category")
                print("2. Product Family")
                print("3. Product Group")
                print("4. Article")
                product_choice = input("Enter choice (1-4, default 3): ").strip()
                product_map = {'1': 'productCategory', '2': 'productFamily', '3': 'productGroup', '4': 'article'}
                product = product_map.get(product_choice, 'productGroup')
                
                year = input("\nEnter year (default 2019): ").strip()
                year = int(year) if year else 2019
                
                # Execute analysis and show detailed table
                result = analyzer.analysis(geo=geo, time=time, product=product, year=year)
                
                if not result.empty and 'aggregation_level' in result.columns:
                    print(f"\n🔍 Available Aggregation Levels:")
                    levels = result['aggregation_level'].unique()
                    for i, level in enumerate(levels, 1):
                        count = len(result[result['aggregation_level'] == level])
                        print(f"{i}. {level} ({count} records)")
                    print(f"{len(levels)+1}. Show All Levels")
                    
                    level_choice = input(f"\nSelect level to display (1-{len(levels)+1}, default {len(levels)+1}): ").strip()
                    
                    if level_choice and level_choice.isdigit():
                        choice_idx = int(level_choice) - 1
                        if 0 <= choice_idx < len(levels):
                            # Show specific level
                            selected_level = levels[choice_idx]
                            filtered_result = result[result['aggregation_level'] == selected_level]
                            print_dataframe(filtered_result, f"Analysis: {geo} × {time} × {product} ({year}) - {selected_level}")
                        else:
                            # Show all levels
                            print_dataframe(result, f"Analysis: {geo} × {time} × {product} ({year}) - All Levels")
                    else:
                        # Default: show all
                        print_dataframe(result, f"Analysis: {geo} × {time} × {product} ({year}) - All Levels")
                else:
                    print_dataframe(result, f"Analysis: {geo} × {time} × {product} ({year})")
                
            elif sub_choice == '2':
                # Cross table analysis
                print("\n📋 Cross Table Analysis")
                print("Configure dimensions for cross table...")
                
                # Geographic dimension
                print("\nSelect Geographic Granularity:")
                print("1. Country  2. Region  3. City  4. Shop")
                geo_choice = input("Enter choice (1-4, default 2): ").strip()
                geo_map = {'1': 'country', '2': 'region', '3': 'city', '4': 'shop'}
                geo = geo_map.get(geo_choice, 'region')
                
                # Time dimension  
                print("\nSelect Time Granularity:")
                print("1. Year  2. Quarter  3. Month  4. Day")
                time_choice = input("Enter choice (1-4, default 2): ").strip()
                time_map = {'1': 'year', '2': 'quarter', '3': 'month', '4': 'day'}
                time = time_map.get(time_choice, 'quarter')
                
                # Product dimension
                print("\nSelect Product Granularity:")
                print("1. ProductCategory  2. ProductFamily  3. ProductGroup  4. Article")
                product_choice = input("Enter choice (1-4, default 3): ").strip()
                product_map = {'1': 'productCategory', '2': 'productFamily', '3': 'productGroup', '4': 'article'}
                product = product_map.get(product_choice, 'productGroup')
                
                # Year and metric
                year = input("\nEnter year (default 2019): ").strip()
                year = int(year) if year else 2019
                
                print("\nSelect metric:")
                print("1. Quantity  2. Revenue  3. Transaction Count")
                metric_choice = input("Enter choice (1-3, default 1): ").strip()
                metric_map = {'1': 'total_quantity', '2': 'total_revenue', '3': 'transaction_count'}
                metric = metric_map.get(metric_choice, 'total_quantity')
                
                # Generate cross table
                analyzer.generate_cross_table(geo=geo, time=time, product=product, year=year, metric=metric)
                
            elif sub_choice == '3':
                # Interactive Drill Down/Roll Up Navigation
                year = input("\nEnter year for navigation (default 2019): ").strip()
                year = int(year) if year else 2019
                analyzer.interactive_drill_navigation(year=year)
                
            elif sub_choice == '4':
                break
            else:
                print("Invalid option, please try again.")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        print("Please ensure the data warehouse is properly built.")
    finally:
        analyzer.close()



if __name__ == "__main__":
    main() 