# 🏪 Supermarket Sales Data Warehouse Analysis System

A comprehensive OLAP (Online Analytical Processing) data warehouse solution for analyzing German supermarket sales data. This system implements advanced SQL extensions (**CUBE** and **GROUPING SETS**) for efficient multidimensional analysis and supports interactive **drill down/roll up navigation** through dimensional hierarchies.

## 🎯 Key Features

### ⭐ Advanced SQL Extensions for OLAP
- **CUBE**: Complete data cube generation with all possible aggregation combinations
- **GROUPING SETS**: Precise control over specific aggregation levels
- **Interactive Navigation**: Real-time drill down/roll up through dimension hierarchies

### 📊 Three Analysis Modes

| Mode | SQL Extension | Purpose | Aggregation Levels |
|------|---------------|---------|-------------------|
| **Detail Table Analysis** | `CUBE` | Complete multidimensional analysis | 8 levels (2³ combinations) |
| **Cross Table Analysis** | `GROUPING SETS` | Efficient cross-tabulation | 5 optimized levels |
| **Interactive Navigation** | `CUBE` | Drill down/roll up exploration | Dynamic hierarchy traversal |

### 🗂️ Dimensional Hierarchies

**Geographic Hierarchy**: Country → Region → City → Shop  
**Time Hierarchy**: Year → Quarter → Month → Day  
**Product Hierarchy**: Category → Family → Group → Article

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- Python 3.7+

### 1. Launch System
```bash
# Clone and start
git clone <repository-url>
cd exercise6
chmod +x start.sh
./start.sh
```

### 2. Run Analysis
```bash
python3 main.py
```

### 3. System Access
- **Application**: Interactive console interface
- **Database**: `localhost:5432` (dwuser/dwpassword)
- **pgAdmin**: `http://localhost:8080` (admin@datawarehouse.com/admin123)

## 🔍 Analysis Capabilities

### 1. Detail Table Analysis (CUBE)
Uses SQL `CUBE` to generate complete data cube with **8 aggregation levels**:

```sql
GROUP BY CUBE(geo_field, time_field, product_field)
```

**Generated Levels:**
- Detail Level (all dimensions)
- By Geo + Time
- By Geo + Product  
- By Time + Product
- By Geography Only
- By Time Only
- By Product Only
- Grand Total

**Features:**
- ✅ Complete multidimensional view
- ✅ All possible aggregation combinations
- ✅ Interactive level selection
- ✅ Automatic GROUPING() identification

### 2. Cross Table Analysis (GROUPING SETS)
Uses SQL `GROUPING SETS` for **efficient cross-tabulation**:

```sql
GROUP BY GROUPING SETS (
    (geo, time, product),  -- Detail
    (geo, time),          -- Row subtotals
    (geo, product),       -- Column subtotals  
    (geo),                -- Geographic totals
    ()                    -- Grand total
)
```

**Generated Levels:**
- Detail records
- Row Subtotals (geo + time)
- Column Subtotals (geo + product)
- Geographic Totals
- Grand Total

**Features:**
- ✅ Optimized for cross-tables
- ✅ Automatic subtotal calculation
- ✅ Memory efficient
- ✅ Enhanced insights display

### 3. Interactive Drill Down/Roll Up Navigation
Real-time navigation through dimensional hierarchies:

```
📍 Current Position: Region × Quarter × ProductGroup
🔽 DRILL DOWN: region → city, quarter → month, productGroup → article
🔼 ROLL UP: region → country, quarter → year, productGroup → productFamily
```

**Navigation Features:**
- ✅ Real-time hierarchy traversal
- ✅ Dynamic position tracking (Level X/4)
- ✅ Boundary detection (most detailed/aggregated)
- ✅ Automatic data recalculation
- ✅ Visual navigation indicators

## 🏗️ Technical Architecture

### Data Warehouse Design (Star Schema)
```
FactSales (77K+ records)
├── DimDate (date hierarchy)
├── DimShop (geographic hierarchy)
└── DimProduct (product hierarchy)
```

### SQL Extensions Implementation

**CUBE Example:**
```sql
SELECT 
    COALESCE(CAST(s.RegionName AS TEXT), '[Total]') as geo_dimension,
    COALESCE(CAST(d.Quarter AS TEXT), '[Total]') as time_dimension,
    COALESCE(CAST(p.ProductGroupName AS TEXT), '[Total]') as product_dimension,
    GROUPING(s.RegionName) as geo_grouping,
    SUM(f.QuantitySold) as total_quantity,
    CASE 
        WHEN GROUPING(s.RegionName) = 0 AND GROUPING(d.Quarter) = 0 THEN 'Detail Level'
        WHEN GROUPING(s.RegionName) = 1 AND GROUPING(d.Quarter) = 1 THEN 'Grand Total'
        -- ... other combinations
    END as aggregation_level
FROM FactSales f
JOIN DimShop s ON f.ShopKey = s.ShopKey
JOIN DimDate d ON f.DateKey = d.DateKey  
JOIN DimProduct p ON f.ProductKey = p.ProductKey
WHERE d.Year = 2019
GROUP BY CUBE(s.RegionName, d.Quarter, p.ProductGroupName)
ORDER BY GROUPING(s.RegionName), GROUPING(d.Quarter), GROUPING(p.ProductGroupName)
```

### Core Classes

**`OLAPAnalyzer`** - Main analysis engine
- `analysis()` - CUBE-based multidimensional analysis
- `generate_cross_table()` - GROUPING SETS cross-tabulation
- `get_cross_table_data()` - Optimized cross-table data generation
- `interactive_drill_navigation()` - Real-time hierarchy navigation


## 🎮 User Interface

### Main Menu
```
🏪 Supermarket Sales Data Warehouse Analysis System
1. Execute ETL Process (Build Data Warehouse)
2. Run OLAP Analysis
3. Database Status Check
4. Exit
```

### OLAP Analysis Submenu
```
📊 Starting OLAP analysis...
1. Detail Table Analysis          (CUBE - 8 levels)
2. Cross Table Analysis           (GROUPING SETS - 5 levels)  
3. Interactive Drill Navigation   (Real-time hierarchy traversal)
4. Return to Main Menu
```

### Navigation Interface
```
📍 Current Navigation Position:
   Geographic: region (Level 2/4)
   Time: quarter (Level 2/4)
   Product: productGroup (Level 3/4)

🧭 Navigation Options:
🔽 DRILL DOWN (Go to more detailed level):
   1. Geographic: region → city
   2. Time: quarter → month
   3. Product: productGroup → article

🔼 ROLL UP (Go to more aggregated level):
   4. Geographic: region → country
   5. Time: quarter → year
   6. Product: productGroup → productFamily
```

## 📈 Performance Benefits

### CUBE vs Traditional Queries
- **Single Query**: One CUBE query vs 8 separate GROUP BY queries
- **Consistency**: All aggregation levels from same execution  
- **Optimization**: Database engine optimizes cube generation
- **Memory Efficiency**: Shared intermediate results

### GROUPING SETS vs Application Logic
- **Server-side Aggregation**: Reduces data transfer
- **SQL Engine Optimization**: Leverages query planner
- **Precise Control**: Only needed aggregation levels
- **Automatic Subtotals**: No manual calculation required

## 🛠️ Project Structure

```
exercise6/
├── main.py                      # Main application entry point
├── olap_queries.py             # OLAP analysis engine (CUBE + GROUPING SETS)
├── etl_process.py              # ETL pipeline for data warehouse setup
├── datawarehouse_setup.sql     # Data warehouse schema (star schema)
├── docker-compose.yml          # PostgreSQL + pgAdmin containers
├── start.sh                    # System startup script
├── requirements.txt            # Python dependencies
├── ressources/
│   ├── sales.csv              # Sales transaction data (~77K records)
│   └── stores-and-products.sql # Base schema and reference data
└── README.md                   # This documentation
```

## 🔧 Technical Requirements

### System Requirements
- **Docker Desktop** (for containerized PostgreSQL)
- **Python 3.7+** with packages:
  - `psycopg2` (PostgreSQL adapter)
  - `pandas` (data manipulation)

### Database Specifications
- **Engine**: PostgreSQL 13+
- **Extensions**: Built-in CUBE and GROUPING SETS support
- **Schema**: Star schema design
- **Data Volume**: ~77,000 sales transactions
- **Index Strategy**: Optimized for OLAP queries

