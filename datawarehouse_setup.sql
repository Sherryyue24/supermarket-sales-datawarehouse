
-- Clean up existing tables
DROP TABLE IF EXISTS FactSales CASCADE;
DROP TABLE IF EXISTS DimDate CASCADE;
DROP TABLE IF EXISTS DimShop CASCADE;
DROP TABLE IF EXISTS DimProduct CASCADE;

-- Date Dimension Table
CREATE TABLE DimDate (
    DateKey INTEGER PRIMARY KEY,
    FullDate DATE NOT NULL,
    Year INTEGER NOT NULL,
    Quarter INTEGER NOT NULL,
    Month INTEGER NOT NULL,
    Day INTEGER NOT NULL,
    DayOfWeek INTEGER NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    QuarterName VARCHAR(10) NOT NULL
);

-- Shop Dimension Table (includes geographic hierarchy)
CREATE TABLE DimShop (
    ShopKey SERIAL PRIMARY KEY,
    ShopID INTEGER NOT NULL,
    ShopName VARCHAR(255) NOT NULL,
    CityID INTEGER NOT NULL,
    CityName VARCHAR(255) NOT NULL,
    RegionID INTEGER NOT NULL,
    RegionName VARCHAR(255) NOT NULL,
    CountryID INTEGER NOT NULL,
    CountryName VARCHAR(255) NOT NULL
);

-- Product Dimension Table (includes product hierarchy)
CREATE TABLE DimProduct (
    ProductKey SERIAL PRIMARY KEY,
    ArticleID INTEGER NOT NULL,
    ArticleName VARCHAR(255) NOT NULL,
    Price DECIMAL(10,2) NOT NULL,
    ProductGroupID INTEGER NOT NULL,
    ProductGroupName VARCHAR(255) NOT NULL,
    ProductFamilyID INTEGER NOT NULL,
    ProductFamilyName VARCHAR(255) NOT NULL,
    ProductCategoryID INTEGER NOT NULL,
    ProductCategoryName VARCHAR(255) NOT NULL
);

-- Sales Fact Table
CREATE TABLE FactSales (
    SalesKey SERIAL PRIMARY KEY,
    DateKey INTEGER NOT NULL,
    ShopKey INTEGER NOT NULL,
    ProductKey INTEGER NOT NULL,
    QuantitySold INTEGER NOT NULL,
    Revenue DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ShopKey) REFERENCES DimShop(ShopKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey)
);

-- Create indexes to improve query performance
CREATE INDEX idx_factsales_date ON FactSales(DateKey);
CREATE INDEX idx_factsales_shop ON FactSales(ShopKey);
CREATE INDEX idx_factsales_product ON FactSales(ProductKey);
CREATE INDEX idx_factsales_composite ON FactSales(DateKey, ShopKey, ProductKey);

-- Create analytical view
CREATE VIEW v_sales_summary AS
SELECT 
    d.Year,
    d.Quarter,
    d.Month,
    s.RegionName,
    s.CityName,
    p.ProductCategoryName,
    p.ProductFamilyName,
    SUM(f.QuantitySold) as TotalQuantity,
    SUM(f.Revenue) as TotalRevenue,
    COUNT(*) as TransactionCount
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
JOIN DimShop s ON f.ShopKey = s.ShopKey
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY d.Year, d.Quarter, d.Month, s.RegionName, s.CityName, 
         p.ProductCategoryName, p.ProductFamilyName; 