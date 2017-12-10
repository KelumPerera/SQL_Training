use AdventureWorksDW2014;

---------- select all columns
SELECT * FROM FactInternetSales;

-- limiting results
SELECT TOP 1000 * FROM FactInternetSales; -- in other DB's use LIMIT 1000;

SELECT * FROM FactInternetSales 
OFFSET 1000 ROWS 
FETCH NEXT 1000 ROWS ONLY; -- select next 1000 after 1000 rows;

-- select using column names
SELECT [ProductKey]
      ,[OrderDateKey]
      ,[DueDateKey]
      ,[ShipDateKey]
      ,[CustomerKey]
      ,[PromotionKey]
      ,[CurrencyKey]
      ,[SalesTerritoryKey]
      ,[SalesOrderNumber]
      ,[SalesOrderLineNumber]
      ,[RevisionNumber]
      ,[OrderQuantity]
      ,[UnitPrice]
      ,[ExtendedAmount]
      ,[UnitPriceDiscountPct]
      ,[DiscountAmount]
      ,[ProductStandardCost]
      ,[TotalProductCost]
      ,[SalesAmount]
      ,[TaxAmt]
      ,[Freight]
      ,[CarrierTrackingNumber]
      ,[CustomerPONumber]
      ,[OrderDate]
      ,[DueDate]
      ,[ShipDate]
  FROM [dbo].[FactInternetSales]


-- select some and use aliases
SELECT TOP 1000
		[SalesOrderNumber] AS 'OrderNumber'
      ,	[SalesOrderLineNumber] AS 'LineNumber'
      ,	[OrderQuantity] AS 'Quantity'
      ,	[UnitPrice] AS 'Price'
      ,	[DiscountAmount] AS 'Discount'
      ,	[SalesAmount] AS 'Sales'
      ,	[TaxAmt] AS 'Taxes'      
      ,	[OrderDate] AS 'Date'      
FROM [dbo].[FactInternetSales]

-------- Joining Tables (Why-> Combine related data, To perform comparisons, To perform lookups)

-- inner join
-- returns results only where the join condition is true
SELECT TOP 1000 *
FROM FactInternetSales s   -- s is used to aliases this table name as to save some typing later on
inner join DimProduct p ON s.ProductKey = p.ProductKey

-- left join
-- returns all rows from sales, regardless of the join condition
SELECT DISTINCT EnglishProductName
FROM FactInternetSales s
left join DimProduct p ON s.ProductKey = p.ProductKey
ORDER BY 1

-- add filter conditions to join
SELECT *
FROM FactInternetSales s
inner join DimProduct p 
	ON	s.ProductKey = p.ProductKey 
	and	p.StartDate > '2013-01-01'   -- Added an aditional filter

--------- Basic filter with WHERE  - (ie- Focused analysis, Get a sample or reduce the data set, Limit the analysis to a aggregate condition-eg Analyse the products that are marketed at least 1 year before)

-- get sales of a specific product only
SELECT *
FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
WHERE p.EnglishProductName = 'Road-650 Black, 62'

-- non-equal-filters
-- get all orders for 2013
SELECT *
FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
WHERE	s.OrderDate >= '2013-01-01'
AND		s.OrderDate <= '2013-12-31'

-- also can use "between" for dates (U'll get same result as above quary)
SELECT *
FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
WHERE s.OrderDate BETWEEN '2013-01-01' AND '2013-12-31';

-- filter for multiple values using IN
SELECT *
FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
WHERE p.EnglishProductName in( 
		'Mountain-400-W Silver, 38',
		'Mountain-400-W Silver, 40',
		'Mountain-400-W Silver, 42',
		'Mountain-400-W Silver, 46')


-- find all current and future matches with LIKE
SELECT *
FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
WHERE p.EnglishProductName LIKE 'Mountain%' --put % where you want wildcard

select OrderDate, sum(SalesAmount)
from FactInternetSales
group by OrderDate
order by OrderDate

-- Simple aggregations (eg- MIN, MAX, AVG, COUNT, STDev, Var, RANK )
-- Use additional aggregations to understand more about product sales such as distribution of sales etc..
-- (eg- Summerize the data, Create a context to get deeper understading, Identify paterns & trends)

SELECT 
	cat.EnglishProductCategoryName 'Category'
    	,sub.EnglishProductSubcategoryName 'SubCategory'
	,count(1) 'Count' -- How many sales where there?
	,sum(s.SalesAmount) 'Sales' -- How much sales did we have?
    	,avg(s.SalesAmount) 'Avg_SalesAmount' -- What was the Avg sale amount?
    	,min(s.SalesAmount) 'Min_SaleAmount' -- What was the Min sale amount?
    	,max(s.SalesAmount) 'Max_SaleAmount' -- What was the Max sale amount
FROM FactInternetSales s
LEFT JOIN DimProduct p ON s.ProductKey = p.ProductKey
LEFT JOIN DimProductSubcategory sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
LEFT JOIN DimProductCategory cat ON sub.ProductCategoryKey = cat.ProductCategoryKey
-- must use group by in order for aggregation to work properly
GROUP BY
	cat.EnglishProductCategoryName -- column aliases aren't allowed
    	,sub.EnglishProductSubcategoryName
ORDER BY
	cat.EnglishProductCategoryName
	,sub.EnglishProductSubcategoryName

-- filter to 2013 with WHERE
SELECT 
	YEAR(s.OrderDate) 'Year'
	,cat.EnglishProductCategoryName 'Category'
    	,sub.EnglishProductSubcategoryName 'SubCategory'	
	,count(1) 'Count' -- use 1 instead of a field for faster performance
	,sum(s.SalesAmount) 'Sales'
    	,avg(s.SalesAmount) 'Avg_Quantity'
    	,min(s.SalesAmount) 'Min_SaleAmount'
    	,max(s.SalesAmount) 'Max_SaleAmount'

FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
INNER JOIN DimProductSubcategory sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
INNER JOIN DimProductCategory cat ON sub.ProductCategoryKey = cat.ProductCategoryKey
-- filter
WHERE YEAR(s.OrderDate) = 2013 --use date function to parse year
-- must use group by in order for aggregation to work properly
GROUP BY
	YEAR(s.OrderDate)
	,cat.EnglishProductCategoryName -- column aliases aren't allowed
    	,sub.EnglishProductSubcategoryName
ORDER BY
	cat.EnglishProductCategoryName
	,sub.EnglishProductSubcategoryName

-- Only show products in 2013 that sold more than $1M USD
SELECT 
	cat.EnglishProductCategoryName 'Category'
    	,sub.EnglishProductSubcategoryName 'SubCategory'	
	,count(1) 'Count' -- use 1 instead of a field for faster performance
	,sum(s.SalesAmount) 'Sales'
    	,avg(s.SalesAmount) 'Avg_Quantity'
    	,min(s.SalesAmount) 'Min_SaleAmount'
    	,max(s.SalesAmount) 'Max_SaleAmount'
FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
INNER JOIN DimProductSubcategory sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
INNER JOIN DimProductCategory cat ON sub.ProductCategoryKey = cat.ProductCategoryKey
-- filter
WHERE YEAR(s.OrderDate) = 2013 --use date function to parse year
-- must use group by in order for aggregation to work properly
GROUP BY
	cat.EnglishProductCategoryName -- column aliases aren't allowed
    	,sub.EnglishProductSubcategoryName	
-- use HAVING to filter after the aggregate is computed
HAVING
	sum(s.SalesAmount) > 1000000
ORDER BY
	cat.EnglishProductCategoryName
	,sub.EnglishProductSubcategoryName

-- Window Functions - Arguments which focuses our analysis to a one particular segments of the data

/*

OVER() 
	-- executes an aggregation over a given partition and sort order
	-- works with Ranking, Aggregate and Analytics functions
*/

USE AdventureWorksDW2014;
GO

-- Show each sales average for Group, Country, and Region all in one query
SELECT DISTINCT		
	t.SalesTerritoryGroup
	,t.SalesTerritoryCountry
	,t.SalesTerritoryRegion
	,AVG(s.SalesAmount) OVER(PARTITION BY t.SalesTerritoryGroup ) as 'GroupAvgSales'		
	,AVG(s.SalesAmount) OVER(PARTITION BY t.SalesTerritoryCountry ) as 'CountryAvgSales'
	,AVG(s.SalesAmount) OVER(PARTITION BY t.SalesTerritoryRegion ) as 'RegionAvgSales'	
	
FROM FactInternetSales s
JOIN DimSalesTerritory t ON
	s.SalesTerritoryKey = t.SalesTerritoryKey	
WHERE
	YEAR(s.OrderDate) = 2013
ORDER BY
	1,2,3


-- Sub Queries

-- Use a sub-query to aggregate an underlying Table
select *
from (
	select sum(SalesAmount) as 'Sales', YEAR(OrderDate) as 'Yr'
	from FactInternetSales
	group by YEAR(OrderDate)
) YrSales

-- Create new aggregates on to of derived
select avg(Sales) as 'AvgSales'
from (
	select sum(SalesAmount) as 'Sales', YEAR(OrderDate) as 'Yr'
	from FactInternetSales
	group by YEAR(OrderDate)
) YrSales

-- Use a subquery to test if values are IN another table
SELECT EnglishProductName 'Product'
FROM DimProduct p
WHERE p.ProductSubcategoryKey IN
    (SELECT sc.ProductSubcategoryKey
     FROM DimProductSubcategory sc
     WHERE sc.EnglishProductSubcategoryName = 'Wheels')

-- Re-write this as a Join instead
SELECT	p.EnglishProductName
FROM	DimProduct p
JOIN	DimProductSubcategory sc ON p.ProductSubcategoryKey = sc.ProductSubcategoryKey
WHERE	sc.EnglishProductSubcategoryName = 'Wheels'

-- Use EXISTS to test if the outer queries value is present in the sub-query
-- Somtimes this is the only way to express this join type
SELECT EnglishProductName 'Product'
FROM DimProduct p
WHERE EXISTS
    (SELECT * -- no data is returned, only a boolean true/false 
     FROM DimProductSubcategory sc
     WHERE	p.ProductSubcategoryKey = sc.ProductSubcategoryKey
	 AND	sc.EnglishProductSubcategoryName = 'Wheels')

-- Show a 6 week rolling average of Weekly Sales for 2013

-- first create weekly sales totals
SELECT	SUM(s.SalesAmount) 'WeeklySales' 
	,DATEPART(ww, s.OrderDate) as 'WeekNum'
FROM	FactInternetSales s
WHERE	YEAR(s.OrderDate) = 2013
GROUP BY
	DATEPART(ww, s.OrderDate)
ORDER BY
	DATEPART(ww, s.OrderDate) ASC

-- use that subquery as our source and calculate the moving average
SELECT
	AVG(WeeklySales) OVER (ORDER BY WeekNum ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as AvgSales
	,WeeklySales as 'TotalSales'
	,WeekNum
FROM (
	SELECT	SUM(s.SalesAmount) 'WeeklySales' 
		,DATEPART(ww, s.OrderDate) as 'WeekNum'
	FROM	FactInternetSales s
	WHERE	YEAR(s.OrderDate) = 2013
	GROUP BY
		DATEPART(ww, s.OrderDate)
	) AS s
GROUP BY
		WeekNum, WeeklySales
ORDER BY
		WeekNum ASC

-- Running Total of Monthly sales for OrderYear 2013
SELECT
	SUM(MonthlySales) OVER (ORDER BY SalesMonth ROWS UNBOUNDED PRECEDING) as YTDSales
	,MonthlySales as 'MonthlySales'
	,SalesMonth
FROM (
	SELECT	SUM(s.SalesAmount) 'MonthlySales' 
		,	MONTH(s.OrderDate) as 'SalesMonth'
	FROM	FactInternetSales s
	WHERE	YEAR(s.OrderDate) = 2013
	GROUP BY
			MONTH(s.OrderDate)
	) AS s
GROUP BY
		SalesMonth, MonthlySales
ORDER BY
		SalesMonth ASC

-- Running Total of Monthly sales for all the years,(Running total resets at each year end)
SELECT
	SUM(MonthlySales) OVER (PARTITION BY SalesYear ORDER BY SalesMonth ROWS UNBOUNDED PRECEDING) as YTDSales
	,MonthlySales as 'MonthlySales'
	,SalesYear
	,SalesMonth
FROM (
	SELECT	SUM(s.SalesAmount) 'MonthlySales' 
		,MONTH(s.OrderDate) as 'SalesMonth'
		,year(s.OrderDate) as 'SalesYear'
	FROM	FactInternetSales s
	GROUP BY
		MONTH(s.OrderDate)
		,year(s.OrderDate)
	) AS s
GROUP BY
		SalesMonth, SalesYear, MonthlySales
ORDER BY
		SalesYear, SalesMonth ASC

-- Employee Table
select *
from DimEmployee

-- Analyzing Employee Data- There won't transaction records, it'll be a single record for each employee
-- How many active employees did we have on Nov 13th, 2013?
SELECT COUNT(1)
FROM DimEmployee emp
WHERE StartDate <= '2013-01-01'
AND	(
		EndDate > '2013-01-01'
	OR
		EndDate IS NULL
	)

-- start with dates table
select *
from DimDate  -- Date dimenssion

-- Show me a trend of active employees by each day
-- Start by getting the Daily count
SELECT
	dt.FullDateAlternateKey as 'Date'
	,count(1) as ActiveCount	
FROM DimDate dt
LEFT JOIN	(SELECT 'Active' as 'EmpStatus', * FROM DimEmployee) emp
	-- regular active employees
	ON (dt.FullDateAlternateKey between emp.StartDate and ISNULL(emp.EndDate,'9999-12-31'))
GROUP BY
		dt.FullDateAlternateKey
ORDER BY
		1

-- Show EOM Function - End of Months function
select DISTINCT top 20 EOMONTH(FullDateAlternateKey)
from DimDate d
order by 1


-- These counts are cumulative, so for monthly totals take the last day of the month
SELECT
	dt.FullDateAlternateKey as 'Date'
	,count(1) as ActiveCount	
FROM DimDate dt
LEFT JOIN	(SELECT 'Active' as 'EmpStatus', * FROM DimEmployee) emp
	-- regular active employees
	ON (dt.FullDateAlternateKey between emp.StartDate and ISNULL(emp.EndDate,'9999-12-31'))
WHERE
	dt.FullDateAlternateKey = EOMONTH(dt.FullDateAlternateKey)
GROUP BY
		dt.FullDateAlternateKey
ORDER BY
		1
-- Manupulation & Arithmatic operation on date & time fields
-- Get total sales for the month and show the last day of the month
SELECT 
		EOMONTH(OrderDate) as 'Month'
	,	SUM(SalesAmount) as 'Sales'
FROM FactInternetSales
GROUP BY
		EOMONTH(OrderDate)
ORDER BY 1

-- Calculate the customer acquisition funnel
SELECT
	c.FirstName
	,c.LastName
	,c.DateFirstPurchase
	,DATEDIFF(d,c.DateFirstPurchase,getdate()) as 'DaysSinceFirstPurchase' -- How long have they been a customer?
FROM DimCustomer c
ORDER BY 3 DESC


-- Calculate a Monthly average of customer tenure
SELECT
	EOMONTH(c.DateFirstPurchase) as 'MonthOfFirstPurchase' -- What month did they become a customer?
	,DATEDIFF(d,EOMONTH(c.DateFirstPurchase),getdate()) as 'DaysSinceFirstPurchase' -- How long have they been a customer?
	,COUNT(1) as 'CustomerCount' -- How manY customers are there for this month?
FROM DimCustomer c
GROUP BY EOMONTH(c.DateFirstPurchase)
ORDER BY 2 DESC


-- The data might not always be updated, so lets find the latest monthly sales amount

-- Get the most recent month
SELECT
	d.CalendarYear
	,d.MonthNumberOfYear
	,mdt.IsMaxDate
	,sum(s.SalesAmount) as 'TotalSales'

FROM DimDate d
JOIN FactInternetSales s ON d.DateKey = s.OrderDateKey
LEFT JOIN (
		SELECT
			1 as 'IsMaxDate',
			MAX(OrderDate) as 'MaxDate'
		FROM
			FactInternetSales
	) mdt
	ON
		d.CalendarYear = YEAR(mdt.MaxDate)
	AND
		d.MonthNumberOfYear = MONTH(mdt.MaxDate)

GROUP BY
		d.CalendarYear,
		d.MonthNumberOfYear,
		mdt.IsMaxDate

ORDER BY
		1 DESC,2 DESC

-- Common Table Expressions (CTEs)- Similar to subquary- Use for Hierarchy analysis, Lookups, self-referancing 

-- use a CTE to get an aggregate of an aggregate
-- Show number of profitable weeks
WITH Sales_CTE (Yr, WeekNum, WeeklySales)  
AS  
(  
    SELECT YEAR(OrderDate) as Yr, DATEPART(wk,OrderDate) as WeekNum, sum(SalesAmount) as WeeklySales
    FROM FactInternetSales  
    GROUP BY YEAR(OrderDate), DATEPART(wk,OrderDate) 
)  
SELECT *, CASE WHEN WeeklySales > 140000 THEN 1 ELSE 0 END as 'Profitable'
FROM Sales_CTE
ORDER BY 1,2 
GO  

-- Summarize by Year
WITH Sales_CTE (Yr, WeekNum, WeeklySales)  
AS  
(  
    SELECT YEAR(OrderDate) as Yr, DATEPART(wk,OrderDate) as WeekNum, sum(SalesAmount) as WeeklySales
    FROM FactInternetSales  
    GROUP BY YEAR(OrderDate), DATEPART(wk,OrderDate) 
)  
SELECT Yr, SUM(CASE WHEN WeeklySales > 140000 THEN 1 ELSE 0 END) as 'Profitable'
FROM Sales_CTE
GROUP BY Yr
ORDER BY 1 
GO



-------------------------------------------------------------------------
-- Use CTE to navigate employee hierarchy
WITH DirectReports (ManagerID, EmployeeID, Title, DeptID, Level)
AS
(
-- Anchor member definition
    SELECT e.ParentEmployeeKey, e.EmployeeKey, e.Title, e.DepartmentName, 
        0 AS Level
    FROM DimEmployee AS e
    WHERE e.ParentEmployeeKey IS NULL
    UNION ALL
-- Recursive member definition
    SELECT e.ParentEmployeeKey, e.EmployeeKey, e.Title, e.DepartmentName,
        Level + 1
    FROM DimEmployee AS e
    INNER JOIN DirectReports AS d
        ON e.ParentEmployeeKey = d.EmployeeID
)
-- Statement that executes the CTE
SELECT ManagerID, EmployeeID, Title, DeptID, Level
FROM DirectReports
WHERE DeptID = 'Information Services' OR Level = 0




-- Year over Year Calculations - Remove seasonality, Relative performance,True comparisons

-- Get Prev Year Sales
WITH MonthlySales (YearNum, MonthNum, Sales)
AS
(
	SELECT d.CalendarYear, d.MonthNumberOfYear, SUM(s.SalesAmount) 
	FROM DimDate d
	JOIN FactInternetSales s ON d.DateKey = s.OrderDateKey
	GROUP BY d.CalendarYear, d.MonthNumberOfYear
)
-- Get Current Year and join to CTE for previous year
SELECT 
	d.CalendarYear
	,d.MonthNumberOfYear
	,ms.Sales PrevSales
	,SUM(s.SalesAmount) CurrentSales
FROM DimDate d
JOIN FactInternetSales s ON d.DateKey = s.OrderDateKey
JOIN MonthlySales ms ON 
	d.CalendarYear-1 = ms.YearNum AND
	d.MonthNumberOfYear = ms.MonthNum
GROUP BY
	d.CalendarYear
	,d.MonthNumberOfYear
	,ms.Sales
ORDER BY
		1 DESC, 2 DESC


-- Now calculate the % change Year over Year
WITH MonthlySales (YearNum, MonthNum, Sales)
AS
(
	SELECT d.CalendarYear, d.MonthNumberOfYear, SUM(s.SalesAmount) 
	FROM DimDate d
	JOIN FactInternetSales s ON d.DateKey = s.OrderDateKey
	GROUP BY d.CalendarYear, d.MonthNumberOfYear
)
-- Get Current Year and join to CTE for previous year
SELECT 
	d.CalendarYear
	,d.MonthNumberOfYear
	,ms.Sales PrevSales
	,SUM(s.SalesAmount) CurrentSales
	,(SUM(s.SalesAmount) - ms.Sales) / SUM(s.SalesAmount) 'PctGrowth'
FROM DimDate d
JOIN FactInternetSales s ON d.DateKey = s.OrderDateKey
JOIN MonthlySales ms ON 
	d.CalendarYear-1 = ms.YearNum AND
	d.MonthNumberOfYear = ms.MonthNum
GROUP BY
	d.CalendarYear
	,d.MonthNumberOfYear
	,ms.Sales
ORDER BY
		1 DESC, 2 DESC

------------------------
---Find Ranks, A value for each row in a partition indicating its position in the resultt set
-- RANK() or DENSE_RANK(), ROW_NUMBER(), PERCENT_RANK()-Show relative % of row within that group of row
--Find the top products of 2013
-- using ROW_NUMBER() as a Rank function
-- fragile solution
SELECT 
	ROW_NUMBER() OVER (ORDER BY sum(s.SalesAmount) DESC)  AS 'Rank'
	,count(DISTINCT s.SalesOrderNumber) 'OrderCount' -- use 1 instead of a field for faster performance
	,sum(s.SalesAmount) 'Sales' 
	,cat.EnglishProductCategoryName 'Category'
    ,	sub.EnglishProductSubcategoryName 'SubCategory'	
FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
INNER JOIN DimProductSubcategory sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
INNER JOIN DimProductCategory cat ON sub.ProductCategoryKey = cat.ProductCategoryKey
-- filter
WHERE YEAR(s.OrderDate) = 2013 --use date function to parse year
-- must use group by in order for aggregation to work properly
GROUP BY
		cat.EnglishProductCategoryName -- column aliases aren't allowed
    ,	sub.EnglishProductSubcategoryName	

ORDER BY 3 DESC;


-- use RANK() function instead
-- when RANK() and ROW_NUBER() have the same order by the restults are the same
SELECT 
	ROW_NUMBER() OVER (ORDER BY sum(s.SalesAmount) DESC)  AS 'Rank'
	,count(DISTINCT s.SalesOrderNumber) 'OrderCount' -- use 1 instead of a field for faster performance
	,RANK() OVER (ORDER BY sum(s.SalesAmount) DESC) 'SalesRank' 
	,sum(s.SalesAmount) 'TotalSales'
	,cat.EnglishProductCategoryName 'Category'
    ,	sub.EnglishProductSubcategoryName 'SubCategory'	
FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
INNER JOIN DimProductSubcategory sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
INNER JOIN DimProductCategory cat ON sub.ProductCategoryKey = cat.ProductCategoryKey
-- filter
WHERE YEAR(s.OrderDate) = 2013 --use date function to parse year
-- must use group by in order for aggregation to work properly
GROUP BY
	cat.EnglishProductCategoryName -- column aliases aren't allowed
    	,sub.EnglishProductSubcategoryName	

ORDER BY cat.EnglishProductCategoryName, sub.EnglishProductSubcategoryName;


-- Show the top product Sub Categories for each year
SELECT 		
	count(DISTINCT s.SalesOrderNumber) 'OrderCount' -- use 1 instead of a field for faster performance
	,RANK() OVER (PARTITION BY YEAR(s.OrderDate) ORDER BY sum(s.SalesAmount) DESC) 'SalesRank' 
	,sum(s.SalesAmount) 'TotalSales'
	,cat.EnglishProductCategoryName 'Category'
    	,sub.EnglishProductSubcategoryName 'SubCategory'	
	,YEAR(s.OrderDate) 'Year'
FROM FactInternetSales s
INNER JOIN DimProduct p ON s.ProductKey = p.ProductKey
INNER JOIN DimProductSubcategory sub ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey
INNER JOIN DimProductCategory cat ON sub.ProductCategoryKey = cat.ProductCategoryKey
-- must use group by in order for aggregation to work properly
GROUP BY
	cat.EnglishProductCategoryName -- column aliases aren't allowed
    	,sub.EnglishProductSubcategoryName	
	,YEAR(s.OrderDate)

ORDER BY YEAR(s.OrderDate), SUM(s.SalesAmount) DESC;

-- Using PIVOT

USE AdventureWorks2014;
GO
SELECT VendorID, [250] AS Emp1, [251] AS Emp2, [256] AS Emp3, [257] AS Emp4, [260] AS Emp5
FROM 
(SELECT PurchaseOrderID, EmployeeID, VendorID
FROM Purchasing.PurchaseOrderHeader) p
PIVOT
(
COUNT (PurchaseOrderID)
FOR EmployeeID IN
( [250], [251], [256], [257], [260] )
) AS pvt
ORDER BY pvt.VendorID;
