--Total Sales by Category
SELECT
    c.CategoryID,
    c.CategoryName,
    SUM(ExtendedPrice) AS TotalSales
FROM
    Products p
JOIN
    Categories c ON p.CategoryID = c.CategoryID
JOIN
    [Order Details Extended] od ON p.ProductID = od.ProductID
JOIN
    Orders o ON od.OrderID = o.OrderID
GROUP BY
    c.CategoryID, c.CategoryName;


--Sales Growth Rate by Year
WITH SalesCTE AS (
    SELECT
        o.OrderID,
        o.OrderDate,
        YEAR(o.OrderDate) AS OrderYear,
        MONTH(o.OrderDate) AS OrderMonth,
        SUM(od.ExtendedPrice) AS TotalSales
    FROM Orders o
    INNER JOIN [Order Details Extended] od ON o.OrderID = od.OrderID
    GROUP BY o.OrderID, o.OrderDate, YEAR(o.OrderDate), MONTH(o.OrderDate)
)

SELECT
    OrderYear,
    SUM(TotalSales) AS TotalSales,
    LAG(SUM(TotalSales)) OVER (ORDER BY OrderYear) AS PreviousTotalSales,
    CASE
        WHEN LAG(SUM(TotalSales)) OVER (ORDER BY OrderYear) IS NOT NULL
        THEN ((SUM(TotalSales) - LAG(SUM(TotalSales)) OVER (ORDER BY OrderYear)) / LAG(SUM(TotalSales)) OVER (ORDER BY OrderYear)) * 100
        ELSE NULL
    END AS SalesGrowthRate
FROM SalesCTE
GROUP BY OrderYear
ORDER BY OrderYear;



--Yearly Total Sales by Product:
SELECT
    YEAR(o.OrderDate) AS Year,
    p.ProductID,
    p.ProductName,
    SUM(od.Quantity) AS TotalQuantity,
    SUM((od.UnitPrice * od.Quantity * (1 - od.Discount))) AS TotalSales
FROM
    Orders o
    JOIN [Order Details Extended] od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
WHERE
    o.OrderDate IS NOT NULL
GROUP BY
    YEAR(o.OrderDate),
    p.ProductID,
    p.ProductName
ORDER BY
    Year, TotalSales DESC;


--Monthly Total Sales by Product:
SELECT
    YEAR(o.OrderDate) AS Year,
    MONTH(o.OrderDate) AS Month,
    p.ProductID,
    p.ProductName,
    SUM(od.Quantity) AS TotalQuantity,
    SUM(od.ExtendedPrice) AS TotalSales
FROM
    Orders o
    JOIN [Order Details Extended] od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
WHERE
    o.OrderDate IS NOT NULL
GROUP BY
    YEAR(o.OrderDate),
    MONTH(o.OrderDate),
    p.ProductID,
    p.ProductName
ORDER BY
Month, TotalSales DESC;


--Average Yearly Sales per Order by Product:
SELECT
    YEAR(o.OrderDate) AS Year,
    p.ProductID,
    p.ProductName,
    AVG(od.Quantity) AS AvgQuantityPerOrder,
    AVG((od.UnitPrice * od.Quantity * (1 - od.Discount))) AS AvgSalesPerOrder
FROM
    Orders o
    JOIN [Order Details Extended] od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
WHERE
    o.OrderDate IS NOT NULL
GROUP BY
    YEAR(o.OrderDate),
    p.ProductID,
    p.ProductName
ORDER BY
    Year, AvgSalesPerOrder DESC;

--Average Monthly Sales  per Order Product:
SELECT
    YEAR(o.OrderDate) AS Year,
    MONTH(o.OrderDate) AS Month,
    p.ProductID,
    p.ProductName,
    AVG(od.Quantity) AS AvgQuantityPerOrder,
    AVG((od.UnitPrice * od.Quantity * (1 - od.Discount))) AS AvgSalesPerOrder
FROM
    Orders o
    JOIN [Order Details Extended] od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
WHERE
    o.OrderDate IS NOT NULL
GROUP BY
    YEAR(o.OrderDate),
    MONTH(o.OrderDate),
    p.ProductID,
    p.ProductName
ORDER BY
    Month, AvgSalesPerOrder DESC;

--Top 10 Selling Products by Quantity
SELECT TOP 10
    Products.ProductID,
    Products.ProductName,
    Categories.CategoryID,
    Categories.CategoryName,
    SUM([Order Details Extended].Quantity) AS TotalSold

FROM
    Products
JOIN
    [Order Details Extended] ON [Order Details Extended].ProductID = Products.ProductID
JOIN
    Categories ON Categories.CategoryID = Products.CategoryID
GROUP BY
    Products.ProductID,
    Products.ProductName,
    Categories.CategoryID,
    Categories.CategoryName
ORDER BY
    TotalSold DESC;

--Top 10 Selling Products by Amount
SELECT TOP 10
    Products.ProductID,
    Products.ProductName,
    Categories.CategoryID,
    Categories.CategoryName,
	sum([Order Details Extended].ExtendedPrice) as TotalSales

FROM
    Products
JOIN
    [Order Details Extended] ON [Order Details Extended].ProductID = Products.ProductID
JOIN
    Categories ON Categories.CategoryID = Products.CategoryID
GROUP BY
    Products.ProductID,
    Products.ProductName,
    Categories.CategoryID,
    Categories.CategoryName
ORDER BY
    TotalSales DESC;


--Orders Processed Per Employee
WITH cte_employee_orders AS (
  SELECT
    o.EmployeeID,
    COUNT(OrderID) AS OrdersProcessed
  FROM Orders o
  INNER JOIN Employees e ON o.EmployeeID = e.EmployeeID
  GROUP BY o.EmployeeID
)
SELECT
  e.EmployeeID,
  e.FirstName,
  e.LastName,
  eo.OrdersProcessed
FROM Employees e
INNER JOIN cte_employee_orders eo ON e.EmployeeID = eo.EmployeeID
ORDER BY eo.OrdersProcessed DESC;


-- Calculate sell-through rate for each product
SELECT
    p.ProductID,
    p.ProductName,
    UnitsSold / NULLIF(p.UnitsInStock, 0) AS SellThroughRate
FROM (
    SELECT
        od.ProductID,
        SUM(od.Quantity) AS UnitsSold
    FROM [Order Details Extended] od
    INNER JOIN Orders o ON od.OrderID = o.OrderID
    WHERE o.OrderDate BETWEEN CONVERT(DATETIME, '1996-01-01', 120) AND CONVERT(DATETIME, '1998-12-31', 120)
    GROUP BY od.ProductID
) AS SoldUnits
INNER JOIN Products AS p ON SoldUnits.ProductID = p.ProductID;

--Create Sell-Through Table
INSERT INTO SellThroughTable (OrderYear, TotalQuantitySold, TotalUnitsInStock)
SELECT
    OrderYear,
    TotalQuantitySold,
    TotalUnitsInStock
FROM (
    SELECT
        YEAR(Orders.OrderDate) AS OrderYear,
        SUM([Order Details Extended].Quantity) AS TotalQuantitySold,
        SUM(Products.UnitsInStock) AS TotalUnitsInStock
    FROM
        [Order Details Extended]
    JOIN
        Orders ON [Order Details Extended].OrderID = Orders.OrderID
    JOIN
        Products ON [Order Details Extended].ProductID = Products.ProductID
    GROUP BY
        YEAR(Orders.OrderDate)
) AS SellThroughTable;
--Sell-Through Rate by Year
SELECT
    OrderYear,
    SUM(TotalQuantitySold) AS TotalQuantitySold,
    SUM(TotalUnitsInStock) AS TotalUnitsInStock,
    ROUND(SUM(CAST(TotalQuantitySold AS DECIMAL)) / SUM(CAST(TotalUnitsInStock AS DECIMAL)) * 100, 2) AS SellThroughRate
FROM SellThroughTable
GROUP BY OrderYear;


-- Calculate stockout rate for all orders
SELECT
    p.ProductName,
    COUNT(o.OrderID) AS TotalOrders,
    SUM(CASE WHEN o.ShippedDate > o.RequiredDate THEN 1 ELSE 0 END) AS Backorders,
    CAST(SUM(CASE WHEN o.ShippedDate > o.RequiredDate THEN 1 ELSE 0 END) AS DECIMAL(5, 2)) / COUNT(*) * 100 AS StockoutRatePercent
FROM Orders o
INNER JOIN [Order Details] od ON o.OrderID = od.OrderID
INNER JOIN Products p ON od.ProductID = p.ProductID
WHERE CONVERT(DATETIME, o.OrderDate, 120) BETWEEN CONVERT(DATETIME, '1996-01-01', 120) AND CONVERT(DATETIME, '1998-12-31', 120)
GROUP BY p.ProductName
ORDER BY StockoutRatePercent DESC;


--Late Deliveries Rate
SELECT
    ROUND((SUM(CASE WHEN ShippedDate > RequiredDate THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) AS LateDeliveryRate
FROM
    Orders;


-- Calculate stockout rate for all orders
SELECT
    p.ProductName,
    COUNT(o.OrderID) AS TotalOrders,
    SUM(CASE WHEN o.ShippedDate > o.RequiredDate THEN 1 ELSE 0 END) AS Backorders,
    CAST(SUM(CASE WHEN o.ShippedDate > o.RequiredDate THEN 1 ELSE 0 END) AS DECIMAL(5, 2)) / COUNT(*) * 100 AS StockoutRatePercent
FROM Orders o
INNER JOIN [Order Details] od ON o.OrderID = od.OrderID
INNER JOIN Products p ON od.ProductID = p.ProductID
WHERE CONVERT(DATETIME, o.OrderDate, 120) BETWEEN CONVERT(DATETIME, '1996-01-01', 120) AND CONVERT(DATETIME, '1998-12-31', 120)
GROUP BY p.ProductName
ORDER BY StockoutRatePercent DESC;


