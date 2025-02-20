USE ttansey_db;

CREATE SCHEMA assignment01
CREATE TABLE assignment01.bakery_sales (
sale_date DATE,
    sale_time TIME,
    ticket_number INT,
    article VARCHAR(255),
    quantity INT,
    unit_price DECIMAL(10, 2),
    sale_datetime DATETIME
);

INSERT INTO assignment01.bakery_sales (sale_date, sale_time, ticket_number, article, quantity, unit_price, sale_datetime)
SELECT sale_date, sale_time, ticket_number, article, quantity, unit_price, sale_datetime
FROM mban_db.assignment01.bakery_sales;

--Count total number of records
SELECT COUNT (*) AS nrecords
FROM assignment01.bakery_sales;

-- List distinct articles
SELECT DISTINCT article
FROM assignment01.bakery_sales
ORDER BY article;

-- Count the unique articles
SELECT COUNT(DISTINCT article) N_unique_articles
FROM assignment01.bakery_sales;


-- QUESTION 1:
--Calculate monthly totals
WITH monthly_sales AS (
    SELECT article,
        YEAR(sale_datetime) AS year,
        MONTH(sale_datetime) AS month,
        SUM(quantity) AS total_quantity_sold,
        SUM(quantity * unit_price) AS total_revenue
    FROM assignment01.bakery_sales
    GROUP BY article,
        YEAR(sale_datetime),
        MONTH(sale_datetime)
    ),
--Rank articles within each month
ranked_sales AS (
    SELECT article AS most_sold_items,
           year,
           month,
           total_quantity_sold,
           FORMAT((total_revenue), 'C', 'en-CA') AS total_revenue,
   ROW_NUMBER() OVER (PARTITION BY YEAR
   , MONTH
    ORDER BY total_quantity_sold DESC) AS rank
    FROM
    monthly_sales
    )
--Select top 3 articles for each month.
SELECT year,
       month,
       rank,
       most_sold_items,
       total_quantity_sold,
       total_revenue
FROM ranked_sales
WHERE
    rank <= 3
ORDER BY
    year,
    month,
    rank;


--QUESTION 2
WITH N_unique_items AS (
    SELECT
        ticket_number,
        COUNT(DISTINCT article) AS unique_articles_count,
        SUM(quantity) AS total_quantity_per_ticket
    FROM
        assignment01.bakery_sales
    WHERE
        sale_date >= '2021-12-01'
        AND sale_date < '2022-01-01'
    GROUP BY
        ticket_number
    HAVING
        COUNT(DISTINCT article) >= 5
)
-- Display each ticket number once with total unique items per ticket
SELECT
    ticket_number,
    unique_articles_count AS N_unique_items
FROM
    N_unique_items
ORDER BY
    ticket_number;


--QUESTION 3
WITH July_sales_by_hour AS (
    SELECT
        'Traditional Baguette' AS article_name,
        FORMAT(sale_datetime, 'yyyy-MM-dd') AS sale_day,
        DATEPART(HOUR, sale_datetime) AS sale_hour,
        SUM(quantity) AS total_quantity,
        SUM(quantity * unit_price) AS total_revenue
    FROM assignment01.bakery_sales
    WHERE article = 'Traditional Baguette'
        AND (
            (sale_datetime >= '2021-07-01' AND sale_datetime < '2021-08-01') OR
            (sale_datetime >= '2022-07-01' AND sale_datetime < '2022-08-01')
        )
    GROUP BY FORMAT(sale_datetime, 'yyyy-MM-dd'),
        DATEPART(HOUR, sale_datetime)
),
ranked_sales AS (
    SELECT
        article_name,
        sale_day,
        sale_hour,
        total_quantity,
        total_revenue,
        ROW_NUMBER() OVER (PARTITION BY sale_day ORDER BY total_quantity DESC) AS rank
    FROM
        July_sales_by_hour
)
SELECT
    article_name,
    sale_day,
    CONCAT(CAST(sale_hour AS VARCHAR(2)), '-', CAST(sale_hour + 1 AS VARCHAR(2))) AS most_popular_hour,
    total_quantity,
    total_revenue
FROM
    ranked_sales
WHERE
    rank = 1
ORDER BY
    sale_day;


-- QUESTION 4
--Missing Values
-- Check for missing values
SELECT
    SUM(CASE WHEN sale_date IS NULL THEN 1 ELSE 0 END) AS missing_sale_date,
    SUM(CASE WHEN sale_time IS NULL THEN 1 ELSE 0 END) AS missing_sale_time,
    SUM(CASE WHEN ticket_number IS NULL THEN 1 ELSE 0 END) AS missing_ticket_number,
    SUM(CASE WHEN article IS NULL THEN 1 ELSE 0 END) AS missing_article,
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS missing_quantity,
    SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS missing_unit_price,
    SUM(CASE WHEN sale_datetime IS NULL THEN 1 ELSE 0 END) AS missing_sale_datetime
FROM
    assignment01.bakery_sales;

--to view null values in missing_unit_price column
SELECT DISTINCT ticket_number
FROM assignment01.bakery_sales
WHERE unit_price IS NULL;

--Delete null rows
DELETE FROM assignment01.bakery_sales
    WHERE ticket_number IN(159219, 164878, 170079, 186662, 161853);

--Check if any other article row contains a "." instead of an article name
SELECT
    SUM(CASE WHEN article = '.' THEN 1 ELSE 0 END) AS missing_article_name
FROM assignment01.bakery_sales;

-- Details on Null values
-- 5 missing values in unit price
-- Ticket numbers : 159219, 164878, 170079, 186662, 161853
-- All these tickets have no unit price and a '.' as the article name
-- These will be removed instead of replaced with a value because the article is unclear,
-- therefore no actual values can be attributed to these tickets, thus decreasing the accuracy of analytics


--Duplicates
SELECT
    sale_date,
    sale_time,
    ticket_number,
    article,
    quantity,
    unit_price,
    sale_datetime,
    COUNT(*) AS count
FROM
    assignment01.bakery_sales
GROUP BY
    sale_date,
    sale_time,
    ticket_number,
    article,
    quantity,
    unit_price,
    sale_datetime
HAVING
    COUNT(*) > 1;

WITH DuplicatesCTE AS (
    SELECT
        sale_date,
        sale_time,
        ticket_number,
        article,
        quantity,
        unit_price,
        sale_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY sale_date, sale_time, ticket_number, article, quantity, unit_price, sale_datetime
            ORDER BY (SELECT NULL)
        ) AS row_num
    FROM
        assignment01.bakery_sales
)
DELETE FROM DuplicatesCTE
WHERE row_num > 1;

-- Details on Duplicates
-- 1155 total duplicate records deleted from a total of 234,005 records.



--Outliers
--data analysis (determines average, min, and max ages) for outliers
SELECT AVG(bs.quantity) AS avg_quantity,
       MIN(bs.quantity) AS min_quantity,
       MAX(bs.quantity) AS max_quantity
FROM assignment01.bakery_sales AS bs;

-- to only display quantities with less than or equal to 0
SELECT bs.ticket_number,
       bs.quantity
FROM assignment01.bakery_sales AS bs
WHERE bs.quantity <= 0;

-- to only display quantities with greater than or equal to 50
SELECT bs.ticket_number,
       bs.quantity
FROM assignment01.bakery_sales AS bs
WHERE bs.quantity >=50 ;

--Exclude outliers from data
SELECT *
FROM assignment01.bakery_sales
WHERE quantity < -100 OR quantity > 100;

--Delete outliers
DELETE FROM assignment01.bakery_sales
WHERE ticket_number IN ('179931', '179932');

--Verify deletion
SELECT *
FROM assignment01.bakery_sales
WHERE ticket_number IN  ('179931','179932');

-- Details on outliers
-- 500 tickets with negative quantity amounts (mostly between -4 - -1 (likely returns)
-- Flagged concerns: handled by deleting ;Substantial outlier of -200  to be classified as outlier,
-- likely a return for an error (max quantity was also at 200 and only 1 ticket);
-- both of these transaction take place on the same day (Jun 12/21 at 9:58;
-- Consecutive ticket numbers  --- appears to be a mistake correction
-- Will be removed (2 records deleted)179931','179932 are the ticket numbers of the chosen outliers
