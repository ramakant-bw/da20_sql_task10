-- Creating Database For Reports

Create Database Reports



-- For Connecting the Reports database with ecom we use Dblink extention

CREATE EXTENSION dblink;



--Creating Procedures to generate monthly and yearly report on furniture 

CREATE OR REPLACE PROCEDURE generate_furniture_reports_cross_db_rounded()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create the monthly report table with a primary key if it doesn't exist
    CREATE TABLE IF NOT EXISTS monthly_furniture_reports (
        shipment_month VARCHAR(3),
        shipment_year VARCHAR(4),
        total_sales NUMERIC,
        total_profit NUMERIC,
        total_orders BIGINT,
        avg_sales_per_order NUMERIC,
        avg_profit_per_order NUMERIC,
        profit_margin NUMERIC,
        PRIMARY KEY (shipment_month, shipment_year)
    );

    -- Create the yearly report table with a primary key if it doesn't exist
    CREATE TABLE IF NOT EXISTS yearly_furniture_reports (
        shipment_year VARCHAR(4),
        total_sales NUMERIC,
        total_profit NUMERIC,
        total_orders BIGINT,
        avg_sales_per_order NUMERIC,
        avg_profit_per_order NUMERIC,
        profit_margin NUMERIC,
        PRIMARY KEY (shipment_year)
    );

    -- Use dblink to get monthly furniture report data and upsert it
    INSERT INTO monthly_furniture_reports (
        shipment_month, shipment_year, total_sales, total_profit, total_orders, 
        avg_sales_per_order, avg_profit_per_order, profit_margin
    )
    SELECT *
    FROM dblink('dbname=ecom user=postgres password=admin',
        'SELECT
            TO_CHAR(ship_date, ''Mon''),
            TO_CHAR(ship_date, ''YYYY''),
            SUM(sales),
            SUM(profit),
            COUNT(DISTINCT order_id),
            ROUND((SUM(sales) / COUNT(DISTINCT order_id))::NUMERIC, 2),
            ROUND((SUM(profit) / COUNT(DISTINCT order_id))::NUMERIC, 2),
            ROUND((SUM(profit) / SUM(sales) * 100)::NUMERIC, 2)
        FROM
            ecom_data
        WHERE
            category = ''Furniture''
        GROUP BY
            TO_CHAR(ship_date, ''YYYY''),
            TO_CHAR(ship_date, ''Mon'')')
    AS monthly_results(
        shipment_month VARCHAR(3),
        shipment_year VARCHAR(4),
        total_sales NUMERIC,
        total_profit NUMERIC,
        total_orders BIGINT,
        avg_sales_per_order NUMERIC,
        avg_profit_per_order NUMERIC,
        profit_margin NUMERIC
    )
    ON CONFLICT (shipment_month, shipment_year) DO UPDATE
    SET
        total_sales = EXCLUDED.total_sales,
        total_profit = EXCLUDED.total_profit,
        total_orders = EXCLUDED.total_orders,
        avg_sales_per_order = EXCLUDED.avg_sales_per_order,
        avg_profit_per_order = EXCLUDED.avg_profit_per_order,
        profit_margin = EXCLUDED.profit_margin;

    -- Use dblink to get yearly furniture report data and upsert it
    INSERT INTO yearly_furniture_reports (
        shipment_year, total_sales, total_profit, total_orders,
        avg_sales_per_order, avg_profit_per_order, profit_margin
    )
    SELECT *
    FROM dblink('dbname=ecom user=postgres password=admin',
        'SELECT
            TO_CHAR(ship_date, ''YYYY''),
            SUM(sales),
            SUM(profit),
            COUNT(DISTINCT order_id),
            ROUND((SUM(sales) / COUNT(DISTINCT order_id))::NUMERIC, 2),
            ROUND((SUM(profit) / COUNT(DISTINCT order_id))::NUMERIC, 2),
            ROUND((SUM(profit) / SUM(sales) * 100)::NUMERIC, 2)
        FROM
            ecom_data
        WHERE
            category = ''Furniture''
        GROUP BY
            TO_CHAR(ship_date, ''YYYY'')')
    AS yearly_results(
        shipment_year VARCHAR(4),
        total_sales NUMERIC,
        total_profit NUMERIC,
        total_orders BIGINT,
        avg_sales_per_order NUMERIC,
        avg_profit_per_order NUMERIC,
        profit_margin NUMERIC
    )
    ON CONFLICT (shipment_year) DO UPDATE
    SET
        total_sales = EXCLUDED.total_sales,
        total_profit = EXCLUDED.total_profit,
        total_orders = EXCLUDED.total_orders,
        avg_sales_per_order = EXCLUDED.avg_sales_per_order,
        avg_profit_per_order = EXCLUDED.avg_profit_per_order,
        profit_margin = EXCLUDED.profit_margin;
END;
$$;




--Generating Reports by calling procedure
CALL generate_furniture_reports_cross_db_rounded();




select * from monthly_furniture_reports
select * from yearly_furniture_reports

