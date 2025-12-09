-- Active: 1758560898007@@relational.fel.cvut.cz@3306@tpcds
WITH customer_sales AS (
    SELECT 
        bill_customer_sk,
        COUNT(*) as total_transactions,
        SUM(total_sales) as total_sales_amount,
        AVG(total_sales) as avg_transaction_value,
        SUM(quantity) as total_items_purchased,
        COUNT(DISTINCT sale_date) as distinct_purchase_days,
        -- Channel preferences
        SUM(CASE WHEN channel = 'store' THEN total_sales ELSE 0 END) as store_sales,
        SUM(CASE WHEN channel = 'catalog' THEN total_sales ELSE 0 END) as catalog_sales,
        SUM(CASE WHEN channel = 'web' THEN total_sales ELSE 0 END) as web_sales,
        SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END) as store_transactions,
        SUM(CASE WHEN channel = 'catalog' THEN 1 ELSE 0 END) as catalog_transactions,
        SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) as web_transactions
    FROM (
        -- Store sales
        SELECT 
            ss_customer_sk as bill_customer_sk,
            ss_sold_date_sk as sale_date,
            ss_ext_sales_price as total_sales,
            ss_quantity as quantity,
            'store' as channel
        FROM store_sales
        
        UNION ALL
        
        -- Catalog sales
        SELECT 
            cs_bill_customer_sk as bill_customer_sk,
            cs_sold_date_sk as sale_date,
            cs_ext_sales_price as total_sales,
            cs_quantity as quantity,
            'catalog' as channel
        FROM catalog_sales
        
        UNION ALL
        
        -- Web sales
        SELECT 
            ws_bill_customer_sk as bill_customer_sk,
            ws_sold_date_sk as sale_date,
            ws_ext_sales_price as total_sales,
            ws_quantity as quantity,
            'web' as channel
        FROM web_sales
    ) all_sales
    GROUP BY bill_customer_sk
),

customer_returns AS (
    SELECT 
        bill_customer_sk,
        COUNT(*) as total_returns,
        SUM(return_amount) as total_return_amount,
        SUM(return_quantity) as total_return_quantity
    FROM (
        -- Store returns
        SELECT 
            sr_customer_sk as bill_customer_sk,
            sr_return_amt as return_amount,
            sr_return_quantity as return_quantity
        FROM store_returns
        
        UNION ALL
        
        -- Catalog returns
        SELECT 
            cr_refunded_customer_sk as refunded_customer_sk,
            cr_return_amount as return_amount,
            cr_return_quantity as return_quantity
        FROM catalog_returns
        
        UNION ALL
        
        -- Web returns
        SELECT 
            wr_refunded_customer_sk as bill_customer_sk,
            wr_return_amt as return_amount,
            wr_return_quantity as return_quantity
        FROM web_returns
    ) all_returns
    GROUP BY bill_customer_sk
)

SELECT 
    c.c_customer_sk,
    c.c_preferred_cust_flag,
    
    -- Customer demographics
    c.c_birth_country,
    c.c_birth_year,
    c.c_birth_month,
    c.c_birth_day,
    c.c_salutation,
    c.c_first_name,
    c.c_last_name,
    c.c_email_address,
    
    -- Customer demographic details
    cd.cd_gender,
    cd.cd_marital_status,
    cd.cd_education_status,
    cd.cd_purchase_estimate,
    cd.cd_credit_rating,
    cd.cd_dep_count as dependent_count,
    cd.cd_dep_employed_count as dependents_employed,
    cd.cd_dep_college_count as dependents_in_college,
    
    -- Household demographics
    hd.hd_buy_potential,
    hd.hd_dep_count as household_dependent_count,
    hd.hd_vehicle_count,
    
    -- Income band
    ib.ib_lower_bound as income_lower_bound,
    ib.ib_upper_bound as income_upper_bound,
    
    -- Address information
    ca.ca_gmt_offset,
    ca.ca_location_type,
    
    -- Sales aggregates
    COALESCE(cs.total_transactions, 0) as total_transactions,
    COALESCE(cs.total_sales_amount, 0) as total_sales_amount,
    COALESCE(cs.avg_transaction_value, 0) as avg_transaction_value,
    COALESCE(cs.total_items_purchased, 0) as total_items_purchased,
    COALESCE(cs.distinct_purchase_days, 0) as distinct_purchase_days,
    
    -- Channel preferences (as percentages)
    CASE 
        WHEN COALESCE(cs.total_sales_amount, 0) > 0 
        THEN COALESCE(cs.store_sales, 0) / cs.total_sales_amount 
        ELSE 0 
    END as store_sales_pct,
    CASE 
        WHEN COALESCE(cs.total_sales_amount, 0) > 0 
        THEN COALESCE(cs.catalog_sales, 0) / cs.total_sales_amount 
        ELSE 0 
    END as catalog_sales_pct,
    CASE 
        WHEN COALESCE(cs.total_sales_amount, 0) > 0 
        THEN COALESCE(cs.web_sales, 0) / cs.total_sales_amount 
        ELSE 0 
    END as web_sales_pct,
    
    -- Channel transaction counts
    COALESCE(cs.store_transactions, 0) as store_transactions,
    COALESCE(cs.catalog_transactions, 0) as catalog_transactions,
    COALESCE(cs.web_transactions, 0) as web_transactions,
    
    -- Return metrics
    COALESCE(cr.total_returns, 0) as total_returns,
    COALESCE(cr.total_return_amount, 0) as total_return_amount,
    COALESCE(cr.total_return_quantity, 0) as total_return_quantity,
    
    -- Return rates
    CASE 
        WHEN COALESCE(cs.total_transactions, 0) > 0 
        THEN COALESCE(cr.total_returns, 0) * 1.0 / cs.total_transactions 
        ELSE 0 
    END as return_rate,
    CASE 
        WHEN COALESCE(cs.total_sales_amount, 0) > 0 
        THEN COALESCE(cr.total_return_amount, 0) / cs.total_sales_amount 
        ELSE 0 
    END as return_amount_rate,
    
    -- Net sales (after returns)
    COALESCE(cs.total_sales_amount, 0) - COALESCE(cr.total_return_amount, 0) as net_sales_amount

FROM customer c
LEFT JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
LEFT JOIN household_demographics hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
LEFT JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
LEFT JOIN income_band ib ON hd.hd_income_band_sk = ib.ib_income_band_sk
LEFT JOIN customer_sales cs ON c.c_customer_sk = cs.bill_customer_sk
LEFT JOIN customer_returns cr ON c.c_customer_sk = cr.bill_customer_sk;