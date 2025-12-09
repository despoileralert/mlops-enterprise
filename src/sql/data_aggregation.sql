WITH sales_aggregated AS (

    SELECT 
        c.c_customer_sk,
        SUM(COALESCE(cs.cs_ext_sales_price, 0)) as catalog_sales_amount,
        SUM(COALESCE(cs.cs_net_profit, 0)) as catalog_net_profit,
        SUM(COALESCE(cs.cs_quantity, 0)) as catalog_quantity,
        MAX(CASE WHEN cs.cs_item_sk IS NOT NULL THEN 1 ELSE 0 END) as has_catalog_sales
    FROM customer c
    LEFT JOIN catalog_sales cs ON c.c_customer_sk = cs.cs_bill_customer_sk
    GROUP BY c.c_customer_sk
),
web_aggregated AS (
    SELECT 
        c.c_customer_sk,
        SUM(COALESCE(ws.ws_ext_sales_price, 0)) as web_sales_amount,
        SUM(COALESCE(ws.ws_net_profit, 0)) as web_net_profit,
        SUM(COALESCE(ws.ws_quantity, 0)) as web_quantity,
        MAX(CASE WHEN ws.ws_item_sk IS NOT NULL THEN 1 ELSE 0 END) as has_web_sales
    FROM customer c
    LEFT JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
    GROUP BY c.c_customer_sk
),
store_aggregated AS (
    SELECT 
        c.c_customer_sk,
        SUM(ss.ss_ext_sales_price) as store_sales_amount,
        SUM(ss.ss_net_profit) as store_net_profit,
        SUM(ss.ss_quantity) as store_quantity,
        COUNT(*) as store_transaction_count
    FROM customer c
    INNER JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
    INNER JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
    GROUP BY c.c_customer_sk
),
returns_aggregated AS (
    SELECT 
        c.c_customer_sk,
        SUM(COALESCE(cr.cr_return_amount, 0)) as catalog_return_amount,
        SUM(COALESCE(wr.wr_return_amt, 0)) as web_return_amount,
        SUM(COALESCE(sr.sr_return_amt, 0)) as store_return_amount
    FROM customer c
    LEFT JOIN catalog_returns cr ON c.c_customer_sk = cr.cr_refunded_customer_sk
    LEFT JOIN web_returns wr ON c.c_customer_sk = wr.wr_refunded_customer_sk
    LEFT JOIN store_returns sr ON c.c_customer_sk = sr.sr_customer_sk
    GROUP BY c.c_customer_sk
)
SELECT 
    c.c_customer_sk,
    c.c_preferred_cust_flag,
    

    hd.hd_demo_sk, hd.hd_income_band_sk, hd.hd_buy_potential, 
    hd.hd_dep_count, hd.hd_vehicle_count,
    cd.cd_gender, cd.cd_marital_status, cd.cd_education_status, cd.cd_credit_rating,
    ca.ca_city, ca.ca_state, ca.ca_country, ca.ca_location_type,
    

    COALESCE(sa.has_catalog_sales, 0) as has_catalog_sales,
    COALESCE(wa.has_web_sales, 0) as has_web_sales,
    1 as has_store_sales, 
    

    COALESCE(sa.catalog_sales_amount, 0) as catalog_sales_amount,
    COALESCE(wa.web_sales_amount, 0) as web_sales_amount,
    st.store_sales_amount,
    

    COALESCE(sa.catalog_net_profit, 0) as catalog_net_profit,
    COALESCE(wa.web_net_profit, 0) as web_net_profit,
    st.store_net_profit,
    

    COALESCE(ra.catalog_return_amount, 0) as catalog_return_amount,
    COALESCE(ra.web_return_amount, 0) as web_return_amount,
    COALESCE(ra.store_return_amount, 0) as store_return_amount,
    

    (COALESCE(sa.catalog_sales_amount, 0) + 
     COALESCE(wa.web_sales_amount, 0) + 
     st.store_sales_amount) as total_sales_amount,
    
    (COALESCE(sa.catalog_net_profit, 0) + 
     COALESCE(wa.web_net_profit, 0) + 
     st.store_net_profit) as total_net_profit,
    
    (COALESCE(ra.catalog_return_amount, 0) + 
     COALESCE(ra.web_return_amount, 0) + 
     COALESCE(ra.store_return_amount, 0)) as total_return_amount,
    
    (COALESCE(sa.has_catalog_sales, 0) + 
     COALESCE(wa.has_web_sales, 0) + 1) as active_channels_count,
    

    st.store_transaction_count,
    
    CASE 
        WHEN (COALESCE(sa.catalog_sales_amount, 0) + COALESCE(wa.web_sales_amount, 0) + st.store_sales_amount) > 0 
        THEN (COALESCE(ra.catalog_return_amount, 0) + COALESCE(ra.web_return_amount, 0) + COALESCE(ra.store_return_amount, 0)) / 
             (COALESCE(sa.catalog_sales_amount, 0) + COALESCE(wa.web_sales_amount, 0) + st.store_sales_amount) * 100
        ELSE 0 
    END as return_rate_percentage

FROM customer c

INNER JOIN household_demographics hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
INNER JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
INNER JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk

INNER JOIN store_aggregated st ON c.c_customer_sk = st.c_customer_sk
LEFT JOIN sales_aggregated sa ON c.c_customer_sk = sa.c_customer_sk
LEFT JOIN web_aggregated wa ON c.c_customer_sk = wa.c_customer_sk
LEFT JOIN returns_aggregated ra ON c.c_customer_sk = ra.c_customer_sk

ORDER BY total_sales_amount DESC;