WITH cte as (SELECT 
    c.c_customer_sk, c.c_preferred_cust_flag,
    hd.*,
    cd.cd_gender, cd.cd_marital_status, cd.cd_education_status, cd.cd_credit_rating,
    ca.ca_city, ca.ca_state, ca.ca_country, ca.ca_location_type,
    cs.cs_item_sk, cs.cs_quantity, ws.ws_item_sk, ws.ws_quantity, ss.ss_item_sk, ss.ss_quantity,
    
    -- Channel participation flags
    CASE WHEN cs.cs_item_sk IS NOT NULL THEN 1 ELSE 0 END as has_catalog_sales,
    CASE WHEN ws.ws_item_sk IS NOT NULL THEN 1 ELSE 0 END as has_web_sales,
    CASE WHEN ss.ss_item_sk IS NOT NULL THEN 1 ELSE 0 END as has_store_sales,
    
    -- Aggregated metrics across all channels
    COALESCE(cs.cs_ext_sales_price, 0) + 
    COALESCE(ws.ws_ext_sales_price, 0) + 
    COALESCE(ss.ss_ext_sales_price, 0) as total_sales_amount,
    
    COALESCE(cs.cs_net_profit, 0) + 
    COALESCE(ws.ws_net_profit, 0) + 
    COALESCE(ss.ss_net_profit, 0) as total_net_profit,
    
    COALESCE(cr.cr_return_amount, 0) + 
    COALESCE(wr.wr_return_amt, 0) + 
    COALESCE(sr.sr_return_amt, 0) as total_return_amount,
    
    -- Channel counts
    (CASE WHEN cs.cs_item_sk IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN ws.ws_item_sk IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN ss.ss_item_sk IS NOT NULL THEN 1 ELSE 0 END) as active_channels_count

FROM customer c
LEFT JOIN catalog_sales cs ON (c.c_customer_sk = cs.cs_ship_customer_sk) AND (c.c_customer_sk = cs.cs_bill_customer_sk)
LEFT JOIN web_sales ws ON (c.c_customer_sk = ws.ws_ship_customer_sk) AND (c.c_customer_sk = ws.ws_bill_customer_sk)
INNER JOIN store_sales ss ON (c.c_customer_sk = ss.ss_customer_sk)
LEFT JOIN catalog_returns cr ON (c.c_customer_sk = cr.cr_returning_customer_sk) AND (c.c_customer_sk = cr.cr_refunded_customer_sk)
LEFT JOIN web_returns wr ON (wr.wr_returning_customer_sk = c.c_customer_sk) AND (wr.wr_refunded_customer_sk = c.c_customer_sk)
LEFT JOIN store_returns sr ON (c.c_customer_sk = sr.sr_customer_sk)
INNER JOIN household_demographics hd ON (c.c_current_hdemo_sk = hd.hd_demo_sk)
INNER JOIN customer_demographics cd ON (c.c_current_cdemo_sk = cd.cd_demo_sk)
INNER JOIN customer_address ca ON (c.c_current_addr_sk = ca.ca_address_sk)
)
SELECT * FROM cte;