-- =====================================================================================
-- Gold aggregate: agg_daily_sales_by_segment
-- -------------------------------------------------------------------------------------
-- transformation_type: sql - the framework runs this single SELECT and writes the
-- result according to the control row's load_type (overwrite).
--
-- Available placeholders: ${gold_catalog}, ${silver_catalog}, ${target_schema},
-- ${target_table}, ${batch_id}, plus every key in the control row's parameters map.
-- An unresolved placeholder fails the task rather than reaching Spark as literal text.
-- =====================================================================================

SELECT
    f.order_date,
    f.customer_segment,
    f.country_code,
    f.currency_code,
    COUNT(*)                                                        AS order_count,
    COUNT(DISTINCT f.customer_id)                                   AS customer_count,
    SUM(f.net_order_amount)                                         AS net_sales_amount,
    AVG(f.net_order_amount)                                         AS avg_order_value,
    SUM(CASE WHEN f.order_status = 'CANCELLED' THEN 1 ELSE 0 END)    AS cancelled_order_count,
    -- Cancellation rate is reported here rather than left to the BI tool so every
    -- consumer computes it the same way.
    ROUND(
        100.0 * SUM(CASE WHEN f.order_status = 'CANCELLED' THEN 1 ELSE 0 END) / COUNT(*),
        2
    )                                                               AS cancellation_rate_pct,
    '${batch_id}'                                                   AS _batch_id,
    current_timestamp()                                             AS _loaded_at
FROM ${gold_catalog}.${target_schema}.fct_sales f
GROUP BY
    f.order_date,
    f.customer_segment,
    f.country_code,
    f.currency_code
