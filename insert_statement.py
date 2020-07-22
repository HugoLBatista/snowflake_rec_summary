#!/usr/bin/env python2.7

from dateutil.relativedelta import relativedelta

from ..config import config
from .. import daterange
from .. import query


JOB_NAME = "insert {}".format(__name__.rpartition(".")[2])


def compute(c, begin, end):
    """
    Given that new facts have been loaded from the interval [begin, end),
    recompute impacted session time ranges.
    @param c: sqlalchemy connection
    @param begin: inclusive begin time of updated facts
    @param end: exclusive end time of updated facts
    """

    # sessions cannot cross month boundaries.
    # Generate range for each month, from the earliest session change through the end of each month.
    for begin_session_start_time, end_session_start_time in daterange.generate_session_ranges(begin, end):

        begin_fact_time = begin_session_start_time
        end_fact_time = daterange.get_month_floor(begin_session_start_time) + relativedelta(months=1)
        assert end_fact_time == end_session_start_time

        safe_quoted = dict(
            comment="{job_name} [{begin:%Y-%m-%dT%H}, {end:%Y-%m-%dT%H}) {begin_fact_time:%Y-%m}".format(
                job_name=JOB_NAME, begin=begin, end=end, begin_fact_time=begin_fact_time),
            schema=c.dialect.identifier_preparer.quote(config("SNOWFLAKE_SCHEMA", default="PUBLIC")),
        )

        with c.begin():
            
            c.execute(query.text("""
                INSERT /* {comment} */
                INTO {schema}.m_session_recommendation_summary (
                    account_id,	
                    session_date,
                    new_customers,
                    page_views,
                    product_views,	
                    SESSION_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_HASRECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,		
                    SESSION_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_NOPURCHASE,
                    SESSION_NORECCLICKED_HASRECCONVERTED_NORECCLIKEDCONVERTED_HASPURCHASE,	
                    SESSION_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_NOPURCHASE,
                    SESSION_REVENUE_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKCONVERTED_HASPURCHASE float null,	
                    SESSION_REVENUE_HASRECCLICKED_HASRECCONVERTED_NORECCLICKCONVERTED_HASPURCHASE float null,	
                    SESSION_REVENUE_HASRECCLICKED_NORECCONVERTED_NORECCLICKCONVERTED_HASPURCHASE float null,
                    SESSION_REVENUE_NORECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE float null,
                    SESSION_REVENUE_NORECCLICKED_NORECCONVERTED_NORECCLICKCONVERTED_HASPURCHASE float null,
                    SESSION_TRANSACTION_COUNT_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_TRANSACTION_COUNT_HASRECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_TRANSACTION_COUNT_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_TRANSACTION_COUNT_NORECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_TRANSACTION_COUNT_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_ITEM_COUNT_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE,
                        SESSION_ITEM_COUNT_HASRECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_ITEM_COUNT_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,	
                    SESSION_ITEM_COUNT_NORECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
                    SESSION_ITEM_COUNT_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
                    ITEM_REVENUE_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE float null,	
                    ITEM_COUNT_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE 
                                )
                WITH session_boundary AS (
                SELECT
                    account_id
                   --  concat(mid_epoch, mid_ts, mid_rnd) as user_id
                    , mid_epoch
                    , mid_ts
                    , mid_rnd
                    , start_date
                    , start_time
                    , end_time
                    , page_views
                    , product_views
                    , has_new_customer
                    
                FROM m_session_summary 
                WHERE start_time >= '2020-07-15 00:00:00.000' and start_time < '2020-07-16 00:00:00.000' and HAS_STEALTH = 'FALSE'
               and account_id = 8708  
      
  
    ), 
 RECOMMENDED AS 
    (SELECT
                     sb.account_id,sb.start_time, sb.end_time, sb.mid_epoch,sb.mid_ts,sb.mid_rnd, f.product_id,
                        MIN(f.fact_time) AS fact_time
                    FROM fact_endcap_product_impression_2 f
                    JOIN session_boundary sb
                        ON f.account_id = sb.account_id
                        AND f.fact_time BETWEEN sb.start_time AND sb.end_time
                        AND f.mid_epoch = sb.mid_epoch
                        AND f.mid_ts = sb.mid_ts
                        AND f.mid_rnd = sb.mid_rnd
                        AND f.fact_time >= '2020-07-15 00:00:00.000' and f.fact_time < '2020-07-16 00:00:00.000'
                    WHERE sb.account_id = 8708                        
                    GROUP BY sb.account_id, sb.start_time, sb.end_time, sb.mid_epoch, sb.mid_ts, sb.mid_rnd, f.product_id
                    UNION
                    SELECT
                        sb.account_id, sb.start_time, sb.end_time, sb.mid_epoch,sb.mid_ts,sb.mid_rnd, f.product_id,
                        MIN(f.fact_time) AS fact_time
                    FROM fact_endcap_product_impression f 
                    JOIN session_boundary sb 
                        ON f.account_id = sb.account_id
                        AND f.fact_time BETWEEN sb.start_time AND sb.end_time  /* in session */
                        AND f.mid_epoch = sb.mid_epoch
                        AND f.mid_ts = sb.mid_ts
                        AND f.mid_rnd = sb.mid_rnd
                        AND f.fact_time >= '2020-07-15 00:00:00.000' and f.fact_time < '2020-07-16 00:00:00.000'
                   WHERE sb.account_id = 8708
                     GROUP BY sb.account_id, sb.start_time, sb.end_time, sb.mid_epoch, sb.mid_ts, sb.mid_rnd, f.product_id
    ),
    CLICKED AS 
    (SELECT
                        sb.account_id,sb.start_time, sb.end_time, sb.mid_epoch, sb.mid_ts, sb.mid_rnd, f.product_id,
                        MIN(f.fact_time) AS fact_time
                    FROM fact_endcap_product_click_2 f
                    JOIN session_boundary sb
                        ON f.account_id = sb.account_id
                        AND f.fact_time BETWEEN sb.start_time AND sb.end_time  /* in session after impression */
                        AND f.mid_epoch = sb.mid_epoch
                        AND f.mid_ts = sb.mid_ts
                        AND f.mid_rnd = sb.mid_rnd
                        --AND f.product_id = sb.product_id
                       AND f.fact_time >= '2020-07-15 00:00:00.000' and f.fact_time < '2020-07-16 00:00:00.000'
                   WHERE sb.account_id = 8708
                   GROUP BY sb.account_id, sb.start_time, sb.end_time, sb.mid_epoch, sb.mid_ts, sb.mid_rnd, f.product_id
                    UNION
                    SELECT
                        sb.account_id, sb.start_time, sb.end_time, sb.mid_epoch, sb.mid_ts, sb.mid_rnd, f.product_id,
                        MIN(f.fact_time) AS fact_time
                    FROM fact_endcap_product_click_2 f
                    JOIN session_boundary sb
                        ON f.account_id = sb.account_id
                        AND f.fact_time BETWEEN sb.start_time AND sb.end_time  /* in session after impression */
                        AND f.mid_epoch = sb.mid_epoch
                        AND f.mid_ts = sb.mid_ts
                        AND f.mid_rnd = sb.mid_rnd
                        -- AND f.product_id = sb.product_id
                     AND f.fact_time >= '2020-07-15 00:00:00.000' and f.fact_time < '2020-07-16 00:00:00.000'
                     WHERE sb.account_id = 8708
                    GROUP BY sb.account_id, sb.start_time, sb.end_time, sb.mid_epoch, sb.mid_ts, sb.mid_rnd, f.product_id
                ),           //clicked no purchased has no no has
    
  PURCHASED AS 
    (SELECT
                      sb.account_id, sb.start_time,sb.end_time, sb.mid_epoch, sb.mid_ts,sb.mid_rnd, f.product_id,purchase_id as purchase_id,quantity as quantity
                     ,1.0*f.currency_unit_price * ex.rate as price,   -- Defaults unit price to default currency of the account
                      MIN(f.fact_time) AS fact_time
                  FROM m_dedup_purchase_line f 
                  JOIN session_boundary sb
                      ON f.account_id = sb.account_id
                      AND f.fact_time BETWEEN sb.start_time AND sb.end_time  /* in session */
                      AND f.mid_epoch = sb.mid_epoch
                      AND f.mid_ts = sb.mid_ts
                      AND f.mid_rnd = sb.mid_rnd
                      AND f.fact_time >= '2020-07-15 00:00:00.000' and f.fact_time < '2020-07-16 00:00:00.000'
                  JOIN config_account a
                        ON a.account_id = sb.account_id
                  JOIN exchange_rate ex
                        ON ex.effective_date::date = f.fact_time::date
                        AND ex.from_currency_code = f.currency
                        AND ex.to_currency_code = a.currency
                 WHERE sb.account_id = 8708
                     -- NOT sb.has_stealth
                     -- sb.has_purchase = 1
                     --  sb.start_time > '2020-07-14 00:00:00.000' and f.fact_time < '2020-07-15 00:00:00.000'
                                            
                  GROUP BY sb.account_id, sb.start_time, sb.end_time, sb.mid_epoch, sb.mid_ts, sb.mid_rnd, f.product_id,purchase_id,
                      quantity, 1.0*f.currency_unit_price * ex.rate
    ),
  COLLECTED_RECOMMENDED AS (
    SELECT
     account_id,start_time,r.mid_epoch, r.mid_ts, r.mid_rnd ,ARRAY_AGG(OBJECT_CONSTRUCT('time', R.fact_time, 'item_id', R.product_id)) AS RECOMMENDED
    FROM 
      RECOMMENDED AS R
    GROUP BY account_id,start_time,r.mid_epoch, r.mid_ts, r.mid_rnd
  ),
  COLLECTED_CLICKS AS (
    SELECT
     account_id,start_time,c.mid_epoch, c.mid_ts, c.mid_rnd, ARRAY_AGG(OBJECT_CONSTRUCT('time', C.fact_time, 'item_id', C.product_id)) AS CLICKED
    FROM 
      CLICKED AS C
    GROUP BY account_id,start_time,c.mid_epoch, c.mid_ts, c.mid_rnd
  ),
  COLLECTED_PURCHASED AS (
    SELECT
     account_id,start_time,p.mid_epoch, p.mid_ts, p.mid_rnd,sum(quantity) as quantity,count (purchase_id) as purchase_id,sum(price*quantity) as total_revenue,ARRAY_AGG(OBJECT_CONSTRUCT('time', P.fact_time, 'item_id', P.product_id, 'price', P.price, 'qty', P.quantity, 'transaction_id', P.purchase_id)) AS PURCHASED
    FROM 
      PURCHASED AS P
    GROUP BY account_id,start_time,p.mid_epoch, p.mid_ts, p.mid_rnd,purchase_id
  ),un_agg_results AS (
    SELECT 
      sb.account_id as account_id, sb.start_date as date,page_views,product_views,has_new_customer
    ,quantity,purchase_id,total_revenue,developer.process_session(R.RECOMMENDED, C.CLICKED, P.PURCHASED) AS metrics
    FROM
    session_boundary sb
    LEFT JOIN    
      COLLECTED_RECOMMENDED AS R
      ON r.mid_epoch = sb.mid_epoch
      AND r.mid_ts = sb.mid_ts
      AND r.mid_rnd = sb.mid_rnd
      AND r.start_time = sb.start_time 
    LEFT JOIN COLLECTED_CLICKS AS C
       ON  c.mid_epoch = r.mid_epoch
       AND c.mid_ts = r.mid_ts
       AND c.mid_rnd = r.mid_rnd
       AND c.start_time = r.start_time 
    LEFT JOIN COLLECTED_PURCHASED AS P
       ON p.mid_epoch = c.mid_epoch
       AND p.mid_ts = c.mid_ts
       AND p.mid_rnd = c.mid_rnd
       AND p.start_time = c.start_time 
  )
SELECT account_id,date, 
  count_if (has_new_customer = 'True') as new_customers,
  sum(page_views) as page_views,sum(product_views) as product_views
  ,sum(quantity) as quantity ,count (purchase_id) as transactions, sum(total_revenue) as total_revenue,
  SUM(GET(METRICS, 'SESSION_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_HASRECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_HASRECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_NOPURCHASE')) AS SESSION_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_NOPURCHASE,
  SUM(GET(METRICS, 'SESSION_NORECCLICKED_HASRECCONVERTED_NORECCLIKEDCONVERTED_HASPURCHASE')) AS SESSION_NORECCLICKED_HASRECCONVERTED_NORECCLIKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_NOPURCHASE')) AS SESSION_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_NOPURCHASE,
  SUM(GET(METRICS, 'SESSION_REVENUE_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKCONVERTED_HASPURCHASE')) AS SESSION_REVENUE_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_REVENUE_HASRECCLICKED_HASRECCONVERTED_NORECCLICKCONVERTED_HASPURCHASE')) AS SESSION_REVENUE_HASRECCLICKED_HASRECCONVERTED_NORECCLICKCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_REVENUE_HASRECCLICKED_NORECCONVERTED_NORECCLICKCONVERTED_HASPURCHASE')) AS SESSION_REVENUE_HASRECCLICKED_NORECCONVERTED_NORECCLICKCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_REVENUE_NORECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_REVENUE_NORECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_REVENUE_NORECCLICKED_NORECCONVERTED_NORECCLICKCONVERTED_HASPURCHASE')) AS SESSION_REVENUE_NORECCLICKED_NORECCONVERTED_NORECCLICKCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_TRANSACTION_COUNT_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_TRANSACTION_COUNT_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_TRANSACTION_COUNT_HASRECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_TRANSACTION_COUNT_HASRECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_TRANSACTION_COUNT_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_TRANSACTION_COUNT_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_TRANSACTION_COUNT_NORECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_TRANSACTION_COUNT_NORECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_TRANSACTION_COUNT_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_TRANSACTION_COUNT_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_ITEM_COUNT_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_ITEM_COUNT_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_ITEM_COUNT_HASRECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_ITEM_COUNT_HASRECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_ITEM_COUNT_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_ITEM_COUNT_HASRECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_ITEM_COUNT_NORECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_ITEM_COUNT_NORECCLICKED_HASRECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'SESSION_ITEM_COUNT_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE')) AS SESSION_ITEM_COUNT_NORECCLICKED_NORECCONVERTED_NORECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'ITEM_REVENUE_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE')) AS ITEM_REVENUE_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE,
  SUM(GET(METRICS, 'ITEM_COUNT_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE')) AS ITEM_COUNT_HASRECCLICKED_HASRECCONVERTED_HASRECCLICKEDCONVERTED_HASPURCHASE
  
FROM
  un_agg_results
  GROUP BY account_id,date

							""".format(**safe_quoted)),
								  begin_fact_time=begin_fact_time,
								  end_fact_time=end_fact_time,
								  begin_session_start_time=begin_session_start_time,
								  end_session_start_time=end_session_start_time)
