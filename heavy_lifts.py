import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# 2025-May-13: updated the daily diff SQL since call and sms were added.

load_dotenv()

# [1] Setup function to pull filtered EOM (End of Market) data
def eom_query(csid):
    return f"""
        with eom_plus as (
SELECT * FROM dq.fn_eom_plus({csid})
)
SELECT *
from eom_plus
WHERE (pct_change < -24)                    -- FILTER for throughput
UNION
SELECT *
from eom_plus
WHERE (metric IN ('m2m_block','m2m_drop')and delta > 2)             -- FILTER for M2M calls
UNION
SELECT *
from eom_plus
WHERE (rank NOT IN ('12v','13v','14v','16v','05d','08u','01m','02m') and delta < -2)      -- FILTER for acc/task 1 or 2?? / removed 15v
UNION
SELECT *
from eom_plus
WHERE (rank IN ('12v','13v','14v') and delta > 2)      -- FILTER for video
UNION
SELECT *
from eom_plus
WHERE (rank IN ('16v') and delta > 10)      -- video stall severity increased threshold
ORDER BY rank
        """
    

def get_rsr_conn():
    host_str = os.getenv("RSR_CONN")
    engine = create_engine(host_str)
    return engine

# execute query to pull eom
def get_eom(csid):
    df = pd.read_sql(eom_query(csid), con=get_rsr_conn())

    return df

# [2] Setup function to pull full EOM data
def eom_full_query(csid):
    return f"""SELECT * FROM dq.fn_eom_plus({csid})"""

# execute query to pull full eom table
def get_eom_full(csid):
    df = pd.read_sql(eom_full_query(csid), con=get_rsr_conn())
    return df



#Begin Call and network pull
def daily_callnet(csid):
    return f"""
        with cte as (
	SELECT
	csid
	,collection_set
	,collection_type_id
	,carrier
	,emp
	,kit_type_id
	,loc_day
	,case when m2m_bloc = 0 then '0' else ROUND((CAST (m2m_bloc AS NUMERIC)/CAST(total_m2m AS NUMERIC)*100),2) end AS block_rate
	,ROUND(((m2m_dr:: NUMERIC)/(total_m2m-m2m_tsk_nul))*100,2)  AS drop_rate
	,case when m2m_volte = 0 then '0' else ROUND((CAST (m2m_volte AS NUMERIC)/CAST(total_m2m AS NUMERIC)*100),2) end AS volte_rate
	,case when m2m_vonr = 0 then '0' else ROUND((CAST (m2m_vonr AS NUMERIC)/CAST(total_m2m AS NUMERIC)*100),2) end AS vonr_rate
	,case when nr_nsa = 0 then '0' else ROUND((CAST (nr_nsa AS NUMERIC)/CAST(data_test AS NUMERIC)*100),2) end AS nsa_rate
	,case when nr_sa = 0 then '0' else ROUND((CAST (nr_sa AS NUMERIC)/CAST(data_test AS NUMERIC)*100),2) end AS sa_rate
	,total_m2m
	,data_test
	FROM(
			SELECT
			ftsr.collection_set_id AS csid
			,ca.collection_set
	    	,ftsr.collection_type_id
			,c.name AS carrier
			,employee_letter as emp
			,kit_type_id
			,to_char(timezone(ca.time_zone::text, timezone('UTC'::text, ftsr.device_time)), 'mmdd') AS loc_day
			,SUM (CASE WHEN test_type_id = 23 THEN 1  ELSE 0 END) AS total_m2m
			,SUM (CASE WHEN test_type_id = 23 AND  flag_access_success = 'f'  THEN 1  ELSE 0 END) AS m2m_bloc
			,SUM (CASE WHEN test_type_id = 23 AND  flag_task_success = 'f' THEN 1 ELSE 0 END) AS m2m_dr
			,SUM (CASE WHEN test_type_id = 23 AND flag_access_success = 't' AND flag_task_success IS NULL THEN 1
					ELSE 0 END) AS m2m_tsk_nul
            ,SUM (CASE WHEN test_type_id = 23 AND call_network_type ='VoNR' AND flag_access_success IS TRUE THEN 1 ELSE 0 END) AS m2m_vonr
			,SUM (CASE WHEN test_type_id = 23 AND call_network_type ='VoLTE' AND flag_access_success IS TRUE THEN 1 ELSE 0 END) AS m2m_volte
			,SUM(CASE WHEN test_type_id IN (19,20,26) THEN 1 ELSE 0 END) as data_test
			,SUM(CASE WHEN test_type_id IN (19,20,26) AND best_network_type = 'NR NSA' THEN 1 ELSE 0 END) as nr_nsa
			,SUM(CASE WHEN test_type_id IN (19,20,26) AND best_network_type = 'NR SA' THEN 1 ELSE 0 END) as nr_sa
			FROM auto.fn_test_summary_reporting({csid}) ftsr
		LEFT JOIN md2.vi_collection_sets ca ON ftsr.collection_set_id = ca.collection_set_id
		LEFT JOIN md2.carriers c ON ftsr.carrier_id = c.carrier_id
		WHERE is_reportable IS TRUE
		GROUP BY 1,2,3,4,5,6,loc_day
		) a
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
		ORDER BY carrier, emp,loc_day,kit_type_id
),

cte2 as (
		SELECT distinct collection_set,csid,collection_type_id, carrier, emp,loc_day, block_rate,drop_rate,volte_rate,vonr_rate, nsa_rate, sa_rate, total_m2m, data_test from cte
	WHERE kit_type_id = 1
	ORDER BY 1,2,3,4,5,6
),

cte3 as (
		SELECT distinct collection_set,csid,collection_type_id, carrier, emp,loc_day, block_rate,drop_rate,volte_rate,vonr_rate, nsa_rate, sa_rate, total_m2m, data_test from cte
	WHERE kit_type_id = 2
	ORDER BY 1,2,3,4,5,6
)
SELECT distinct cte.collection_set,cte.csid,cte.collection_type_id,cte.carrier, cte.emp, cte.loc_day, cte2.block_rate AS k1_block 
,cte3.block_rate as k2_block
,ROUND(cte3.block_rate-cte2.block_rate,1)as block_delta
,cte2.drop_rate as k1_drop
 ,cte3.drop_rate as k2_drop
 ,ROUND(cte3.drop_rate-cte2.drop_rate,1)as drop_delta
,cte2.volte_rate as k1_volte
 ,cte3.volte_rate as k2_volte
 ,ROUND(cte3.volte_rate-cte2.volte_rate,1)as volte_delta
 ,cte2.vonr_rate as k1_vonr
 ,cte3.vonr_rate as k2_vonr
 ,ROUND(cte3.vonr_rate-cte2.vonr_rate,1)as vonr_delta
  ,cte2.nsa_rate as k1_nsa
 ,cte3.nsa_rate as k2_nsa
 ,ROUND(cte3.nsa_rate-cte2.nsa_rate,1)as nsa_delta
  ,cte2.sa_rate as k1_sa
 ,cte3.sa_rate as k2_sa
 ,ROUND(cte3.sa_rate-cte2.sa_rate,1)as sa_delta
 , cte2.total_m2m
 ,cte2.data_test
 FROM cte
LEFT JOIN cte2 using (csid,carrier,loc_day,emp) 
LEFT JOIN cte3 using (csid,carrier,loc_day,emp) 
ORDER BY cte.loc_day;
        """
def get_rsr_conn():
    host_str = os.getenv("RSR_CONN")
    engine = create_engine(host_str)
    return engine

# execute query to pull call net
def get_callnet(csid):
    df = pd.read_sql(daily_callnet(csid), con=get_rsr_conn())

    return df

#Begin Market Network comparison
def market_net(csid):
    return f"""
        SELECT * FROM dq.best_net_comp({csid})
        """


# execute query for market level network comparisons
def get_marketnet(csid):
    df = pd.read_sql(market_net(csid), con=get_rsr_conn())

    return df


#setup function for daily differences table
def daily_diff(csid):
    return f"""
 with diff_day as (
    SELECT ABS (acc_delta) AS acc_dif_ab, ABS(tsk_delta) AS tsk_dif_ab,* FROM dq.fn_dq_kit_diff({csid})
    --WHERE carrier <> 'Dish'      -- include Dish in the results for 2025-2H
    )	
SELECT *
    from diff_day
    WHERE (test_type_id = 27 AND tsk_dif_ab >10 AND n_grp > 19) OR (test_type_id = 27 AND acc_dif_ab > 10 AND n_grp > 19)
    UNION
    SELECT *
    from diff_day
    WHERE (test_type_id IN (19,20,26,23) AND tsk_dif_ab > 5 AND n_grp > 19) OR (test_type_id IN (19,20,26,23)  AND acc_dif_ab > 5 AND n_grp > 19)
	UNION
	SELECT *
    from diff_day
    WHERE (test_type_id =14 AND tsk_dif_ab > 10000 AND n_grp > 19) OR (test_type_id =14 AND acc_dif_ab > 25 AND n_grp > 19)
    """


# Pull Data from Postgres for daily differences
def get_datadiff(csid):
    df = pd.read_sql(daily_diff(csid), con=get_rsr_conn())

    return df


# BEGIN pull for dq check info 

def dq_check(csid):
    return f"SELECT * FROM analytic.fn_dq_check({csid});"
    #return f"SELECT * FROM analytic.fn_dq_check({csid});"



# execute query for dq check info
def get_dqcheck(csid):
    df = pd.read_sql(dq_check(csid), con=get_rsr_conn())

    return df

# Pull Sort of MAD data
def dq_sort_of_mad(csid):
    return f"SELECT * FROM dq.fn_sort_of_mad({csid}) UNION SELECT * FROM dq.fn_sort_of_mad((SELECT fn_get_previous_csid FROM analytic.fn_get_previous_csid({csid})));"


# Pull Sort of MAD data
#def dq_sort_of_mad(csid):
#	return f"SELECT * FROM dq.fn_sort_of_mad((SELECT fn_get_previous_csid FROM analytic.fn_get_previous_csid({csid})));"

def get_MADish(csid):
    df = pd.read_sql(dq_sort_of_mad(csid), con=get_rsr_conn())

    return df


# Setup function to pull data
#def dq_excluded(csid):
 #   return f"SELECT * FROM dq.fn_exclusion_review({csid});"

# Alternate function for excluded data
def dq_excluded(csid):
    return f"""
        SELECT
    'manual_blacklist_remark' AS exclusion_category,
    manual_blacklist_remark AS exclusion_detail,
    COUNT(*) AS row_count
FROM
     dq.fn_exclusion_review({csid})
WHERE
    manual_blacklist_remark IS NOT NULL
GROUP BY
    manual_blacklist_remark

UNION ALL

SELECT
    'auto_bl_reason' AS exclusion_category,
    auto_bl_reason AS exclusion_detail,
    COUNT(*) AS row_count
FROM
     dq.fn_exclusion_review({csid})
WHERE
    auto_bl_reason IS NOT NULL
GROUP BY
    auto_bl_reason
ORDER BY exclusion_category, row_count DESC;
    """


# execute Query to pull exclusion table from postgres
def get_excluded(csid):
    df = pd.read_sql(dq_excluded(csid), con=get_rsr_conn())

    return df


#setup function to pull agorithm data
def dq_dev_algo(csid):
    return f"SELECT * FROM dq.fn_dev_algo_fails({csid});"


#2. execute query to pull dev_algo data from postgres
def get_algo(csid):
    df = pd.read_sql(dq_dev_algo(csid), con=get_rsr_conn())

    return df


#setup function to pull layer3 review table
def dq_layer3_m2m(csid):
    return f"SELECT * FROM dq.fn_m2m_fail_layer3_py({csid});" # WHERE report_set <> 'Dish';"  -- 2025-2H: include Dish in the results

# 2. Execute query to pull layer3 review table from postgres
def get_layer3_m2m(csid):
    df = pd.read_sql(dq_layer3_m2m(csid), con=get_rsr_conn())

    return df

# setup function to pull blocklisting rate
def dq_bl_test(csid):
    return f"""
        SELECT
                collection_set_id AS csid
				,test_type_id
				,total_count - bl AS reportable
				, total_count AS valid_tests
				, bl AS blocklists
                , ROUND ((CAST (bl AS NUMERIC)/CAST(total_count AS NUMERIC)*100),2) AS bl_rate
FROM (
        SELECT
             collection_set_id
			 ,test_type_id
			 , count(*) as total_count
             ,SUM (CASE WHEN blacklisted = 't' THEN 1 ELSE 0 END) AS bl
        FROM prod_ms_partitions.test_summary_{csid} tsp1
        	WHERE period_name IS NOT NULL
			AND flag_valid IS TRUE
                GROUP BY collection_set_id,test_type_id
                ) a
		ORDER BY test_type_id;
    """

# execute query to pull blocklisting rate from postgres
def get_bl_test(csid):
    df = pd.read_sql(dq_bl_test(csid), con=get_rsr_conn())

    return df