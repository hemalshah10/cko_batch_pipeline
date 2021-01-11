--Populate fact table 
 Insert into fact_pageview (dim_user_id, user_id, url_id, date_id, no_of_pageviews)
    (Select dut2.dim_user_id,
     dut1.user_id,
     du.url_id,
     dd.date_id,
     count(dut1.user_id) as no_of_pageviews
     from staging_pageview sp
    inner join dim_users_type2 dut2 on sp.user_id = dut2.user_id  
    inner join dim_users_type1 dut1 on sp.user_id = dut1.user_id
    inner join dim_url du on sp.url = du.url
    inner join dim_date dd on TO_DATE(sp.pageview_datetime) = TO_DATE(dd.date_time)
    where dut2.current_flag = '1'
    GROUP BY dut2.dim_user_id, dut1.user_id, du.url_id, dd.date_id
    )
 
--View for consumption from BI tool 
Create or replace view user_postcode_pageview as (
Select a.dim_user_id,
     a.user_id,
     a.url_id,
     a.date_id,
     no_of_pageviews,
     duto.postcode as current_postcode,
     dutt.postcode as postcode_history
     from
    (Select dut2.dim_user_id,
     dut1.user_id,
     du.url_id,
     dd.date_id,
     count(dut1.user_id) as no_of_pageviews
     from staging_pageview sp
    inner join dim_users_type2 dut2 on sp.user_id = dut2.user_id  
    inner join dim_users_type1 dut1 on sp.user_id = dut1.user_id
    inner join dim_url du on sp.url = du.url
    inner join dim_date dd on TO_DATE(sp.pageview_datetime) = TO_DATE(dd.date_time)
    where dut2.current_flag = '1'
    GROUP BY dut2.dim_user_id, dut1.user_id, du.url_id, dd.date_id
    )  a
    inner join dim_users_type2 dutt on a.user_id = dutt.user_id  
    inner join dim_users_type1 duto on a.user_id = duto.user_id
)

