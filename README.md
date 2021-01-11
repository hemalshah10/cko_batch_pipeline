# cko_batch_pipeline

## Introduction

For this project, the chosen database technology used was Snowflake. The transformation pipeline was also developed in Snowflake.

### Steps to run the pipeline

#### Users Pipeline 
1. The extract process lands the data in the users_extract table where we add the current date/time to the data.
2. The data is then moved in to the stagings_users table. 
```
Insert into staging_users (select * from users_extract)
```
3. The data is then moved to the dim_user_type1 table.
```
Merge into dim_users_type1 u using staging_users su on su.id = u.user_id
    when matched then update set u.postcode = su.postcode, u.insert_date = su.insert_date
    when not matched then insert (u.user_id, u.postcode, u.insert_date) values (su.id, su.postcode, su.insert_date)
```
If an existing user is added to the latest extact we update the record in the dim_user_type1 table with the new postcode and insert_date.
However, if a new user is added to the latest extract we create a new record with the users postcode and insert_date.

The stream that was created will capture the changes needed to load the dim_users_type2 table. 
You can view the data it captures in the USER_POSTCODE_CHANGE_DATA_DIM.
```
select * from USER_POSTCODE_CHANGE_DATA_DIM
```
4. We then run another merge statement to populate the dim_users_type2 table with any changes in the users data to create the history of the user.
```merge into dim_users_type2 uh
using user_postcode_change_data_dim upc 
   on  uh.user_id = upc.user_id 
   and uh.start_date = upc.start_date
when matched and upc.dml_type = 'U' then update 
        uh.end_date = upc.end_date,
        uh.current_flag = 0
when matched and upc.dml_type = 'D' then update 
        uh.end_date = upc.end_date,
        uh.current_flag = 0
when not matched and upc.dml_type = 'I' then insert 
           (dim_user_id, user_id, postcode, start_date, end_date, current_flag)
    values (dim_user_id_seq.nextval, upc.user_id, upc.postcode, upc.start_date, upc.end_date, upc.current_flag);
```
If an existing users postcode is changed, we amennd the users previous record with an end date of today and set the current_flag to 0. 
If a new user is added we create a new record in the table.

5. Truncate the staging table.
```
truncate table staging_users;
```

#### Pageview Pipeline
1. The extract process lands the data in the pageview_extract table where we add the current date/time to the data.
2. The data is then moved in to the staging_pageview table 
```
Insert into staging_pageview (select * from pageview_extract);
```
3. Load any new URL's to the dim_url table 
```
Insert into dim_url (url_id, url) 
(Select dim_user_id_seq.nextval, url from staging_pageview where url not in 
 (select  
 sp.url
 from staging_pageview sp
 inner join dim_url du on sp.url = du.url
 ))
 ```
#### Populating the Fact table 
Populate the fact pageview table with the following script 
```
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

```
#### Populate the view to be consumed by BI tool
```
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
```

### Scheduling the pipelines
The Transform pipeline can be scheduled using Snowflake Tasks. 

#### Users Pipeline
The users pipeline can be scheduled to run every day at (00:05). The pipeline would run steps 1 - 5. 

#### Pageview Pipeline
The users pipeline can be scheduled to run every hour at 5AM. The pipeline would run steps 1 - 3. 
