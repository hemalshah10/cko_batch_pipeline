--1. Create extract tables

-- Create Users Extract table
CREATE OR REPLACE TABLE users_extract
(
 id          varchar NOT NULL,
 postcode    varchar NOT NULL,
 insert_date datetime NOT NULL
);

--Create Pageview Extract table
CREATE OR REPLACE TABLE pageview_extract
(
 user_id           varchar NOT NULL,
 url               varchar NOT NULL,
 pageview_datetime datetime NOT NULL,
 insert_date       datetime NOT NULL
);


--2. Create sequences 
create or replace sequence dim_user_id_seq;
create or replace sequence url_id_seq;
CREATE OR REPLACE SEQUENCE DATE_ID_SEQ;

--3. Create staging tables

-- Create Staging users table
CREATE OR REPLACE TABLE staging_users
(
 id          varchar NOT NULL,
 postcode    varchar NOT NULL,
 insert_date datetime NOT NULL
);

-- Create Staging Pageview table
CREATE OR REPLACE TABLE staging_pageview
(
 user_id           varchar NOT NULL,
 url               varchar NOT NULL,
 pageview_datetime datetime NOT NULL,
 insert_date       datetime NOT NULL
); 

--4. Create Dimension tables 

--create dim_user_type1
CREATE OR REPLACE TABLE dim_users_type1
(
 user_id     varchar NOT NULL,
 postcode    varchar NOT NULL,
 insert_date datetime NOT NULL,
 CONSTRAINT PK_dim_users_type1 PRIMARY KEY ( user_id )
);

--create dim_user_type2
CREATE OR REPLACE TABLE dim_users_type2
(
 dim_user_id  number(38,0) NOT NULL,
 user_id      varchar NOT NULL,
 postcode     varchar NOT NULL,
 start_date   datetime NOT NULL,
 end_date     datetime,
 current_flag int NOT NULL,
 CONSTRAINT PK_dim_users_type1_clone PRIMARY KEY ( dim_user_id )
);

-- Create URL dimension table 
CREATE TABLE dim_url
(
 url_id number(38,0) NOT NULL,
 url    varchar NOT NULL,
 CONSTRAINT PK_dim_page PRIMARY KEY ( url_id )
);


--create dim_date table
CREATE OR REPLACE TABLE dim_date (
  DATE_ID           NUMBER  NOT NULL primary key,
  DATE_TIME         DATETIME        NOT NULL
  ,YEAR             SMALLINT    NOT NULL
  ,MONTH            SMALLINT    NOT NULL
  ,MONTH_NAME       CHAR(3)     NOT NULL
  ,DAY_OF_MON       SMALLINT    NOT NULL
  ,DAY_OF_WEEK      VARCHAR(9)  NOT NULL
  ,WEEK_OF_YEAR     SMALLINT    NOT NULL
  ,DAY_OF_YEAR      SMALLINT    NOT NULL
  ,HOUR             SMALLINT    NOT NULL
)
AS
  WITH CTE_MY_DATE AS (
    SELECT DATEADD(HOUR, SEQ4(), '2020-01-01') AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>20000))  
  )
  SELECT date_ID_seq.nextval DATE_ID
        ,MY_DATE
        ,YEAR(MY_DATE)
        ,MONTH(MY_DATE)
        ,MONTHNAME(MY_DATE)
        ,DAY(MY_DATE)
        ,DAYOFWEEK(MY_DATE)
        ,WEEKOFYEAR(MY_DATE)
        ,DAYOFYEAR(MY_DATE)
        ,HOUR(MY_DATE)
    FROM CTE_MY_DATE
;



-- 5. Create fact tables 

-- Create Fact Pageview table
CREATE TABLE fact_pageview
(
 dim_user_id     number(38,0) NOT NULL,
 user_id         varchar NOT NULL,
 url_id          number(38,0) NOT NULL,
 date_id         number(38,0) NOT NULL,
 no_of_pageviews integer NOT NULL,
 CONSTRAINT PK_fact_pageview PRIMARY KEY ( dim_user_id, user_id, url_id, date_id ),
 CONSTRAINT fk_date_id FOREIGN KEY ( date_id ) REFERENCES dim_date ( date_id ),
 CONSTRAINT fk_dim_user_id FOREIGN KEY ( dim_user_id ) REFERENCES dim_users_type2 ( dim_user_id ),
 CONSTRAINT fk_url_id FOREIGN KEY ( url_id ) REFERENCES dim_url ( url_id ),
 CONSTRAINT fk_user_id FOREIGN KEY ( user_id ) REFERENCES dim_users_type1 ( user_id )
);

-- 6. Create stream
create or replace stream dim_user_postcode_changes on table dim_users_type1;

-- 7. Create view 
create or replace view user_postcode_change_data_dim as 
select user_id, postcode, start_date, end_date, current_flag, 'I' as dml_type
from (select user_id, postcode, insert_date as start_date,
      lag(insert_date) over (partition by user_id order by insert_date desc) as end_time_raw,
      case when end_time_raw is null then 
      null else end_time_raw end as end_date,
            case when end_date is null then 1 else 0 end as 
current_flag, 'I' as dml_type
      from (select user_id, postcode, insert_date from dim_user_postcode_changes
      where metadata$action = 'INSERT'
      and metadata$isupdate = 'FALSE'))
union 
select user_id, postcode, start_date, end_date, current_flag, dml_type
from (select user_id, postcode, insert_date as start_date,
       lag(insert_date) over (partition by user_id order by insert_date desc) as end_time_raw,
      case when end_time_raw is null then 
      null else end_time_raw end as end_date,
       case when end_date is null then 1 else 0 end as current_flag,
             dml_type
      from ( select user_id, postcode, insert_date, 'I' as dml_type
           from dim_user_postcode_changes 
           where metadata$action = 'INSERT'
            and metadata$isupdate = 'TRUE'
            union
           select user_id, null, start_date, 'U' as dml_type  
           from dim_users_type2
           where user_id in (select distinct user_id from dim_user_postcode_changes where metadata$action = 
'INSERT'
                                  and metadata$isupdate = 
'TRUE')
     and current_flag = 1))
union 
select upc.user_id, null, uh.start_date, current_timestamp(), null,'D' from dim_users_type2 uh
inner join dim_user_postcode_changes upc on uh.user_id = upc.user_id
where upc.metadata$action = 'DELETE'
and   upc.metadata$isupdate = 'FALSE'
and   uh.current_flag = 1;
