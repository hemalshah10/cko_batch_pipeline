--Insert into staging_preview
Insert into staging_pageview (select * from pageview_extract);

--Insert into dim_url
Insert into dim_url (url_id, url) 
(Select dim_user_id_seq.nextval, url from staging_pageview where url not in 
 (select  
 sp.url
 from staging_pageview sp
 inner join dim_url du on sp.url = du.url
 ))