--Insert into staging_users
Insert into staging_users (select * from users_extract);

--Insert into dim_users_type1
Merge into dim_users_type1 u using staging_users su on su.id = u.user_id
    when matched then update set u.postcode = su.postcode, u.insert_date = su.insert_date
    when not matched then insert (u.user_id, u.postcode, u.insert_date) values (su.id, su.postcode, su.insert_date);
    
select * from USER_POSTCODE_CHANGE_DATA_DIM

--Insert into dim_users_type2
merge into dim_users_type2 uh  
using user_postcode_change_data_dim upc 
   on  uh.user_id = upc.user_id 
   and uh.start_date = upc.start_date
when matched and upc.dml_type = 'U' then update 
    set uh.end_date = upc.end_date,
        uh.current_flag = 0
when matched and upc.dml_type = 'D' then update 
    set uh.end_date = upc.end_date,
        uh.current_flag = 0
when not matched and upc.dml_type = 'I' then insert 
           (dim_user_id, user_id, postcode, start_date, end_date, current_flag)
    values (dim_user_id_seq.nextval, upc.user_id, upc.postcode, upc.start_date, upc.end_date, upc.current_flag);

--Clear staging_users
Truncate table staging_users