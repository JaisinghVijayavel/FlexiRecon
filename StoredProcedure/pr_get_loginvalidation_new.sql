DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_loginvalidation_new` $$
CREATE PROCEDURE `pr_get_loginvalidation_new`
(
  in in_user_code varchar(32),
  in in_password text,
  in in_ip_addr varchar(255)
)
me:BEGIN
  declare done int default 0;
  declare err_msg text default '';
  declare err_flag boolean default false;
  declare v_user_gid int default 0;
  declare v_usergroup_code varchar(128) default '';
  declare v_user_code varchar(32) default '';
  declare v_usergroup_desc varchar(128) default '';
  declare v_user_name varchar(128) default '';
  declare v_user_password text default '';
  declare v_user_status char(1) default '';
  declare v_password_attempt int default 0;
  declare v_max_password_attempt int default 0;
  declare v_last_login_date datetime;
  declare v_password_expiry_date date;
  declare v_password_expiry_days int default 0;
  declare v_config_value varchar(255) default '';
  declare n int default 0;
  declare c int default 0;
  declare v_out_msg text default '';
  declare v_out_result int default 0;
  declare v_min_tran_date date default null;
  declare v_fin_start_date date default null;

  select 
		config_value into v_config_value 
	from admin_mst_tconfig
  where config_name = 'password_expiry_days'
  and delete_flag = 'N';

  set v_config_value = ifnull(v_config_value,'');

  if v_config_value <> '' then
    set v_password_expiry_days = cast(v_config_value as signed);
  else
    set v_password_expiry_days = 30;
  end if;

  select
		config_value into v_config_value
	from admin_mst_tconfig
	where config_name = 'password_attempt_count'
	and delete_flag = 'N';

  set v_config_value = ifnull(v_config_value,'');

  if v_config_value <> '' then
    set v_max_password_attempt = cast(v_config_value as signed);
  else
    set v_max_password_attempt = 5;
  end if;

	set v_usergroup_desc = (select distinct b.role_name from admin_mst_tuser a
											    left join admin_mst_trole b on a.role_code=b.role_code
														and b.active_status='Y'
														and b.delete_flag = 'N'
													where a.user_code = in_user_code
													and a.delete_flag = 'N' );

  select
    user_gid,
    role_code as usergroup_gid,
    user_code,
    user_name,
    user_password,
    user_status,
    password_attempt,
    last_login_date,
    password_expiry_date
  into
    v_user_gid,
    v_usergroup_code,
    v_user_code,
    v_user_name,
    v_user_password,
    v_user_status,
    v_password_attempt,
    v_last_login_date,
    v_password_expiry_date
  from admin_mst_tuser
  where user_code = in_user_code
  and delete_flag = 'N';

  set v_user_gid = ifnull(v_user_gid,0);
  set v_usergroup_code = ifnull(v_usergroup_code,0);
  set v_usergroup_desc= ifnull(v_usergroup_desc,0);
  set v_user_code = ifnull(v_user_code,'');
  set v_user_name = ifnull(v_user_name,'');
  set v_user_password = ifnull(v_user_password,'');
  set v_user_status = ifnull(v_user_status,'');
  set v_password_attempt = ifnull(v_password_attempt,0);
  set v_last_login_date = ifnull(v_last_login_date,curdate());
  set v_password_expiry_date = ifnull(v_password_expiry_date,adddate(curdate(),v_password_expiry_days));

  if v_user_gid = 0 then
    set v_out_result = 0;
    set v_out_msg = 'Invalid user !';

    insert into admin_trn_tloginattempt
    (
      user_code,
      attempt_date,
      attempt_password,
      ip_addr
    )
    values
    (
      in_user_code,
      sysdate(),
      in_password,
      in_ip_addr
    );

    select v_user_gid as user_gid,
        v_user_name as user_name,
        v_password_expiry_date as password_expiry_date,
        v_usergroup_code as usergroup_code,
        v_usergroup_desc as usergroup_desc,
        v_out_result as out_result,
        v_last_login_date as lastlogin,
        v_out_msg as out_msg,
        v_user_status as user_status;

    leave me;
  end if;

  
  if v_user_password <> in_password then
    set v_out_result = 0;
    set v_out_msg = 'Invalid password !';
 
    set v_password_attempt = v_password_attempt + 1;

    if v_password_attempt >= v_max_password_attempt then
      update admin_mst_tuser set 
        user_status = 'L',
        password_attempt = password_attempt + 1
      where user_gid = v_user_gid
      and delete_flag = 'N';

      set v_out_msg = 'Your id was deactivated ! Please contact system administrator !';
    else
      update admin_mst_tuser set
        password_attempt = password_attempt + 1
      where user_gid = v_user_gid
      and delete_flag = 'N';
    end if;

    insert into admin_trn_tloginattempt
    (
      user_code,
      attempt_date,
      attempt_password,
      ip_addr
    )
    values
    (
      in_user_code,
      sysdate(),
      in_password,
      in_ip_addr
    );

    select v_user_gid as user_gid,
        v_user_name as user_name,
        v_password_expiry_date as password_expiry_date,
        v_usergroup_code as usergroup_code,
        v_usergroup_desc as usergroup_desc,
        v_out_result as out_result,
        v_last_login_date as lastlogin,
        v_out_msg as out_msg,
        v_user_status as user_status;

    leave me;
  end if;

  if v_user_status = 'L' then
    set v_out_result = 0;
    set v_out_msg = 'Your id was deactivated ! Please contact system administrator !';

    select v_user_gid as user_gid,
        v_user_name as user_name,
        v_password_expiry_date as password_expiry_date,
        v_usergroup_code as usergroup_code,
        v_usergroup_desc as usergroup_desc,
        v_out_result as out_result,
        v_last_login_date as lastlogin,
        v_out_msg as out_msg,
        v_user_status as user_status;

    leave me;
  elseif v_user_status = 'S' then
    set v_out_result = 0;
    set v_out_msg = 'Your id was suspended !';

    select v_user_gid as user_gid,
        v_user_name as user_name,
        v_password_expiry_date as password_expiry_date,
        v_usergroup_code as usergroup_code,
        v_usergroup_desc as usergroup_desc,
        v_out_result as out_result,
        v_last_login_date as lastlogin,
        v_out_msg as out_msg,
        v_user_status as user_status;

    leave me;
  elseif v_user_status <> 'A' and v_user_status <> 'N' then
    set v_out_result = 0;
    set v_out_msg = concat('Your id status : ',v_user_status);

    select v_user_gid as user_gid,
        v_user_name as user_name,
        v_password_expiry_date as password_expiry_date,
        v_usergroup_code as usergroup_code,
        v_usergroup_desc as usergroup_desc,
        v_out_result as out_result,
        v_last_login_date as lastlogin,
        v_out_msg as out_msg,
        v_user_status as user_status;

    leave me;
  end if;

  if datediff(curdate(),v_last_login_date) > v_password_expiry_days then
    update admin_mst_tuser set
      user_status = 'L'
    where user_gid = v_user_gid
    and delete_flag = 'N';

    set v_out_result = 0;
    set v_out_msg = 'Your id was deactivated ! Please contact system administrator !';

    select v_user_gid as user_gid,
        v_user_name as user_name,
        v_password_expiry_date as password_expiry_date,
        v_usergroup_code as usergroup_code,
        v_usergroup_desc as usergroup_desc,
        v_out_result as out_result,
        v_last_login_date as lastlogin,
        v_out_msg as out_msg,
        v_user_status as user_status;

    leave me;
  end if;

  insert into admin_trn_tloginhistory
  (
    user_code,
    login_date,
    ip_addr
  )
  values
  (
    in_user_code,
    sysdate(),
    in_ip_addr
  );

  update admin_mst_tuser set
    last_login_date = sysdate(),
    password_attempt = 0
  where user_gid = v_user_gid
  and delete_flag = 'N';

  set v_out_msg = 'Login success !';
  set v_out_result = 1;

  set v_last_login_date = (select last_login_date from admin_mst_tuser
    where user_gid = v_user_gid
    and delete_flag = 'N');


  select min(tran_date) into v_min_tran_date from recon_trn_ttran;


  select date(concat(cast(year(curdate()) as nchar),'-04-01')) into v_fin_start_date;

  if curdate() < v_fin_start_date then
    select date(concat(cast((year(curdate())-1) as nchar),'-04-01')) into v_fin_start_date;
  end if;

  select v_user_gid as user_gid,
    v_user_name as user_name,
    CAST(v_password_expiry_date AS CHAR(50)) as password_expiry_date,
    v_usergroup_code as usergroup_code,
    v_usergroup_desc as usergroup_desc,
    v_out_result as out_result,
    v_out_msg as out_msg,
    v_user_status as user_status,
    v_last_login_date as lastlogin,
    CAST(date_format(ifnull(v_min_tran_date,curdate()),'%d-%m-%Y')AS CHAR(50)) as min_tran_date,
    CAST(date_format(v_fin_start_date,'%d-%m-%Y')AS CHAR(50)) as fin_start_date;
END $$

DELIMITER ;