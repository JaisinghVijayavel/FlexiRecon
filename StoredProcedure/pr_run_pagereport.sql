DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_pagereport` $$
CREATE PROCEDURE `pr_run_pagereport`(
  in in_recon_code varchar(32),
  in in_report_code varchar(32),
  in in_report_condition text,
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_rec_count int,
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_rptsession_gid int default 0;
  declare v_report_code text default '';
  declare v_report_desc text default '';
  declare v_sp_name text default '';
  declare v_table_name text default '';
  declare v_src_table_name text default '';
  declare v_sql text default '';
  declare v_recon_code_field text default '';
  declare v_report_default_condition text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  /*
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    set @text = concat(@text,' ',err_msg);

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    ROLLBACK;

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;
  */

  if exists(select report_desc from recon_mst_treport
     where report_code = in_report_code
     and delete_flag = 'N') then
    select
      report_code,
      report_desc,
      table_name,
      src_table_name,
      sp_name,
      recon_code_field,
      default_condition
    into
      v_report_code,
      v_report_desc,
      v_table_name,
      v_src_table_name,
      v_sp_name,
      v_recon_code_field,
      v_report_default_condition
    from recon_mst_treport
    where report_code = in_report_code
    and delete_flag = 'N';
  elseif exists(select report_desc from recon_mst_treport
     where report_code = in_report_code
     and delete_flag = 'N') then
    select
      report_code,
      report_desc,
      table_name,
      src_table_name,
      sp_name,
      recon_code_field,
      default_condition
    into
      v_report_code,
      v_report_desc,
      v_table_name,
      v_src_table_name,
      v_sp_name,
      v_recon_code_field,
      v_report_default_condition
    from recon_mst_treport
    where report_code = in_report_code
    and delete_flag = 'N';
  else
      set out_msg = 'Invalid report';
      set out_result = 0;

      leave me;
  end if;

  set v_report_code = ifnull(v_report_code,0);
  set v_report_desc = ifnull(v_report_desc,in_report_code);
  set v_table_name = ifnull(v_table_name,'');
  set v_src_table_name = ifnull(v_src_table_name,'');
  set v_sp_name = ifnull(v_sp_name,'');
  set v_recon_code_field = ifnull(v_recon_code_field,'recon_code');
  set v_report_default_condition = ifnull(v_report_default_condition,'');

  if v_table_name = '' then
    set out_msg = 'Invalid table name';
    set out_result = 0;

    leave me;
  end if;

  set in_report_condition = ifnull(in_report_condition,'');

  set in_report_condition = concat(' and ',v_recon_code_field,' = ',char(34),in_recon_code,char(34),' ', in_report_condition);

  set in_report_condition = concat(in_report_condition,' ',v_report_default_condition);

  -- create new report session
  insert into recon_trn_treportsession (report_code,ip_addr,insert_date,insert_by)
    select v_report_code,in_ip_addr,sysdate(),in_user_code;

  select last_insert_id() into v_rptsession_gid;

  if v_rptsession_gid > 0 then
    set v_sql = concat('delete from ',v_table_name,' where rptsession_gid in (');
    set v_sql = concat(v_sql,'select rptsession_gid from recon_trn_treportsession ');
    set v_sql = concat(v_sql,'where rptsession_gid < ',cast(v_rptsession_gid as nchar),') ');
    set v_sql = concat(v_sql,'and rptsession_gid > 0 ');
    set v_sql = concat(v_sql,'and user_code = ',char(39),in_user_code,char(39));

    call pr_run_sql(v_sql,@msg,@result);
  end if;

  call pr_run_sp(in_recon_code,v_sp_name,0,v_rptsession_gid,in_report_condition,in_user_code,@msg,@result);

  set v_sql = concat('select count(*) into @rec_count from ',v_table_name,' ');
  set v_sql = concat(v_sql,'where rptsession_gid = ',cast(v_rptsession_gid as nchar),' ');

  call pr_run_sql(v_sql,@msg,@result);

  set out_rec_count = @rec_count;
  set out_msg = concat(v_report_desc,' generation initiated in the report session id ',cast(v_rptsession_gid as nchar));
  set out_result = v_rptsession_gid;

  /*
  if in_resultset_flag = true then
    -- if v_rptsession_gid > 0 then
    --   set v_resultset_condition = concat(' and rptsession_gid = ',cast(v_rptsession_gid as unsigned),' ');
    -- end if;

    -- call pr_get_tablequery(v_table_name,v_resultset_condition,0,in_user_code,@msg,@result);

    call pr_get_pagenoreport(in_rptsession_gid,in_report_code,1,in_page_size,out_rec_count,@msg,@result);
  end if;
  */
end $$

DELIMITER ;