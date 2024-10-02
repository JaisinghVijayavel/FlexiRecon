DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_report` $$
CREATE PROCEDURE `pr_run_report`(
  in in_recon_code varchar(32),
  in in_report_code varchar(32),
  in in_report_param text,
  in in_report_condition text,
  in in_ip_addr varchar(255),
  in in_outputfile_flag boolean,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_job_gid int default 0;
  declare v_report_exec_type char(1) default '';
  declare v_report_desc text default '';
  declare v_sp_name text default '';
  declare v_table_name text default '';
  declare v_recon_code_field text default '';
  declare v_recon_flag text default '';
  declare v_report_default_condition text default '';
  declare v_sort_order text default '';
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    set @text = concat(@text,' ',err_msg);

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    ROLLBACK;

    if in_outputfile_flag then
      call pr_upd_job(v_job_gid,'F',@full_error,@msg,@result);
    end if;

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;

  set in_recon_code = ifnull(in_recon_code,'');

  if exists(select report_desc from recon_mst_treport
     where report_code = in_report_code
     and delete_flag = 'N') then
    select
      report_desc,
      report_exec_type,
      sp_name,
      table_name,
      recon_code_field,
      default_condition,
      sort_order,
      recon_flag
    into
      v_report_desc,
      v_report_exec_type,
      v_sp_name,
      v_table_name,
      v_recon_code_field,
      v_report_default_condition,
      v_sort_order,
      v_recon_flag
    from recon_mst_treport
    where report_code = in_report_code
    and delete_flag = 'N';
  elseif exists(select report_desc from recon_mst_treport
     where report_gid = cast(in_report_code as unsigned)
     and delete_flag = 'N') then
    select
      report_desc,
      report_exec_type,
      sp_name,
      table_name,
      recon_code_field,
      default_condition,
      sort_order
    into
      v_report_desc,
      v_report_exec_type,
      v_sp_name,
      v_table_name,
      v_recon_code_field,
      v_report_default_condition,
      v_sort_order
    from recon_mst_treport
    where report_gid = cast(in_report_code as unsigned)
    and delete_flag = 'N';
  else
    set out_msg = 'Invalid report';
    set out_result = 0;

    leave me;
  end if;

  set v_report_desc = ifnull(v_report_desc,in_report_code);
  set v_report_exec_type = ifnull(v_report_exec_type,'');
  set v_sp_name = ifnull(v_sp_name,'');
  set v_table_name = ifnull(v_table_name,'');
  set v_recon_code_field = ifnull(v_recon_code_field,'recon_code');
  set v_report_default_condition = ifnull(v_report_default_condition,'');
  set v_sort_order = ifnull(v_sort_order,'');

  if v_sort_order <> '' then
    set v_sort_order = concat('order by ',v_sort_order);
  end if;

  set in_report_condition = ifnull(in_report_condition,'');

  if in_recon_code <> '' and v_recon_flag = 'Y' then
    set in_report_condition = concat(' and ',v_recon_code_field,' = ',char(39),in_recon_code,char(39),' ', in_report_condition);
  end if;

  set in_report_condition = concat(in_report_condition,' ',v_report_default_condition);

  if v_table_name = '' then
    set out_msg = 'Invalid table name';
    set out_result = 0;

    leave me;
  end if;

  if in_outputfile_flag then
    call pr_ins_job(v_recon_code,'R',0,v_report_desc,in_report_param,in_user_code,in_ip_addr,'I','Initiated...',v_job_gid,@msg,@result);
  end if;

  if v_report_exec_type = 'S' and v_sp_name <> '' then
    if v_job_gid = 0 then
      set v_sql = "delete from  recon_trn_tpreview where job_gid = 0 and rptsession_gid = 0";
      call pr_run_sql(v_sql,@msg,@result);

      set v_sql = "delete from  recon_trn_tpreviewdtl where job_gid = 0 and rptsession_gid = 0";
      call pr_run_sql(v_sql,@msg,@result);

      set v_sql = concat("delete from ",v_table_name," where job_gid = 0 and rptsession_gid = 0 ");

      call pr_run_sql(v_sql,@msg,@result);
    end if;

    call pr_run_sp(in_recon_code,v_sp_name,v_job_gid,0,in_report_condition,v_sort_order,in_user_code,@msg,@result);

    call pr_get_tablequery(in_recon_code,'',v_table_name,concat(' and job_gid = ', cast(v_job_gid as nchar) ,' '),v_job_gid,in_user_code,@msg,@result);


    /*
    if v_job_gid = 0 then
      set v_sql = concat("delete from ",v_table_name," where job_gid = 0");

      call pr_run_sql(v_sql,@msg,@result);
    end if;
    */
  elseif v_report_exec_type = 'C' then
    call pr_run_customsp(in_recon_code,in_report_code,v_sp_name,v_job_gid,in_user_code,@msg,@result);
  else
    call pr_get_tablequery(in_recon_code,'',v_table_name,in_report_condition,v_job_gid,in_user_code,@msg,@result);
  end if;

  set out_msg = concat(v_report_desc,' generation initiated in the job id ',cast(v_job_gid as nchar));
  set out_result = v_job_gid;
end $$

DELIMITER ;