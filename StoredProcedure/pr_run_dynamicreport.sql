DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_dynamicreport` $$
CREATE PROCEDURE `pr_run_dynamicreport`(
  in in_reporttemplate_code varchar(32),
  in in_recon_code varchar(32),
  in in_report_code varchar(32),
  in in_report_param text,
  in in_report_condition text,
  in in_outputfile_flag boolean,
  in in_outputfile_type varchar(32),
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_recon_code varchar(32);
  declare v_report_code varchar(32);
  declare v_sortby_code varchar(32);
  declare v_job_gid int default 0;
  declare v_report_exec_type char(1) default '';
  declare v_report_desc text default '';
  declare v_sp_name text default '';
  declare v_table_name text default '';
  declare v_recon_code_field text default '';
  declare v_recon_flag text default '';
  declare v_report_default_condition text default '';
  declare v_sorting_field text default '';
  declare v_dataset_db_name text default '';

  declare v_sql text default '';
  declare v_txt text default '';

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

  set in_reporttemplate_code = ifnull(in_reporttemplate_code,'');
  set in_outputfile_type = ifnull(in_outputfile_type,'');

  if in_reporttemplate_code <> '' then
    select
      recon_code,
      report_code,
      sortby_code
    into
      v_recon_code,
      v_report_code,
      v_sortby_code
    from recon_mst_treporttemplate
    where reporttemplate_code = in_reporttemplate_code
    and delete_flag = 'N';

    set v_recon_code = ifnull(v_recon_code,'');
    set v_report_code = ifnull(v_report_code,'');
    set v_sortby_code = lower(ifnull(v_sortby_code,'asc'));
  else
    set v_recon_code = ifnull(in_recon_code,'');
    set v_report_code = ifnull(in_report_code,'');
    set v_sortby_code = 'asc';
  end if;

  if v_sortby_code = 'asc' then
    set v_sortby_code = '';
  end if;

  -- file type xlsx/csv
  set in_outputfile_type = lower(in_outputfile_type);

  if exists(select report_desc from recon_mst_treport
     where report_code = v_report_code
     and delete_flag = 'N') then
    select
      report_desc,
      report_exec_type,
      sp_name,
      table_name,
      recon_code_field,
      default_condition,
      recon_flag
    into
      v_report_desc,
      v_report_exec_type,
      v_sp_name,
      v_table_name,
      v_recon_code_field,
      v_report_default_condition,
      v_recon_flag
    from recon_mst_treport
    where report_code = v_report_code
    and delete_flag = 'N';
  else
    set out_msg = 'Invalid report';
    set out_result = 0;

    leave me;
  end if;

  set v_report_desc = ifnull(v_report_desc,v_report_code);
  set v_report_exec_type = ifnull(v_report_exec_type,'');
  set v_sp_name = ifnull(v_sp_name,'');
  set v_table_name = ifnull(v_table_name,'');
  set v_recon_code_field = ifnull(v_recon_code_field,'recon_code');
  set v_report_default_condition = ifnull(v_report_default_condition,'');

  set in_report_condition = ifnull(in_report_condition,'');

  if v_recon_code <> '' and v_recon_flag = 'Y' then
    set in_report_condition = concat(' and ',v_recon_code_field,' = ',char(39),v_recon_code,char(39),' ', in_report_condition);
  end if;

  set in_report_condition = concat(in_report_condition,' ',v_report_default_condition);

  -- get report template name (if available)
  set in_reporttemplate_code = ifnull(in_reporttemplate_code,'');

  if exists(select * from recon_mst_treporttemplate
     where reporttemplate_code = in_reporttemplate_code
     and delete_flag = 'N') then

    select
      reporttemplate_name,
      sortby_code
    into
      v_txt,
      v_sortby_code
    from recon_mst_treporttemplate
    where reporttemplate_code = in_reporttemplate_code
    and delete_flag = 'N';

    set v_txt = ifnull(v_txt,'');
    set v_sortby_code = ifnull(v_sortby_code,'');

    if v_txt <> '' then
      set v_report_desc = v_txt;
    end if;

    if v_sortby_code = 'asc' then
      set v_sortby_code = '';
    end if;
  end if;

  if in_outputfile_type <> 'csv' and in_outputfile_type <> '' then
    set v_report_desc = concat(v_report_desc,' (',in_outputfile_type,')');
  end if;

  if v_table_name = '' and v_report_exec_type <> 'D' then
    set out_msg = 'Invalid table name';
    set out_result = 0;

    leave me;
  end if;

  -- sorting order
  if in_reporttemplate_code <> '' then
		-- sort order
		select
			group_concat(concat(ifnull(b.field_name,if(instr(a.report_field,'.') = 0,a.report_field,SPLIT(a.report_field,'.',2))),' ',v_sortby_code))
		into
			v_sorting_field
		from recon_mst_treporttemplatesorting as a
		left join recon_mst_tsystemfield as b on b.report_field_name = a.report_field
			and b.table_name = v_table_name
			and b.delete_flag = 'N'
		where a.reporttemplate_code = in_reporttemplate_code
		and a.active_status = 'Y'
		and a.delete_flag = 'N'
		order by a.sorting_order;
  else
		select
			group_concat(concat(ifnull(b.field_name,if(instr(a.report_field,'.') = 0,a.report_field,SPLIT(a.report_field,'.',2))),' ',v_sortby_code))
		into
			v_sorting_field
		from recon_mst_treportsorting as a
		left join recon_mst_tsystemfield as b on b.report_field_name = a.report_field
			and b.table_name = v_table_name
			and b.delete_flag = 'N'
		where a.report_code = v_report_code
		and a.active_status = 'Y'
		and a.delete_flag = 'N'
		order by a.sorting_order;
  end if;

  set v_sorting_field = ifnull(v_sorting_field,'');

  if v_sorting_field <> '' then
    set v_sorting_field = concat('order by ',v_sorting_field);
  end if;

  if in_outputfile_flag then
    call pr_ins_job(v_recon_code,'R',0,v_report_desc,in_report_param,in_user_code,in_ip_addr,'I','Initiated...',v_job_gid,@msg,@result);

    update recon_trn_tjob set
      file_type = in_outputfile_type 
    where job_gid = v_job_gid
    and delete_flag = 'N';
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

    call pr_run_sp(v_recon_code,v_sp_name,v_job_gid,0,in_report_condition,v_sorting_field,in_user_code,@msg,@result);

    -- call pr_ins_errorlog('vijay','localhost','sp','pr_run_dynamicreport',v_sorting_field,@msg,@result);

    call pr_run_tablequery(in_reporttemplate_code,
                           v_recon_code,
                           v_report_code,
                           v_table_name,
                           concat(' and job_gid = ', cast(v_job_gid as nchar) ,' ',v_sorting_field),
                           v_job_gid,
                           in_outputfile_flag,
                           in_outputfile_type,
                           in_user_code,@msg,@result);

    /*
    if v_job_gid = 0 then
      set v_sql = concat("delete from ",v_table_name," where job_gid = 0");

      call pr_run_sql(v_sql,@msg,@result);
    end if;
    */
  elseif v_report_exec_type = 'D' then    -- dataset
    set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

    if v_dataset_db_name <> '' then
      set v_table_name = concat(v_dataset_db_name,'.',in_report_code);
    else
      set v_table_name = in_report_code;
    end if;

    call pr_run_tablequery(in_reporttemplate_code,
                           v_recon_code,
                           v_report_code,
                           v_table_name,
                           in_report_condition,
                           v_job_gid,
                           in_outputfile_flag,
                           in_outputfile_type,
                           in_user_code,@msg,@result);
  elseif v_report_exec_type = 'C' then
    call pr_run_customsp(in_recon_code,in_report_code,v_sp_name,v_job_gid,in_user_code,@msg,@result);
  else
    call pr_run_tablequery(in_reporttemplate_code,
                           v_recon_code,
                           v_report_code,
                           v_table_name,
                           in_report_condition,
                           v_job_gid,
                           in_outputfile_flag,
                           in_outputfile_type,
                           in_user_code,@msg,@result);
  end if;

  set out_msg = concat(v_report_desc,' generation initiated in the job id ',cast(v_job_gid as nchar));
  set out_result = v_job_gid;

  select out_result as result, out_msg as msg;
end $$

DELIMITER ;