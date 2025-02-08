DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_dsinvalidrecord` $$
CREATE PROCEDURE `pr_get_dsinvalidrecord`
(
  in in_scheduler_gid int,
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_sql_field text default '';
  declare v_pipeline_code text default '';
  declare v_sql text default '';
  declare v_file_name varchar(128) default '';
  declare v_rpt_path text default '';
  declare v_job_gid int default 0;
  declare v_iis_date_format text default '';

  declare v_field_txt text default '';
  declare v_field_formatted_txt text default '';
  declare v_header text default '';
  declare v_header_txt text default '';

  -- get iis date format
  set v_iis_date_format = fn_get_configvalue('iis_date_format');

  -- get pipeline
  select
    pipeline_code into v_pipeline_code
  from con_trn_tscheduler
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  set v_pipeline_code = ifnull(v_pipeline_code,'');

  -- get job_gid
  select
    job_gid into v_job_gid
  from recon_trn_tjob
  where jobtype_code = 'S'
  and job_ref_gid = in_scheduler_gid
  and delete_flag = 'N';

  set v_job_gid = ifnull(v_job_gid,0);

  if v_job_gid = 0 then
    leave me;
  end if;

  -- set header
  set v_header = concat("'Scheduler Id',
    'Status',
    'Validation Remarks',
    'Pipeline Code',
    'File Name'");

  -- set field
  set v_sql_field= '';
  set v_sql_field= concat(v_sql_field,'a.scheduler_gid as ',char(39),'Scheduler Id',char(39),',');
  set v_sql_field= concat(v_sql_field,'b.status_flag as ',char(39),'Status',char(39),',');
  set v_sql_field= concat(v_sql_field,'b.validation_remarks as ',char(39),'Validation Remarks',char(39),',');
  set v_sql_field= concat(v_sql_field,'a.pipeline_code as ',char(39),'Pipeline Code',char(39),',');
  set v_sql_field= concat(v_sql_field,'a.file_name as ',char(39),'File Name',char(39));

  -- source dataset field
	select
		group_concat(
      replace(cast_dataset_table_field,dataset_table_field,concat('b.',dataset_table_field))
      ,' as ',char(39),sourcefield_name,char(39)),
		group_concat(
      concat('b.',dataset_table_field)
      ,' as ',char(39),sourcefield_name,char(39)),
    group_concat(char(39),sourcefield_name,char(39))
  into
    v_field_formatted_txt,
    v_field_txt,
    v_header_txt
	from con_trn_tpplsourcefield
	where pipeline_code = v_pipeline_code
	and sourcefieldmapping_flag = 'Y'
	and delete_flag = 'N'
	order by dataset_table_field_sno;

  set v_field_formatted_txt = ifnull(v_field_formatted_txt,'');
  set v_field_txt = ifnull(v_field_txt,'');
  set v_header_txt = ifnull(v_header_txt,'');

  if v_iis_date_format <> '' then
    set v_field_txt = v_field_formatted_txt;

    set v_field_txt = replace(v_field_txt,'#DATETIME_FORMAT#',v_iis_date_format);
    set v_field_txt = replace(v_field_txt,'#DATE_FORMAT#',v_iis_date_format);
  end if;

  if v_field_txt <> '' then
    set v_sql_field= concat(v_sql_field,',',v_field_txt);
  end if;

  if v_header_txt <> '' then
    set v_header = concat(v_header,',',v_header_txt);
  end if;

  set v_sql = '';

  -- add header
  set v_sql = concat(v_sql,'select ',v_header,' ');

  set v_sql = concat(v_sql,'union all ');

  -- add actual query
  set v_sql = concat(v_sql,'select ',v_sql_field,' from con_trn_tscheduler as a ');
  set v_sql = concat(v_sql,'inner join con_trn_tbcp as b on a.scheduler_gid = b.scheduler_gid ');
  set v_sql = concat(v_sql,'and b.delete_flag = ',char(39),'N',char(39),' ');
  set v_sql = concat(v_sql,'where a.scheduler_gid = ',cast(in_scheduler_gid as nchar),' ');
  set v_sql = concat(v_sql,'and b.status_flag <> ',char(39),'V',char(39),' ');
  set v_sql = concat(v_sql,'and a.delete_flag = ',char(39),'N',char(39));

	set v_rpt_path = fn_get_configvalue('mysql_rpt_path');
	set v_file_name = concat(cast(v_job_gid as nchar),"_scheduler.csv");

	set @outfile_qry = concat(" INTO outfile '",v_rpt_path,v_file_name,"'
					FIELDS TERMINATED BY ','
					OPTIONALLY ENCLOSED BY '""'
					LINES TERMINATED BY '\n' ;");

  set v_sql = concat(v_sql,@outfile_qry);

  call pr_run_sql1(v_sql,@msg,@result);
end $$

DELIMITER ;