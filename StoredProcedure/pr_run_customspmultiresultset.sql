DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_customspmultiresultset` $$
CREATE PROCEDURE `pr_run_customspmultiresultset`
(
  in in_archival_code varchar(32),
  in in_reporttemplate_code varchar(32),
  in in_recon_code text,
  in in_report_code text,
  in in_report_param text,
  in in_sp_name text,
  in in_condition longtext, 
  in in_job_gid int,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_reportparam_type text default '';
  declare v_reportparam_code text default '';
  declare v_reportparam_value text default '';
  declare v_sql text default '';

  select in_job_gid as result, concat('Report initiated in job id ',cast(in_job_gid as nchar)) as msg;

  select
    resultset_name,resultset_order,sheet_name
  from recon_mst_treporttemplateresultset
  where reporttemplate_code = in_reporttemplate_code
  and active_status = 'Y'
  and delete_flag = 'N'
  order by resultset_order;

 
  set v_sql = concat('call pr_run_multiresultset(',char(39),in_recon_code,char(39),',',char(39),in_report_code,char(39),',',char(39),in_reporttemplate_code,char(39)
                ,',',char(39),replace(in_condition,char(39),char(12)),char(39),',',char(39),in_report_param,char(39),',',in_job_gid,',',char(39),in_archival_code,char(39),',',char(39),in_user_code,char(39),',','@msg',',','@result',')');

	set @sql = v_sql;
	prepare sp_stmt from @sql;
	execute sp_stmt;
	deallocate prepare sp_stmt;
  
  set out_msg = @msg;
  set out_result = @result;
end $$

DELIMITER ;