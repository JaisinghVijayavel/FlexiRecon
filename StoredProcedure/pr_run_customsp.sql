DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_customsp` $$
CREATE PROCEDURE `pr_run_customsp`
(
  in in_archival_code text,
  in in_recon_code text,
  in in_report_code text,
  in in_sp_name text,
  in in_condition text,
  in in_job_gid int,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
begin
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 24-04-2025

    Version : 1
  */

  declare v_reportparam_type text default '';
  declare v_reportparam_code text default '';
  declare v_reportparam_value text default '';
  declare v_sql text default '';

  -- return job info
  select in_job_gid as result, concat('Report initiated in job id ',cast(in_job_gid as nchar)) as msg;

  -- return resultset sheet name
  select
    resultset_name,resultset_order,sheet_name
  from recon_mst_treportresultset
  where report_code = in_report_code
  and active_status = 'Y'
  and delete_flag = 'N'
  order by resultset_order;

  -- include sp name
  set v_sql = concat('call ',in_sp_name,'( ');

	-- param block
	param_block:begin
		declare param_done int default 0;
		declare param_cursor cursor for
		select
      reportparam_type,
      reportparam_code,
      fn_get_reportfiltervalue1(in_archival_code,in_recon_code,in_condition,
                                reportparam_value,in_user_code) as reportparam_value
    from recon_mst_treportparam
    where report_code = in_report_code
    and active_status = 'Y'
    and delete_flag = 'N'
    order by reportparam_order;

		declare continue handler for not found set param_done=1;

		open param_cursor;

		param_loop: loop
			fetch param_cursor into v_reportparam_type,
                              v_reportparam_code,
                              v_reportparam_value;

			if param_done = 1 then leave param_loop; end if;

			if v_reportparam_type = 'in' then
        set v_sql = concat(v_sql,'"',v_reportparam_value,'",');
      else
        set v_sql = concat(v_sql,'@',v_reportparam_code,',');
      end if;
		end loop param_loop;
		close param_cursor;
	end param_block;

  set v_sql = substr(v_sql,1,length(v_sql)-1);
  set v_sql = concat(v_sql,')');

  -- run the stored procedure
  set @sql = v_sql;
  prepare sp_stmt from @sql;
  execute sp_stmt;
  deallocate prepare sp_stmt;

  set out_msg = @msg;
  set out_result = @result;
end $$

DELIMITER ;