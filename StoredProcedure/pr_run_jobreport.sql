DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_jobreport` $$
CREATE PROCEDURE `pr_run_jobreport`
(
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_sorting_order text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  set v_sql = concat("insert into recon_rpt_tjob
		(
			rptsession_gid,
			job_gid,
      user_code,
			root_job_gid,
      job_ref_gid,
      job_name,
			recon_code,
			recon_name,
			jobtype_code,
			jobtype_desc,
			job_input_param,
			job_initiated_by,
			start_date,
			end_date,
			ip_addr,
			file_type,
			job_status,
			job_status_desc,
			job_remark,
			file_name,
			update_date
		)
		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
      '",in_user_code,"' as user_code,
      a.job_gid,
      a.job_ref_gid,
      a.job_name,
			a.recon_code,
			c.recon_name,
			a.jobtype_code,
			b.jobtype_desc,
			a.job_input_param,
			a.job_initiated_by,
			a.start_date,
			a.end_date,
			a.ip_addr,
			a.file_type,
			a.job_status,
			d.jobstatus_desc,
			a.job_remark,
			a.file_name,
			a.update_date
		from recon_trn_tjob as a
		left join recon_mst_tjobtype as b on a.jobtype_code = b.jobtype_code and b.delete_flag = 'N'
		left join recon_mst_trecon as c on a.recon_code = c.recon_code and c.delete_flag = 'N'
		left join recon_mst_tjobstatus d on a.job_status = d.job_status and d.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition," ",in_sorting_order,"
  ");

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;