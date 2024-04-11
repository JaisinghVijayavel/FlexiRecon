DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_jobcompleted` $$
CREATE PROCEDURE `pr_get_jobcompleted`(
  in in_start_date date,
  in in_end_date date,
  in in_jobtype_code char(1),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
me:BEGIN
  declare v_app_datetime_format text default '';

  set v_app_datetime_format = fn_get_configvalue('app_datetime_format');

	if in_jobtype_code = '0' then
	select
		a.job_gid,
		a.jobtype_code,
		a.job_name,
    a.recon_code,
		d.recon_name,
		date_format(a.start_date,v_app_datetime_format) as start_date,
		date_format(a.end_date,v_app_datetime_format) as end_date,
		a.job_status,
		a.job_remark,
		b.jobstatus_desc,
		c.jobtype_desc,
    a.file_type
	from recon_trn_tjob a
	inner join recon_mst_tjobstatus b on a.job_status = b.job_status
	inner join recon_mst_tjobtype c on a.jobtype_code = c.jobtype_code
    left join recon_mst_trecon d on a.recon_code = d.recon_code
	where a.start_date >= in_start_date
	and a.end_date < adddate(in_end_date,interval 1 day)
	and a.delete_flag = 'N'
	and a.job_status IN ('C', 'F')
	order by a.job_gid desc;
else
	select
		a.job_gid,
		a.jobtype_code,
		a.job_name,
		a.recon_code,
		d.recon_name,
		date_format(a.start_date,v_app_datetime_format) as start_date,
		date_format(a.end_date,v_app_datetime_format) as end_date,
		a.job_status,
		a.job_remark,
		b.jobstatus_desc,
		c.jobtype_desc,
    a.file_type
	from recon_trn_tjob a
	inner join recon_mst_tjobstatus b on a.job_status = b.job_status
	inner join recon_mst_tjobtype c on a.jobtype_code = c.jobtype_code
    left join recon_mst_trecon d on a.recon_code = d.recon_code
	where a.start_date >= in_start_date
	and a.end_date < adddate(in_end_date,interval 1 day)
	and a.jobtype_code = in_jobtype_code
	and a.delete_flag = 'N'
	and a.job_status IN ('C', 'F')
	order by a.job_gid desc;
end if;
END $$

DELIMITER ;