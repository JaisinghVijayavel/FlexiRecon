DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_undojobprocess` $$
CREATE PROCEDURE `pr_get_undojobprocess`
(
  in in_recon_code varchar(32),
  In in_job_status varchar(15),
  In in_job_type char (1),
  In in_From_date date,
  In in_To_date date
)
begin
  declare v_mysql_rpt_path text default '';

  set v_mysql_rpt_path = fn_get_configvalue('mysql_rpt_path');

	set @sql = concat("
	select
		a.job_gid,
		a.jobtype_code,
		b.jobtype_desc,
		a.job_ref_gid,
		a.job_name,
		a.job_status,
		c.jobstatus_desc,
		a.ip_addr,
		a.job_initiated_by,
		date_format(ifnull(a.start_date,''),'%d-%m-%Y %h:%i:%s') as start_date,
		date_format(ifnull(a.end_date,''),'%d-%m-%Y %h:%i:%s') as end_date,
		ifnull(a.ip_addr,'') as ip_addr,
		ifnull(a.job_remark,'')as job_remark,
		v_mysql_rpt_path as file_path
	from recon_trn_tjob a 
	left join recon_mst_tjobtype b on a.jobtype_code = b.jobtype_code
		and b.delete_flag = 'N'
	left join recon_mst_tjobstatus c on a.job_status = c.job_status 
		and c.delete_flag = 'N'
	where a.job_status in ('C','F')
	and a.jobtype_code in ('A','M') and a.delete_flag = 'N' and a.recon_code =  '",in_recon_code,"'
	and date_format(a.start_date,'%Y-%m-%d') >= date_format('",in_From_date,"','%Y-%m-%d')
	and date_format(a.start_date,'%Y-%m-%d') <= date_format('",in_To_date,"','%Y-%m-%d')
	order by a.job_gid desc ");

	call pr_run_sql(@sql,@msg,@result);

  /*
  IF in_job_status = 'All' then
	  select
		a.job_gid,
		a.jobtype_code,
		b.jobtype_desc,
		a.job_ref_gid,
		a.job_name,
		a.job_status,
		c.jobstatus_desc,
    a.ip_addr,
		a.job_initiated_by,
		date_format(ifnull(a.start_date,""),"%d-%m-%Y %h:%i:%s") as start_date,
		date_format(ifnull(a.end_date,""),"%d-%m-%Y %h:%i:%s") as end_date,
		ifnull(a.ip_addr,"") as ip_addr,
		ifnull(a.job_remark,"")as job_remark,
        (select config_value from admin_mst_tconfig where config_name = 'mysql_rpt_path') as file_path
	  from
		recon_trn_tjob a left join recon_mst_tjobtype b on a.jobtype_code = b.jobtype_code
		left join recon_mst_tjobstatus c on a.job_status = c.job_status and c.delete_flag = 'N'
	  where  a.delete_flag = 'N'
    and a.jobtype_code = in_job_type and recon_code=in_recon_code
    and date_format(a.start_date,"%Y-%m-%d") >= date_format(in_From_date,"%Y-%m-%d")
    and date_format(a.start_date,"%Y-%m-%d") <= date_format(in_To_date,"%Y-%m-%d")
    order by a.job_gid desc;

	ELSE
    set @sql = concat("
		select
		a.job_gid,
		a.jobtype_code,
		b.jobtype_desc,
		a.job_ref_gid,
		a.job_name,
		a.job_status,
		c.jobstatus_desc,
    a.ip_addr,
		a.job_initiated_by,
		date_format(ifnull(a.start_date,''),'%d-%m-%Y %h:%i:%s') as start_date,
		date_format(ifnull(a.end_date,''),'%d-%m-%Y %h:%i:%s') as end_date,
		ifnull(a.ip_addr,'') as ip_addr,
		ifnull(a.job_remark,'')as job_remark,
        (select config_value from admin_mst_tconfig where config_name = 'mysql_rpt_path') as file_path
	  from
		recon_trn_tjob a left join recon_mst_tjobtype b on a.jobtype_code = b.jobtype_code
		left join recon_mst_tjobstatus c on a.job_status = c.job_status and c.delete_flag = 'N'
	  where a.job_status in ('",in_job_status,"')
    and a.jobtype_code = '",in_job_type,"' and a.delete_flag = 'N' and a.recon_code =  '",in_recon_code,"'
    and date_format(a.start_date,'%Y-%m-%d') >= date_format('",in_From_date,"','%Y-%m-%d')
    and date_format(a.start_date,'%Y-%m-%d') <= date_format('",in_To_date,"','%Y-%m-%d')
    order by a.job_gid desc ");

    call pr_run_sql(@sql,@msg,@result);
	END IF;*/
end $$

DELIMITER ;