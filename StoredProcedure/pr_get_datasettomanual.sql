DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_datasettomanual` $$
CREATE PROCEDURE `pr_get_datasettomanual`
(
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
 )
BEGIN
  declare v_app_datetime_format text default '';

  set v_app_datetime_format = fn_get_configvalue('app_datetime_format');

  drop temporary table if exists recon_tmp_tscheduler;

  CREATE temporary TABLE recon_tmp_tscheduler
  (
    scheduler_gid int unsigned NOT NULL,
    recon_code varchar(32),
    PRIMARY KEY (scheduler_gid)
  ) ENGINE = MyISAM;

  insert into recon_tmp_tscheduler (scheduler_gid,recon_code)
    select scheduler_gid,max(recon_code) from recon_trn_tmanualtran
    where scheduler_gid > 0 and delete_flag = 'N'
    group by scheduler_gid;

  insert into recon_tmp_tscheduler (scheduler_gid,recon_code)
    select scheduler_gid,max(recon_code) from recon_trn_tmanualtranbrkp
    where scheduler_gid > 0 and delete_flag = 'N'
    group by scheduler_gid;

	select
		a.scheduler_gid,
    date_format(b.scheduled_date,v_app_datetime_format) as scheduled_date,
		date_format(b.scheduler_start_date,v_app_datetime_format) as scheduler_start_date,
    date_format(b.scheduler_end_date,v_app_datetime_format) as scheduler_end_date,
		b.scheduler_initiated_by,
		b.pipeline_code,c.pipeline_name,
		b.file_name,
		d.dataset_code,d.dataset_name,
    if(d.dataset_code = 'KOMANUAL','Knockoff','Posting') as dataset_type,
    s.recon_code,r.recon_name,
    date_format(j.start_date,v_app_datetime_format) as last_sync_date,
    fn_get_mastername(j.job_status,'QCD_JOB_STATUS') as last_sync_status
	from recon_tmp_tscheduler as s
  inner join recon_trn_tscheduler as a on s.scheduler_gid = a.scheduler_gid
	  and a.scheduler_status = 'C'
    and a.delete_flag = 'N'
	inner join con_trn_tscheduler as b on a.scheduler_gid = b.scheduler_gid
	inner join con_mst_tpipeline as c on b.pipeline_code = c.pipeline_code
	inner join recon_mst_tdataset as d on c.target_dataset_code = d.dataset_code
    and d.dataset_code in ('KOMANUAL','POSTMANUAL')
    and d.delete_flag = 'N'
  left join recon_mst_trecon as r on s.recon_code = r.recon_code and r.delete_flag = 'N'
  left join recon_trn_tjob as j on j.job_gid = d.last_job_gid and j.delete_flag = 'N'
  order by 1 asc;

  drop temporary table if exists recon_tmp_tscheduler;
END $$

DELIMITER ;