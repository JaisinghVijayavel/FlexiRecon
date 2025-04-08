DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_scheduler` $$
CREATE PROCEDURE `pr_get_scheduler`
(
  in in_processed_date date,
  in in_scheduler_status varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  declare v_app_datetime_format text default '';

  set v_app_datetime_format = fn_get_configvalue('app_datetime_format');

	select
		a.scheduler_gid,
    date_format(b.scheduled_date,v_app_datetime_format) as scheduled_date,
		date_format(b.scheduler_start_date,v_app_datetime_format) as scheduler_start_date,
    date_format(b.scheduler_end_date,v_app_datetime_format) as scheduler_end_date,
		b.scheduler_initiated_by,
		b.pipeline_code,
    concat(b.pipeline_code,'-',c.pipeline_name) as pipeline_name,
		b.file_name,
		d.dataset_code,
    concat(d.dataset_code,'-',d.dataset_name) as dataset_name
	from recon_trn_tscheduler as a
	inner join con_trn_tscheduler as b on a.scheduler_gid = b.scheduler_gid
	inner join con_mst_tpipeline as c on b.pipeline_code = c.pipeline_code
	inner join recon_mst_tdataset as d on c.target_dataset_code = d.dataset_code
    and d.delete_flag = 'N'
	where b.scheduled_date >= in_processed_date
	and b.scheduled_date < date_add(in_processed_date,interval 1 day)
  and a.scheduler_status in ('C','F')
	and a.delete_flag = 'N';
END $$

DELIMITER ;