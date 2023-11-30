DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_transferlist` $$
CREATE PROCEDURE `pr_get_transferlist`
(
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
	select
		a.scheduler_gid,b.scheduled_date,
		b.scheduler_start_date,b.scheduler_end_date,
		b.scheduler_initiated_by,
		b.pipeline_code,c.pipeline_name,
		b.file_name,
		d.dataset_code,d.dataset_name
	from recon_trn_tscheduler as a
	inner join con_trn_tscheduler as b on a.scheduler_gid = b.scheduler_gid
	inner join con_mst_tpipeline as c on b.pipeline_code = c.pipeline_code
	inner join recon_mst_tdataset as d on c.target_dataset_code = d.dataset_code
    and d.dataset_code not in ('KOMANUAL','POSTMANUAL')
    and d.delete_flag = 'N'
	where a.scheduler_status = 'S'
	and a.delete_flag = 'N';
END $$

DELIMITER ;