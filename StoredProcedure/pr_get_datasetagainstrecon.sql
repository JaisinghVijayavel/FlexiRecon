DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_datasetagainstrecon` $$
CREATE PROCEDURE `pr_get_datasetagainstrecon`
(
  in in_recon_code varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
	select 
		a.recondataset_gid,
		a.recon_code,
		a.dataset_code,
        b.dataset_category,
		b.dataset_name,
		a.dataset_type,
		-- d.start_date as last_sync_date,
        date_format(str_to_date(d.start_date,'%Y-%m-%d %H:%i:%s') ,fn_get_configvalue('app_datetime_format')) as last_sync_date,
		fn_get_mastername(a.dataset_type, 'QCD_DS_TYPE') as dataset_type_desc,
		fn_get_mastername(d.job_status, 'QCD_JOB_STATUS') as last_sync_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
		a.parent_dataset_code,
		ifnull(c.dataset_name,'') as parent_dataset_name,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_trecondataset a
	inner join recon_mst_tdataset b on a.dataset_code = b.dataset_code
	left join recon_mst_tdataset c on a.parent_dataset_code = c.dataset_code and a.dataset_type='S'
	left join recon_trn_tjob as d on b.last_job_gid = d.job_gid and d.delete_flag = 'N'
	where a.recon_code = in_recon_code
	and  a.active_status = 'Y' 
	and a.delete_flag = 'N';
END $$

DELIMITER ;