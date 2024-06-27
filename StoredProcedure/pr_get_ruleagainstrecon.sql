DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_ruleagainstrecon` $$
CREATE PROCEDURE `pr_get_ruleagainstrecon`
(
	in in_recon_code varchar(32)
)
BEGIN
  declare v_app_datetime_format text default '';

  set v_app_datetime_format = fn_get_configvalue('app_datetime_format');

  if v_app_datetime_format = '' then
    set v_app_datetime_format = '%d-%m-%Y %H:%i:%s';
  end if;

	select
		a.rule_gid,
		a.rule_code,
		a.rule_name,
		a.rule_order,
		a.rule_apply_on,
		fn_get_mastername(a.rule_apply_on, 'QCD_RS_RULE_APPLLIED') as rule_apply_on_desc,
		a.rule_order,
		a.group_flag,
		fn_get_mastername(a.group_flag, 'QCD_RULE_GRP') as  group_flag_desc,
		a.rule_order as ruleorder,
    a.recon_rule_version,
		a.source_dataset_code,
		a.comparison_dataset_code,
		a.parent_acc_mode,
		fn_get_mastername(a.parent_acc_mode, 'QCD_RS_ACC_MODE') as parent_acc_mode_desc,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
		b.dataset_name as source_dataset_desc,
		c.dataset_name as comparison_dataset_desc,
		b.dataset_name as dataset_name
	from recon_mst_trule a
	inner join recon_mst_tdataset b on a.source_dataset_code = b.dataset_code
		and b.delete_flag = 'N'
	inner join recon_mst_tdataset c on a.comparison_dataset_code = c.dataset_code
		and c.delete_flag = 'N'
	where a.recon_code = in_recon_code
  and a.active_status='Y'
	and a.delete_flag = 'N'
	ORDER BY a.rule_order;

	select
		a.recondataset_gid,
		a.recon_code,
		a.dataset_code,
		b.dataset_name,
		a.dataset_type,
    date_format(d.start_date,v_app_datetime_format) as last_sync_date,
		fn_get_mastername(d.job_status, 'QCD_JOB_STATUS') as last_sync_status,
		fn_get_mastername(a.dataset_type, 'QCD_DS_TYPE') as dataset_type_desc,
		a.parent_dataset_code,
		ifnull(c.dataset_name,'') as parent_dataset_name,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_trecondataset a
	inner join recon_mst_tdataset b on a.dataset_code = b.dataset_code
		and b.delete_flag = 'N'
	left join recon_mst_tdataset c on a.parent_dataset_code = c.dataset_code
		and c.delete_flag = 'N'
	left join recon_trn_tjob as d on d.job_gid = b.last_job_gid and d.delete_flag = 'N'
	where a.recon_code = in_recon_code
	and  a.active_status = 'Y'
	and a.delete_flag = 'N';
END $$

DELIMITER ;