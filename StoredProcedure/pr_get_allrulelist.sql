DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_allrulelist` $$
CREATE PROCEDURE `pr_get_allrulelist`
(
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
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
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_trule a
	where a.delete_flag = 'N'
    -- and a.active_status='Y'
	ORDER BY a.rule_order;
END $$

DELIMITER ;