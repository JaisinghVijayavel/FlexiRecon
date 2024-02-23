DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_recon_rule` $$
CREATE PROCEDURE `pr_get_recon_rule`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  select
    a.rule_gid,
    a.rule_code,
    a.rule_name,
    a.recon_code,
    b.recon_name,
    a.rule_order,
    a.rule_apply_on,
    a.source_dataset_code,
    a.comparison_dataset_code,
    d.dataset_name as source_dataset_desc,
    c.dataset_name as comparison_dataset_desc,
    fn_get_mastername(a.rule_apply_on, 'QCD_RS_RULE_APPLLIED') as rule_apply_on_desc,
    group_flag,
    fn_get_mastername(a.group_flag, 'QCD_RULE_GRP') as group_flag_desc,
	  a.active_status,
	  fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
  from recon_mst_trule a
  inner join recon_mst_tdataset d on a.source_dataset_code = d.dataset_code
		and d.delete_flag = 'N'
	inner join recon_mst_tdataset c on a.comparison_dataset_code = c.dataset_code
	  and c.delete_flag = 'N'
  inner join recon_mst_trecon b on a.recon_code = b.recon_code
    and b.delete_flag = 'N' 
  where 1 = 1
  and a.recon_code = in_recon_code
  and a.active_status !='N'
  and a.delete_flag = 'N'
  order by a.rule_order asc;
END $$

DELIMITER ;