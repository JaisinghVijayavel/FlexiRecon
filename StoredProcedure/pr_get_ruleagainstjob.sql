DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_ruleagainstjob`$$
CREATE PROCEDURE `pr_get_ruleagainstjob`
(
  in in_job_gid int,
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  select
    rule_gid,
    rule_code,
    rule_name,
    recon_code,
    rule_apply_on,
    fn_get_mastername(rule_apply_on, 'QCD_RS_RULE_APPLLIED') as rule_apply_on_desc,
    rule_order,
    group_flag,
    fn_get_mastername(group_flag, 'QCD_RULE_GRP') as group_flag_desc,
    active_status,
    fn_get_mastername(active_status, 'QCD_STATUS') as active_status_desc
  from recon_mst_trule
  where rule_code in
  (
    select distinct rule_code from recon_trn_tko
    where job_gid = in_job_gid
    union
    select distinct posted_rule_code as rule_code from recon_trn_ttranbrkp 
    where posted_job_gid = in_job_gid
  )
  and delete_flag = 'N';
END $$

DELIMITER ;