DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_rulerecorder` $$
CREATE FUNCTION `fn_get_rulerecorder`
(
  in_rule_code varchar(32),
  in_applied_on varchar(32),
  in_field_prefix varchar(32)
) RETURNS text
begin
  declare v_txt text;

  set in_field_prefix = ifnull(in_field_prefix,'');

  select
    group_concat(concat(in_field_prefix,recorder_field)) into v_txt
  from recon_mst_trulerecorder
  where rule_code = in_rule_code
  and recorder_applied_on = in_applied_on
  and active_status = 'Y'
  and delete_flag = 'N'
  order by recorder_seqno;

  return ifnull(v_txt,'');
end $$

DELIMITER ;