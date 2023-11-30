DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_rulename` $$
CREATE FUNCTION `fn_get_rulename`(in_rule_code varchar(32)) RETURNS text
begin
  declare v_rule_name text;

  select
    rule_name
  into
    v_rule_name
  from recon_mst_trule
  where rule_code = in_rule_code
  and delete_flag = 'N';

  set v_rule_name = ifnull(v_rule_name,'');

  return v_rule_name;
end $$

DELIMITER ;