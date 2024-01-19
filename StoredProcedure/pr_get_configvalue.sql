DELIMITER $$

DROP procedure IF EXISTS `pr_get_configvalue` $$
CREATE procedure `pr_get_configvalue`
(
  in in_config_name text,
  out out_config_value text,
  out out_msg text,
  out out_result int(10)
)
begin
  declare v_config_value text default '';

  if exists(select config_value from admin_mst_tconfig
    where config_name = in_config_name
    and delete_flag = 'N') then
    select
      config_value
    into
      v_config_value
    from admin_mst_tconfig
    where config_name = in_config_name
    and delete_flag = 'N';

    set out_config_value = v_config_value;
    set out_msg = 'Available';
    set out_result = 1;
  else
    set out_config_value = '';
    set out_msg = 'Not available';
    set out_result = 0;
  end if;
end $$

DELIMITER ;