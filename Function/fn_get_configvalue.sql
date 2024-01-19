DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_configvalue`$$
CREATE FUNCTION `fn_get_configvalue`(in_config_name text) RETURNS text CHARSET latin1
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
  end if;

  return v_config_value;
end;

 $$

DELIMITER ;