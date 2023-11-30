DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_fieldorgtype` $$
CREATE FUNCTION `fn_get_fieldorgtype`(in_recon_code varchar(32),in_field_name varchar(128)) RETURNS text CHARSET latin1
begin
  declare v_field_org_type varchar(128);

  if instr(in_field_name,'.') = 0 then
    select
      field_org_type
    into
      v_field_org_type
    from recon_mst_tfieldstru
    where field_name = in_field_name
    and delete_flag = 'N';
  else
    select
      field_org_type
    into
      v_field_org_type
    from recon_mst_tfieldstru
    where field_name = SPLIT(in_field_name,'.',2)
    and delete_flag = 'N';
  end if;

  set v_field_org_type = ifnull(v_field_org_type,'');

  return v_field_org_type;
end $$

DELIMITER ;