DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_fieldaliasname` $$
CREATE FUNCTION `fn_get_fieldaliasname`(in_field_name varchar(255)) RETURNS text
begin
  declare v_field_alias_name text;

  if instr(in_field_name,'.') > 0 then
    set v_field_alias_name = SPLIT(in_field_name,'.',2);
  end if;

  select
    field_alias_name
  into
    v_field_alias_name
  from recon_mst_tfieldstru
  where field_name = in_field_name
  and delete_flag = 'N';

  set v_field_alias_name = ifnull(v_field_alias_name,in_field_name);

  return v_field_alias_name;
end $$

DELIMITER ;