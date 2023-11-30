DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_fieldtype` $$
CREATE FUNCTION `fn_get_fieldtype`(in_recon_code varchar(32),in_field_name varchar(128)) RETURNS text CHARSET latin1
begin
  declare v_field_type varchar(128);

  if instr(in_field_name,'.') = 0 then
    select
      field_type
    into
      v_field_type
    from recon_mst_treconfield
    where recon_field_name = in_field_name
    and recon_code = in_recon_code
    and delete_flag = 'N';
  else
    select
      field_type
    into
      v_field_type
    from recon_mst_treconfield
    where recon_field_name = SPLIT(in_field_name,'.',2)
    and recon_code = in_recon_code 
    and delete_flag = 'N';
  end if;

  set v_field_type = ifnull(v_field_type,'');

  return v_field_type;
end $$

DELIMITER ;