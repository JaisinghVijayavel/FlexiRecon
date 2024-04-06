DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_reconfieldname` $$
CREATE FUNCTION `fn_get_reconfieldname`
(
  in_recon_code varchar(32),
  in_recon_field_name varchar(255)
) RETURNS text
begin
  declare v_recon_field_name text;
  declare v_field_desc text;

  set v_recon_field_name = in_recon_field_name;

  if instr(v_recon_field_name,'.') > 0 then
    set v_recon_field_name = SPLIT(v_recon_field_name,'.',2);
  end if;

  select
    recon_field_desc
  into
    v_field_desc
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_name = v_recon_field_name
  and delete_flag = 'N';

  set v_field_desc = ifnull(v_field_desc,'');

  if v_field_desc = '' then
    set v_field_desc = fn_get_fieldaliasname(v_recon_field_name);
  end if;

  return v_field_desc;
end $$

DELIMITER ;