DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_fieldtype` $$
CREATE FUNCTION `fn_get_fieldtype`(in_recon_code varchar(32),in_field_name varchar(128)) RETURNS text 
begin
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 06-03-2026

    Version : 1
  */

  declare v_field_type text;
  declare v_field_name text;

  set v_field_name = in_field_name;

  if instr(v_field_name,'.') > 0 then
    set v_field_name = SPLIT(v_field_name,'.',2);
  end if;

  select
    recon_field_type
  into
    v_field_type
  from recon_mst_treconfield
  where recon_field_name = v_field_name
  and recon_code = in_recon_code
  and delete_flag = 'N';

  set v_field_type = ifnull(v_field_type,'');

  if v_field_type = '' then
    select
      field_type
    into
      v_field_type
    from recon_mst_tfieldstru
    where field_name = v_field_name
    and delete_flag = 'N';
  end if;

  set v_field_type = ifnull(v_field_type,'');

  if v_field_type = '' then
    set v_field_type = 'TEXT';
  end if;

  return v_field_type;
end $$

DELIMITER ;