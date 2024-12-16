DELIMITER $$

DROP function IF EXISTS `fn_get_reconfieldfromdesc` $$
CREATE function `fn_get_reconfieldfromdesc`
(
  in_recon_code varchar(32),
  in_field_desc varchar(255)
) returns text
begin
  declare v_field_name varchar(255);
  declare v_field_type varchar(128);

  -- check in reconfield table
  select
    recon_field_name,
    recon_field_type
  into
    v_field_name,
    v_field_type
  from recon_mst_treconfield
  where recon_field_desc = in_field_desc
  and recon_code = in_recon_code
  and delete_flag = 'N';

  set v_field_name = ifnull(v_field_name,'');

  if v_field_name = '' then
    -- check in fieldstru table (system fields)
    select
      field_name,
      field_type
    into
      v_field_name,
      v_field_type
    from recon_mst_tfieldstru
    where field_alias_name = in_field_desc
    and delete_flag = 'N';
  end if;

  set v_field_name = ifnull(v_field_name,'');

  return v_field_name;
end $$

DELIMITER ;