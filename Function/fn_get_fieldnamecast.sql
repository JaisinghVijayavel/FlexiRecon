DELIMITER $$

DROP function IF EXISTS `fn_get_fieldnamecast` $$
CREATE function `fn_get_fieldnamecast`
(
  in_recon_code varchar(32),
  in_field_name varchar(255)
) returns text
begin
  declare v_field_name varchar(255);
  declare v_field_type varchar(128);
  declare v_field_org_type varchar(128);

  set v_field_name = in_field_name;
  set v_field_type = fn_get_fieldtype(in_recon_code,in_field_name);
  set v_field_org_type = fn_get_fieldorgtype(in_recon_code,in_field_name);

  if v_field_type = '' then
    set v_field_type = 'TEXT';
  end if;

  if v_field_org_type = '' then
    set v_field_org_type = 'TEXT';
  end if;

  if v_field_type <> v_field_org_type then
    if v_field_type = 'INTEGER' then
      set v_field_name = concat("cast(",v_field_name," as signed)");
    elseif v_field_type = 'NUMERIC' or v_field_type = 'NUMBER' then
      set v_field_name = concat("cast(",v_field_name," as decimal(15,3))");
    elseif v_field_type = 'DATE' then
      set v_field_name = concat("cast(",v_field_name," as date)");
    elseif v_field_type = 'DATRETIME' then
      set v_field_name = concat("cast(",v_field_name," as datetime)");
    end if;
  end if;

  return v_field_name;
end $$

DELIMITER ;