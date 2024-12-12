DELIMITER $$

DROP function IF EXISTS `fn_get_castfield` $$
CREATE function `fn_get_castfield`
(
  in_field_name varchar(255),
  in_field_type varchar(255)
) returns text
begin
  declare v_field_name varchar(255);
  declare v_field_type varchar(128);
  declare v_field_cast text default '';

  set v_field_name = ifnull(in_field_name,'');
  set v_field_type = ifnull(in_field_type,'');

  if v_field_name <> '' and in_field_type <> '' then
		if v_field_type = 'INTEGER' then
			set v_field_cast = concat("cast(",in_field_name," as signed)");
		elseif v_field_type = 'NUMERIC' then
			set v_field_cast = concat("cast(",v_field_name," as decimal(15,3))");
		elseif v_field_type = 'DATE' then
			set v_field_cast = concat("cast(",v_field_name," as date)");
		elseif v_field_type = 'DATRETIME' then
			set v_field_cast = concat("cast(",v_field_name," as datetime)");
    else
      set v_field_cast = concat("cast(",v_field_name," as nchar)");
		end if;
  else
      set v_field_cast = concat("cast(",v_field_name," as nchar)");
  end if;

  return v_field_cast;
end $$

DELIMITER ;