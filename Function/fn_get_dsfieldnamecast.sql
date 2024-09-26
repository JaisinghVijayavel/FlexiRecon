DELIMITER $$

DROP function IF EXISTS `fn_get_dsfieldnamecast` $$
CREATE function `fn_get_dsfieldnamecast`
(
  in_dataset_code varchar(32),
  in_field_name varchar(255)
) returns text
begin
  declare v_field_name varchar(255);
  declare v_field_type varchar(128);

  if instr(in_field_name,'.') = 0 then
	  select
		  dataset_table_field,
		  field_type
	  into
		  v_field_name,
		  v_field_type
	  from recon_mst_tdatasetfield
	  where dataset_table_field = in_field_name
	  and dataset_code = in_dataset_code
	  and delete_flag = 'N';
  else
	  select
		  dataset_table_field,
		  field_type
	  into
		  v_field_name,
		  v_field_type
	  from recon_mst_tdatasetfield
	  where dataset_table_field = split(in_field_name,'.',2)
	  and dataset_code = in_dataset_code
	  and delete_flag = 'N';
  end if;

	set v_field_name = ifnull(v_field_name,'');
	set v_field_type = ifnull(v_field_type,'');

  if v_field_type = '' then
    set v_field_type = 'TEXT';
  end if;

  if lower(mid(v_field_name,1,3)) = 'col' then
		if v_field_type = 'INTEGER' then
			set v_field_name = concat("cast(",in_field_name," as signed)");
		elseif v_field_type = 'NUMERIC' then
			set v_field_name = concat("cast(",in_field_name," as decimal(15,3))");
		elseif v_field_type = 'DATE' then
			set v_field_name = concat("cast(",in_field_name," as date)");
		elseif v_field_type = 'DATRETIME' then
			set v_field_name = concat("cast(",in_field_name," as datetime)");
    else
      set v_field_name = in_field_name;
		end if;
  else
    set v_field_name = in_field_name;
  end if;

  return v_field_name;
end $$

DELIMITER ;