DELIMITER $$

DROP function IF EXISTS `fn_get_con_fieldcast` $$
CREATE function `fn_get_con_fieldcast`
(
  in_pipeline_code varchar(32),
  in_dataset_code varchar(32),
  in_sourcefield_name varchar(255)
) returns text
begin
  /*
    Created By : Vijayavel
    Created Date : 13-04-2026

    Updated By :
    updated Date :

    Version : 1
  */

  declare v_field_name varchar(255);
  declare v_field_type varchar(128);

	select
		dataset_table_field,
		sourcefield_datatype
	into
		v_field_name,
		v_field_type
	from con_trn_tpplsourcefield
	where sourcefield_name = in_sourcefield_name
	and pipeline_code = in_pipeline_code
  and dataset_code = in_dataset_code
	and delete_flag = 'N';

	set v_field_name = ifnull(v_field_name,'');
	set v_field_type = ifnull(v_field_type,'');

  if v_field_type = '' then
    set v_field_type = 'TEXT';
  end if;

  if v_field_type = 'INTEGER' then
    set v_field_name = concat("cast(",v_field_name," as signed)");
  elseif v_field_type = 'NUMERIC' then
    set v_field_name = concat("cast(",v_field_name," as decimal(15,3))");
  elseif v_field_type = 'DATE' then
    set v_field_name = concat("cast(",v_field_name," as date)");
  elseif v_field_type = 'DATRETIME' then
    set v_field_name = concat("cast(",v_field_name," as datetime)");
  end if;

  return v_field_name;
end $$

DELIMITER ;