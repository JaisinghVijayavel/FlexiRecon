DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_confieldcast` $$
CREATE FUNCTION `fn_get_confieldcast`
(
  in_dataset_code text,
  in_field_name text
) RETURNS text
begin
  declare v_field_type text default '';
  declare v_cast_field text default '';

  select
    field_type
  into
    v_field_type
  from recon_mst_tdatasetfield
  where dataset_code = in_dataset_code
  and dataset_table_field = in_field_name
  and delete_flag = 'N';

  set v_field_type = ifnull(v_field_type,'');

  if v_field_type = 'DATE' or v_field_type = 'DATETIME' then
    set v_cast_field = concat('STR_TO_DATE(if(',in_field_name,'='''',null,',in_field_name,'),''#DATE_FORMAT#'')');
  elseif v_field_type = 'INTEGER' then
    set v_cast_field = concat('CAST(',in_field_name, ' AS signed)');
  elseif v_field_type = 'NUMERIC' then
    set v_cast_field = concat('CAST(',in_field_name, ' AS DECIMAL(15,2))');
  else
    set v_cast_field = in_field_name;
  end if;

  return v_cast_field;
end $$

DELIMITER ;