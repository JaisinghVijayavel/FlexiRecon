DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_datasetfieldname` $$
CREATE FUNCTION `fn_get_datasetfieldname`(in_dataset_code varchar(32),in_dataset_field varchar(128)) RETURNS text
begin
  declare v_dataset_field_name text;

  select
    field_name
  into
    v_dataset_field_name
  from recon_mst_tdatasetfield
  where dataset_code = in_dataset_code
  and dataset_table_field = in_dataset_field
  and delete_flag = 'N';

  set v_dataset_field_name = ifnull(v_dataset_field_name,in_dataset_field);

  return v_dataset_field_name;
end $$

DELIMITER ;