DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_datasetfieldtype` $$
CREATE FUNCTION `fn_get_datasetfieldtype`(in_dataset_code varchar(32),in_dataset_field varchar(128)) RETURNS text
begin
  /*
    Created By : Vijayavel
    Created Date : 06-03-2026

    Updated By :
    updated Date :

    Version : 1
  */

  declare v_dataset_field_type text;

  select
    field_type
  into
    v_dataset_field_type
  from recon_mst_tdatasetfield
  where dataset_code = in_dataset_code
  and dataset_table_field = in_dataset_field
  and delete_flag = 'N';

  set v_dataset_field_type = ifnull(v_dataset_field_type,'TEXT');

  return v_dataset_field_type;
end $$

DELIMITER ;