DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_datasetname` $$
CREATE FUNCTION `fn_get_datasetname`(in_dataset_code varchar(32)) RETURNS text
begin
  declare v_dataset_name text;

  select
    dataset_name
  into
    v_dataset_name
  from recon_mst_tdataset
  where dataset_code = in_dataset_code
  and delete_flag = 'N';

  set v_dataset_name = ifnull(v_dataset_name,'');

  return v_dataset_name;
end $$

DELIMITER ;