DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_dsfldnamefromdesc` $$
CREATE FUNCTION `fn_get_dsfldnamefromdesc`
(
  in_ds_code varchar(32),
  in_ds_flddesc varchar(255)
) RETURNS text
begin
  declare v_dataset_field text;

  select
    dataset_table_field
  into
    v_dataset_field
  from recon_mst_tdatasetfield
  where dataset_code = in_ds_code
  and field_name = in_ds_flddesc
  and delete_flag = 'N';

  set v_dataset_field = ifnull(v_dataset_field,'');

  return v_dataset_field;
end $$

DELIMITER ;