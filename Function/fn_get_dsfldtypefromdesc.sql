DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_dsfldtypefromdesc` $$
CREATE FUNCTION `fn_get_dsfldtypefromdesc`
(
  in_ds_code varchar(32),
  in_ds_flddesc varchar(255)
) RETURNS text
begin
  declare v_field_type text;

  select
    field_type
  into
    v_field_type
  from recon_mst_tdataset
  where dataset_code = in_ds_code
  and dataset_name = in_ds_flddesc
  and delete_flag = 'N';

  set v_field_type = ifnull(v_field_type,'');

  if v_field_type = '' then
    set v_field_type = 'TEXT';
  end if;

  return v_field_type;
end $$

DELIMITER ;