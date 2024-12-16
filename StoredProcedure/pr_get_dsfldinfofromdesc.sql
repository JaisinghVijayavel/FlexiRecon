DELIMITER $$

DROP procedure IF EXISTS `pr_get_dsfldinfofromdesc` $$
CREATE procedure `pr_get_dsfldinfofromdesc`
(
  in in_ds_code varchar(32),
  in in_ds_flddesc varchar(255),
  out out_ds_fldname varchar(255),
  out out_ds_fldtype varchar(255)
)
begin
  declare v_field_name text;
  declare v_field_type text;

  select
    dataset_table_field,
    field_type
  into
    v_field_name,
    v_field_type
  from recon_mst_tdatasetfield
  where dataset_code = in_ds_code
  and field_name = in_ds_flddesc
  and delete_flag = 'N';

  set v_field_name = ifnull(v_field_name,'');
  set v_field_type = ifnull(v_field_type,'');

  set out_ds_fldname = v_field_name;
  set out_ds_fldtype = v_field_type;
end $$

DELIMITER ;