DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_dsfieldtype` $$
CREATE FUNCTION `fn_get_dsfieldtype`(in_dataset_code varchar(32),in_field_name varchar(128)) RETURNS text
begin
  declare v_field_type varchar(128);

  if instr(in_field_name,'.') = 0 then
    select
      field_type
    into
      v_field_type
    from recon_mst_tdatasetfield
    where dataset_table_field = in_field_name
    and dataset_code = in_dataset_code
    and delete_flag = 'N';

    set v_field_type = ifnull(v_field_type,'');

    if v_field_type = '' then
      select
        field_type
      into
        v_field_type
      from recon_mst_tfieldstru
      where field_name = in_field_name
      and delete_flag = 'N';
    end if;
  else
    select
      field_type
    into
      v_field_type
    from recon_mst_tdatasetfield
    where dataset_table_field = SPLIT(in_field_name,'.',2)
    and dataset_code = in_dataset_code
    and delete_flag = 'N';

    set v_field_type = ifnull(v_field_type,'');

    if v_field_type = '' then
      select
        field_type
      into
        v_field_type
      from recon_mst_tfieldstru
      where field_name = SPLIT(in_field_name,'.',2)
      and delete_flag = 'N';
    end if;
  end if;

  set v_field_type = ifnull(v_field_type,'');

  if v_field_type = '' then
    set v_field_type = 'TEXT';
  end if;

  return v_field_type;
end $$

DELIMITER ;