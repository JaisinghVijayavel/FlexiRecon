DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_Datasetfielddetail` $$
CREATE PROCEDURE `pr_get_Datasetfielddetail`(
  in in_dataset_code varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
begin

  /*
    Created By : vinoth
    Created Date :

    Updated By : Vijayavel
    Updated Date : 15-06-2026

    Version : 2
  */

  select
    datasetfield_gid,
    dataset_code,
    field_name,
    field_type,
	  fn_get_mastername(field_type, 'QCD_RC_FIELD_TYPE') as fieldtype_desc,
    ifnull(field_length,'') as field_length,
    ifnull(precision_length,0) as precision_length,
    ifnull(scale_length,0) as scale_length,
    case field_mandatory when 'Y' then 'Yes' else 'No' end as field_mandatory,
    dataset_table_field
  from
    recon_mst_tdatasetfield a
  where dataset_code=in_dataset_code
  and a.delete_flag = 'N'
  union
  select
    -1,
    in_dataset_code,
    'Dataset Id',
    'INTEGER',
    'INTEGER',
    8,
    null,
    null,
    'Yes',
    'dataset_gid'
  union
  select
    -2,
    in_dataset_code,
    'Scheduler Id',
    'INTEGER',
    'INTEGER',
    8,
    null,
    null,
    'Yes',
    'scheduler_gid';
end $$

DELIMITER ;