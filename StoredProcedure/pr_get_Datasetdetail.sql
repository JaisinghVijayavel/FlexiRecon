DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_Datasetdetail` $$
CREATE PROCEDURE `pr_get_Datasetdetail`
(
  in in_dataset_gid Int,
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
begin
  declare v_dataset_code text default '';
	
	select
    dataset_gid,
    dataset_code,
    dataset_name,
    dataset_category,
    active_status,      
    fn_get_mastername(active_status, 'QCD_STATUS') as active_status_desc
  from
    recon_mst_tdataset
  where dataset_gid=in_dataset_gid
	and delete_flag = 'N';

  select
    dataset_code into v_dataset_code
  from recon_mst_tdataset
  where dataset_gid=in_dataset_gid
	and delete_flag = 'N';

  select
    datasetfield_gid,
    dataset_code,
    field_name,
    field_type,
		fn_get_mastername(field_type, 'QCD_RC_FIELD_TYPE') as fieldtype_desc,
		case 
			when field_type = 'NUMERIC' then 
			concat('Precision : ',cast(precision_length as nchar),',',
						 'Scale : ',cast(scale_length as nchar))
		else field_length
    end as field_length,
    ifnull(precision_length,0) as precision_length,
    ifnull(scale_length,0) as scale_length,
    case field_mandatory when 'Y' then 'Yes' else 'No' end as field_mandatory,
    dataset_table_field
  from
    recon_mst_tdatasetfield a 
  where true 
	and dataset_code= v_dataset_code 
	and delete_flag = 'N' 
	order by dataset_field_sno asc;
end $$

DELIMITER ;