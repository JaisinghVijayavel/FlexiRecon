DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_recon_datamapping_list` $$
CREATE PROCEDURE `pr_get_recon_datamapping_list`
(
	in in_recon_code varchar(32),
	in in_recon_field_name varchar(255),
	in in_dataset_code varchar(255),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
me:BEGIN
	select distinct
		a.reconfieldmapping_gid,
		-- a.recon_code,
		-- a.recon_field_name,
		a.dataset_code,
		d.dataset_name,
		b.field_name,
		-- c.display_order,
		a.active_status,
		case a.active_status when 'Y' then 'Active' else 'Inactive' end as active_status_desc,
		dataset_table_field
	from recon_mst_treconfieldmapping a
	inner join recon_mst_tdatasetfield b on a.dataset_field_name=b.dataset_table_field
		and a.dataset_code=b.dataset_code
    and b.delete_flag = 'N'
  inner join recon_mst_treconfield as c on a.recon_field_name = c.recon_field_name
    and a.recon_code = c.recon_code 
    and c.delete_flag = 'N'
	inner join recon_mst_tdataset d on a.dataset_code=d.dataset_code and d.delete_flag = 'N'
	where a.recon_code= in_recon_code
	and (a.recon_field_name= in_recon_field_name
  or c.recon_field_desc = in_recon_field_name)
	and a.delete_flag='N';
END $$

DELIMITER ;