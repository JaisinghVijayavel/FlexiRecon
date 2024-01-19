DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_dataset_clone` $$
CREATE PROCEDURE `pr_recon_mst_dataset_clone`
(
	in in_clone_dataset_code varchar(32),
	in in_new_dataset_code varchar(32),
	in in_action  varchar(16),
	in in_action_by varchar(10),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN
	/*
		Created By : Hema
		Created Date : Oct-16-2023
        
		Updated By : Vijayavel J
		Updated Date : Dec-19-2023
		
		Version No : 2
	*/
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_msg text default '';
	
  if (in_action = 'INSERT') then
		insert into recon_mst_tdatasetfield
		(
		datasetfield_gid,
		dataset_code,
		field_name,
		field_type,
		field_length,
		field_mandatory,
		precision_length,
		scale_length,
		dataset_field_sno,
		dataset_table_field,
		active_status,
		insert_date,
		insert_by,
		delete_flag
		)
		SELECT 
		0,
		in_new_dataset_code,
		field_name,
		field_type,
		field_length,
		field_mandatory,
		precision_length,
		scale_length,
		dataset_field_sno,
		dataset_table_field,
		active_status,
		now(),
		in_action_by,
		delete_flag 
		FROM recon_mst_tdatasetfield 
		where dataset_code = in_clone_dataset_code 
		and active_status = 'Y';
		set v_msg = 'Record Saved Successfully.. !';
		set out_result = 1;
		set out_msg = v_msg;
	elseif(in_action = 'DELETE') then
		update recon_mst_tdatasetfield set 
			update_date = sysdate(),
			update_by = in_action_by,
			delete_flag = 'Y'  
		where dataset_code = in_new_dataset_code;
		set v_msg = 'Record deleted Successfully.. !';
		set out_result = 1;
		set out_msg = v_msg;
	end if;
END $$

DELIMITER ;