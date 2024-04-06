DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_tpreprocesscondition` $$
CREATE PROCEDURE `pr_recon_mst_tpreprocesscondition`
(
	inout in_preprocesscondition_gid int,
	in in_preprocess_code varchar(32),
	in in_Ldataset_code varchar(32),
	in in_Lreturn_field varchar(32),
	in in_setrecon_field varchar(32),
	in in_condition_seqno double,
	in in_recon_field varchar(128),
	in in_extraction_criteria text,
	in in_extraction_filter text,
	in in_lookup_field varchar(128),
	in in_comparison_criteria text,
	in in_comparison_filter text,
	in in_open_flag varchar(32),
	in in_close_flag varchar(32),
	in in_join_condition varchar(32),
	in in_active_status char(1),
	in in_action varchar(32),
	in in_action_by varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_preprocesscondition_gid int default 0;
	declare v_msg text default '';
	
	if in_action = "UPDATE"  or in_action = "INSERT" then
		if in_preprocess_code = '' or in_preprocess_code is null then
			set err_msg := concat(err_msg,'Preprocess code cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_recon_field = '' or in_recon_field is null then
			set err_msg := concat(err_msg,'Recon field cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_lookup_field = '' or in_lookup_field is null then
			set err_msg := concat(err_msg,'Lookup field name cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_extraction_criteria = '' or in_extraction_criteria is null then
			set err_msg := concat(err_msg,'Extraction criteria name cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_comparison_criteria = '' or in_comparison_criteria is null then
			set err_msg := concat(err_msg,'Comparision criteria name cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_active_status <> 'Y' and in_active_status <> 'N' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;
  end if;
	
	
	if err_flag = true then	
		set out_result = 0;
		set out_msg = err_msg;
		leave me;
	end if;
	
	start transaction;
	if (in_action = 'INSERT') then 
		set in_preprocesscondition_gid = 0;  
		
		INSERT INTO recon_mst_tpreprocesscondition
		(
			preprocess_code,
			condition_seqno,
			recon_field,
			extraction_criteria,
			extraction_filter,
			lookup_field,
			comparison_criteria,
			comparison_filter,
			open_parentheses_flag,
			close_parentheses_flag,
			join_condition,
			active_status,
			insert_date,
			insert_by
		)
		VALUES
		(
			in_preprocess_code,
			in_condition_seqno,
			in_recon_field,
			in_extraction_criteria,
			in_extraction_filter,
			in_lookup_field,
			in_comparison_criteria,
			in_comparison_filter,
			in_open_flag,
			in_close_flag,
			in_join_condition,
			in_active_status,
			sysdate(),
			in_user_code
		);

		select 
			max(preprocesscondition_gid) into v_preprocesscondition_gid 
		from recon_mst_tpreprocesscondition;
		
		set in_preprocesscondition_gid = v_preprocesscondition_gid;
		
		-- update       
		update recon_mst_tpreprocess set 
			lookup_dataset_code= in_Ldataset_code,
			lookup_return_field= in_Lreturn_field,
			set_recon_field= in_setrecon_field
		where preprocess_code=in_preprocess_code
		and delete_flag = 'N';
    
		set v_msg = 'Record saved successfully.. !';
  elseif(in_action = 'UPDATE') then
		UPDATE recon_mst_tpreprocesscondition SET 
			preprocess_code = in_preprocess_code,
			condition_seqno = in_condition_seqno,
			recon_field = in_recon_field,
			extraction_criteria = in_extraction_criteria,
			extraction_filter = in_extraction_filter,
			lookup_field = in_lookup_field,
			comparison_criteria = in_comparison_criteria,
			comparison_filter = in_comparison_filter,
			open_parentheses_flag = in_open_flag,
			close_parentheses_flag = in_close_flag,
			join_condition = in_join_condition,
			active_status = in_active_status,
			update_date = sysdate(),
			update_by = in_user_code
		WHERE preprocesscondition_gid = in_preprocesscondition_gid
		and delete_flag = 'N';
        
		set v_preprocesscondition_gid = in_preprocesscondition_gid;
        
    -- update       
		update recon_mst_tpreprocess set 
			lookup_dataset_code= in_Ldataset_code,
			lookup_return_field= in_Lreturn_field,
			set_recon_field= in_setrecon_field
		where preprocess_code=in_preprocess_code
		and delete_flag = 'N';
    
		set v_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then 
		update recon_mst_tpreprocesscondition set
			update_date = sysdate(),
			update_by = in_action_by,
			active_status = 'N'  
		where preprocesscondition_gid = in_preprocesscondition_gid
		and delete_flag = 'N';
        
		set v_preprocesscondition_gid = in_preprocesscondition_gid;
		set v_msg = 'Record deleted successfully.. !';
	end if;
	
	commit;
	
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;