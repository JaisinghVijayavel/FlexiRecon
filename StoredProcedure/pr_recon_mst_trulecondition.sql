DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_trulecondition` $$
CREATE PROCEDURE `pr_recon_mst_trulecondition`
(
	inout in_rulecondition_gid int(10),
	in in_rule_code varchar(32),
	in in_source_field varchar(128),
	in in_comparison_field varchar(128),
	in in_extraction_criteria text,
	in in_comparison_criteria text,
	in in_open_flag varchar(32),
	in in_close_flag varchar(32),
	in in_join_condition varchar(32),
	in in_active_status char(1),
	in in_action varchar(32),
	in in_action_by varchar(10),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_rulecondition_gid int default 0;
	declare v_msg text default '';
	
	if in_action = "UPDATE"  or in_action = "INSERT" then
		if in_rule_code = '' or in_rule_code is null then
			set err_msg := concat(err_msg,'Rule code cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_source_field = '' or in_source_field is null then
			set err_msg := concat(err_msg,'Source field cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_comparison_field = '' or in_comparison_field is null then
			set err_msg := concat(err_msg,'Comparision field name cannot be empty,');
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
	
  if in_action = "UPDATE"  or in_action = "DELETE" then
		set in_rulecondition_gid = ifnull(in_rulecondition_gid,0);
		
		if not exists(select rulecondition_gid from recon_mst_trulecondition 
			where rulecondition_gid = in_rulecondition_gid 
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid record gid,');
			set err_flag := true;
		end if;
        
		if not exists(select rule_gid from recon_mst_trule 
			where rule_code = in_rule_code 
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid rule,');
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
		set in_rulecondition_gid = 0;  
		
		insert into recon_mst_trulecondition
		(
			rulecondition_gid,
			rule_code,
			source_field,
			comparison_field,
			extraction_criteria,
			comparison_criteria,
			open_parentheses_flag,
			close_parentheses_flag,
			join_condition,
			active_status,
			insert_date,
			insert_by
		)
		value
		(
			in_rulecondition_gid,
			in_rule_code,
			in_source_field,
			in_comparison_field,
			in_extraction_criteria,
			in_comparison_criteria,
			in_open_flag,
			in_close_flag,
			in_join_condition,
			in_active_status,
			sysdate(),
			in_action_by
		);
		
		select 
			max(rulecondition_gid) into v_rulecondition_gid 
		from recon_mst_trulecondition;
		
		set in_rulecondition_gid = v_rulecondition_gid;
		
		set v_msg = 'Record saved successfully.. !';
  elseif(in_action = 'UPDATE') then
		update recon_mst_trulecondition set
			rule_code = in_rule_code,
			source_field = in_source_field,
			comparison_field = in_comparison_field,
			extraction_criteria = in_extraction_criteria,
			comparison_criteria = in_comparison_criteria,
			open_parentheses_flag = in_open_flag,
			close_parentheses_flag = in_close_flag,
			join_condition = in_join_condition,
			update_date = sysdate(),
			update_by = in_action_by 
		where rulecondition_gid = in_rulecondition_gid
		and delete_flag = 'N';
        
		set v_rulecondition_gid = in_rulecondition_gid;
		set v_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then 
		update recon_mst_trulecondition set
			update_date = sysdate(),
			update_by = in_action_by,
			delete_flag = 'Y'  
		where rulecondition_gid = in_rulecondition_gid
		and delete_flag = 'N';
        
		set v_rulecondition_gid = in_rulecondition_gid;
		set v_msg = 'Record deleted successfully.. !';
	end if;
	
	commit;
	
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;