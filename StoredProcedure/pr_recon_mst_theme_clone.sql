DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_theme_clone` $$
CREATE PROCEDURE `pr_recon_mst_theme_clone`
(
	in in_clone_theme_code varchar(32),
	in in_new_theme_code varchar(32),
	in in_action  varchar(32),
	in in_action_by varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN
	/*
		Created By : vinoth
		Created Date :feb-27-2024
        
	*/
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_msg text default '';
	
  if (in_action = 'INSERT') then
		INSERT INTO recon_mst_tthemefilter
		(
			theme_code,
			theme_seqno,
			recon_field,
			filter_criteria,
			filter_value,
			open_parentheses_flag,
			close_parentheses_flag,
			join_condition,
			active_status,
			insert_date,
			insert_by
		)
		SELECT 
			in_new_theme_code,
			theme_seqno,
			recon_field,
			filter_criteria,
			filter_value,
			open_parentheses_flag,
			close_parentheses_flag,
			join_condition,
			active_status,
			now(),
			in_action_by
		FROM recon_mst_tthemefilter 
		where theme_code = in_clone_theme_code 
		and active_status = 'Y'
		and delete_flag = 'N';
		
		set v_msg = 'Record Saved Successfully.. !';
		set out_result = 1;
		set out_msg = v_msg;
	elseif(in_action = 'DELETE') then
		update recon_mst_tthemefilter set 
			update_date = sysdate(),
			update_by = in_action_by,
			delete_flag = 'Y'  
		where theme_code = in_new_theme_code
		and delete_flag = 'N';
		
		set v_msg = 'Record deleted Successfully.. !';
		set out_result = 1;
		set out_msg = v_msg;
	end if;
END $$

DELIMITER ;