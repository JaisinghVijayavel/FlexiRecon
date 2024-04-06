DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_tthemefield` $$
CREATE PROCEDURE `pr_recon_mst_tthemefield`
(
	inout in_themefilter_gid int,
  in in_theme_seqno int,
	in in_theme_code varchar(32),    
	in in_recon_field varchar(255),
	in in_filter_criteria text,
	in in_filter_value text,
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
	declare v_themefilter_gid int default 0;
	declare v_msg text default ''; 
	declare v_count int default 0;	
	
	if (in_action = 'INSERT') then
		set in_themefilter_gid = 0;
		
		select count(*)+1 into v_count from recon_mst_tthemefilter where theme_code= in_theme_code;
		
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
		VALUES
		(
			in_theme_code,
			in_theme_seqno,
			in_recon_field,
			in_filter_criteria,
			in_filter_value,
			in_open_flag,
			in_close_flag,
			in_join_condition,
			'Y',
			sysdate(),
			in_user_code
		);
		
		select max(themefilter_gid) into v_themefilter_gid from recon_mst_tthemefilter;
		set in_themefilter_gid = v_themefilter_gid;
		
		set v_msg = 'Record saved successfully.. !';  
	elseif(in_action = 'UPDATE') then
		UPDATE recon_mst_tthemefilter SET
			theme_code = in_theme_code,
			theme_seqno = in_theme_seqno,
			recon_field = in_recon_field,
			filter_criteria = in_filter_criteria,
			filter_value = in_filter_value,
			open_parentheses_flag = in_open_flag,
			close_parentheses_flag = in_close_flag,
			join_condition = in_join_condition,
			update_date = sysdate(),
			update_by = in_user_code
		where themefilter_gid = in_themefilter_gid
		and delete_flag = 'N';     
		
		set in_themefilter_gid = in_themefilter_gid;
		
		set v_msg = 'Record updated successfully.. !';  
	elseif(in_action = 'DELETE') then
		update recon_mst_tthemefilter set
			update_date = sysdate(),
			update_by = in_action_by,			
			active_status='N'
		where themefilter_gid = in_themefilter_gid
		and delete_flag = 'N';   
		
		set in_themefilter_gid = in_themefilter_gid;
		
		set v_msg = 'Record deleted successfully.. !';  
	end if;
    
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;