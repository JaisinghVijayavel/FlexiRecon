DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_upd_theme` $$
CREATE PROCEDURE `pr_recon_upd_theme`
(
  in in_base_recon_code varchar(32),
  in in_base_theme_code varchar(32),
  in in_update_recon_code varchar(32),
  in in_update_theme_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int(10)
)
BEGIN
-- theme
	update recon_mst_tthemecondition set active_status = 'N',delete_flag='Y',update_date = sysdate(),update_by =  in_user_code where theme_code = in_update_theme_code;
	update recon_mst_tthemefilter set update_date = sysdate(),update_by = in_user_code,active_status='N',delete_flag='Y' where theme_code = in_update_theme_code;
	update recon_mst_tthemeaggfield set	update_date = sysdate(),update_by = in_user_code,active_status='N',delete_flag='Y'where theme_code = in_update_theme_code;
	update recon_mst_tthemeaggcondition set	update_date = sysdate(),update_by = in_user_code,active_status='N',delete_flag='Y' where theme_code = in_update_theme_code;

 -- update in theme condition
    insert into recon_mst_tthemecondition
	(
		theme_code,
        themecondition_seqno,
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
    select
		in_update_theme_code,
		themecondition_seqno,
		source_field,
		comparison_field,
		extraction_criteria,
		comparison_criteria,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		active_status,
		sysdate(),
		in_user_code
		from recon_mst_tthemecondition where theme_code = in_base_theme_code and active_status = 'Y' and delete_flag = 'N';
    -- update identifier
    INSERT INTO recon_mst_tthemefilter
	(
		theme_code,
		themefilter_seqno,
		filter_applied_on,
		filter_field,
		filter_criteria,
		filter_value_flag,
		filter_value,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		active_status,
		insert_date,
		insert_by
		) 
	select
		in_update_theme_code,
		themefilter_seqno,
		filter_applied_on,
		filter_field,
		filter_criteria,
		filter_value_flag,
		filter_value,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		active_status,
		sysdate(),
		in_user_code
		from recon_mst_tthemefilter where theme_code = in_base_theme_code and active_status = 'Y' and delete_flag = 'N';
    
    -- update grouping
    insert into recon_mst_tthemegrpfield
    (
        theme_code,
        grpfield_seqno,
        grpfield_applied_on,
        grp_field,
        active_status,
        insert_date,
        insert_by
        )
	select 
		in_update_theme_code,
		grpfield_seqno,
        grpfield_applied_on,
        grp_field,
        active_status,
        sysdate(),
		in_user_code
		from recon_mst_tthemegrpfield where theme_code = in_base_theme_code and active_status = 'Y' and delete_flag = 'N';
    
    -- update aggfunction
    insert into recon_mst_tthemeaggfield
    (
        theme_code,
        themeaggfield_seqno,
        themeaggfield_applied_on,
        themeaggfield_name,
        recon_field,
        themeagg_function,
        themeagg_field,
        themeagg_field_sno,
        themeagg_field_type,
        active_status,
        insert_date,
        insert_by
        ) 
		select 
		in_update_theme_code,
		themeaggfield_seqno,
        themeaggfield_applied_on,
        themeaggfield_name,
        recon_field,
        themeagg_function,
        themeagg_field,
        themeagg_field_sno,
        themeagg_field_type,
        active_status,
        sysdate(),
		in_user_code
		from recon_mst_tthemeaggfield where theme_code = in_base_theme_code and active_status = 'Y' and delete_flag = 'N';
    
    -- update agg condition
    insert into recon_mst_tthemeaggcondition
    (
        theme_code,
        themeaggcondition_seqno,
        themeagg_applied_on,
        themeagg_field,
        themeagg_criteria,
        themeagg_value_flag,
        themeagg_value,
        open_parentheses_flag,
        close_parentheses_flag,
        join_condition,
        active_status,
        insert_date,
        insert_by
        ) 
	select 
		in_update_theme_code,
		themeaggcondition_seqno,
        themeagg_applied_on,
        themeagg_field,
        themeagg_criteria,
        themeagg_value_flag,
        themeagg_value,
        open_parentheses_flag,
        close_parentheses_flag,
        join_condition,
        active_status,
        sysdate(),
		in_user_code
		from recon_mst_tthemeaggcondition where theme_code = in_base_theme_code and active_status = 'Y' and delete_flag = 'N';
	
	set out_result = 1;
	set out_msg = 'Success';

END $$

DELIMITER ;