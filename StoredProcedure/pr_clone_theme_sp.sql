DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_clone_theme_sp` $$
CREATE PROCEDURE `pr_clone_theme_sp`(
  in in_theme_name varchar(255),
  in in_clone_theme_code varchar(32),
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  out out_theme_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_sql text default '';
  declare v_txt text default '';
  declare v_theme_code text default '';
  declare v_theme_gid int default 0;
  declare v_theme_order int default 0; 
  declare v_source_datset_code text default '';
  declare v_comparison_dataset_code text default '';
  
  set out_theme_code='';
  set in_theme_name = ifnull(in_theme_name,'');  
	
	if exists(select theme_code from recon_mst_ttheme where theme_code = in_clone_theme_code and delete_flag = 'N') then
		set v_theme_code = fn_get_autocode('THEME');       
		select max(theme_order)+1 into v_theme_order from recon_mst_ttheme where recon_code=in_recon_code and delete_flag='N';
    -- insert in theme table
	INSERT INTO recon_mst_ttheme
	(	
		theme_code,
		theme_name,
		recon_code,
		theme_desc,
    theme_query,
		theme_type_code,
		shortexcess_code,
		source_dataset_code,
		source_dataset_type,
		source_acc_mode,
		comparison_dataset_code,
		comparison_dataset_type,
		comparison_acc_mode,
		group_flag,
		hold_flag,
		theme_order,
		recon_version,
		active_status,
		inactive_reason,
		clone_theme_code,
		insert_date,
		insert_by)
    select
		v_theme_code,
		theme_name,
		in_recon_code,
    theme_desc,
    theme_query,
		theme_type_code,
		shortexcess_code,
		source_dataset_code,
		source_dataset_type,
		source_acc_mode,
		comparison_dataset_code,
		comparison_dataset_type,
		comparison_acc_mode,
		group_flag,
		hold_flag,
		theme_order,
		recon_version,
		active_status,
		inactive_reason,
		clone_theme_code,
		sysdate(),
		in_user_code
		from recon_mst_ttheme where theme_code = in_clone_theme_code and delete_flag = 'N';

  set v_source_datset_code= (select dataset_code from recon_mst_trecondataset  a
  inner join recon_mst_ttheme b on a.recon_code=b.recon_code
  where a.recon_code = in_recon_code and source_dataset_code = clone_dataset_code and theme_code=v_theme_code and a.delete_flag = 'N' and a.active_status='Y');

  update recon_mst_ttheme set source_dataset_code =v_source_datset_code where theme_code=v_theme_code;

  set v_comparison_dataset_code=(select dataset_code from recon_mst_trecondataset  a
  inner join recon_mst_ttheme b on a.recon_code=b.recon_code
  where a.recon_code = in_recon_code and comparison_dataset_code = clone_dataset_code and theme_code=v_theme_code and a.delete_flag = 'N' and a.active_status='Y');

   update recon_mst_ttheme set comparison_dataset_code = v_comparison_dataset_code where theme_code=v_theme_code;

    -- insert in theme condition
    insert into recon_mst_tthemecondition
	(
		theme_code,
        themecondition_seqno,
		source_field,
		extraction_criteria,
		comparison_field,
		comparison_criteria,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		themecon_version,
		active_status,
		insert_date,
		insert_by
	)
    select
		v_theme_code,
		themecondition_seqno,
		source_field,
		extraction_criteria,
		comparison_field,
		comparison_criteria,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		themecon_version,
		active_status,
		sysdate(),
		in_user_code
		from recon_mst_tthemecondition where theme_code = in_clone_theme_code and active_status = 'Y' and delete_flag = 'N';
    -- identifier
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
		themefilter_version,
		active_status,
		insert_date,
		insert_by
		) 
	select
		v_theme_code,
		themefilter_seqno,
		filter_applied_on,
		filter_field,
		filter_criteria,
		filter_value_flag,
		filter_value,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		themefilter_version,
		active_status,
		sysdate(),
		in_user_code
		from recon_mst_tthemefilter where theme_code = in_clone_theme_code and active_status = 'Y' and delete_flag = 'N';
    
    -- grouping
    insert into recon_mst_tthemegrpfield
    (
        theme_code,
		grpfield_seqno,
		grpfield_applied_on,
		grp_field,
		themegrp_version,
		active_status,
        insert_date,
        insert_by
        )
	select 
		v_theme_code,
		grpfield_seqno,
		grpfield_applied_on,
		grp_field,
		themegrp_version,
		active_status,
        sysdate(),
		in_user_code
		from recon_mst_tthemegrpfield where theme_code = in_clone_theme_code and active_status = 'Y' and delete_flag = 'N';
    
    -- aggfunction
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
		themeaggfield_version,
		active_status,
        insert_date,
        insert_by
        ) 
		select 
		v_theme_code,
		themeaggfield_seqno,
		themeaggfield_applied_on,
		themeaggfield_name,
		recon_field,
		themeagg_function,
		themeagg_field,
		themeagg_field_sno,
		themeagg_field_type,
		themeaggfield_version,
		active_status,
        sysdate(),
		in_user_code
		from recon_mst_tthemeaggfield where theme_code = in_clone_theme_code and active_status = 'Y' and delete_flag = 'N';
    
    -- agg condition
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
		themeagg_version,
		active_status,
        insert_date,
        insert_by
        ) 
	select 
		v_theme_code,
		themeaggcondition_seqno,
		themeagg_applied_on,
		themeagg_field,
		themeagg_criteria,
		themeagg_value_flag,
		themeagg_value,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		themeagg_version,
		active_status,
        sysdate(),
		in_user_code
		from recon_mst_tthemeaggcondition where theme_code = in_clone_theme_code and active_status = 'Y' and delete_flag = 'N';
	end if;
	set out_theme_code = v_theme_code;
	set out_result = 1;
	set out_msg = 'Success';
end $$

DELIMITER ;