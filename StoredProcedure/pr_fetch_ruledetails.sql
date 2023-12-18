DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_ruledetails` $$
CREATE PROCEDURE `pr_fetch_ruledetails`
(
  in in_rule_gid Int(25),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
me:BEGIN
	/*
		Created By : Hema
		Created Date : Oct-16-2023

		Updated By : Vijayavel J
		Updated Date : Dec-14-2023

		Version No : 2
	*/
	
  declare v_rule_code text default '';
	
	select
		a.rule_gid,
		a.rule_code,
		a.rule_name,
		a.recon_code,
		b.recon_name,
		a.rule_apply_on,
		fn_get_mastername(a.rule_apply_on, 'QCD_RS_RULE_APPLLIED') as rule_apply_on_desc,
		DATE_FORMAT(a.period_from,'%d/%m/%Y') as  period_from,
		ifnull(DATE_FORMAT(a.period_to,'%d/%m/%Y'),'') as period_to,
		a.until_active_flag,
		a.source_dataset_code,
		a.comparison_dataset_code,
		a.source_acc_mode,
		fn_get_mastername(a.source_acc_mode, 'QCD_RS_ACC_MODE') as source_acc_mode_desc,
		a.comparison_acc_mode,
		fn_get_mastername(a.comparison_acc_mode, 'QCD_RS_ACC_MODE') as comparison_acc_mode_desc,
		a.reversal_flag,
		a.group_flag,
		a.rule_order as ruleorder,
		a.parent_dataset_code,
		a.parent_acc_mode,
		fn_get_mastername(a.parent_acc_mode, 'QCD_RS_ACC_MODE') as parent_acc_mode_desc,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_trule a
	inner join recon_mst_trecon b on a.recon_code = b.recon_code
		and b.delete_flag = 'N' 
	where a.rule_gid = in_rule_gid  
	and a.delete_flag="N";

	select 
		rule_code into v_rule_code 
	from recon_mst_trule 
	where rule_gid=in_rule_gid
	and delete_flag = 'N';
	
	set v_rule_code = ifnull(v_rule_code,'');

	-- rule condition
	select
		rulecondition_gid,
		a.rule_code,
		a.source_field as source_field_code,
		c.recon_field_desc as source_field_desc,
		a.comparison_field,
		d.recon_field_desc as comparison_field_desc,
		extraction_criteria,
		comparison_criteria,
		case open_parentheses_flag when 'Y' then 'true' else 'false' end as open_parentheses_flag,
		case close_parentheses_flag when 'Y' then 'true' else 'false' end as close_parentheses_flag,
		join_condition,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_trulecondition a
		inner join recon_mst_trule b on b.rule_code = a.rule_code
		inner join recon_mst_treconfield c on c.recon_field_name = a.source_field and b.recon_code = c.recon_code
		inner join recon_mst_treconfield d on d.recon_field_name = a.comparison_field and b.recon_code = d.recon_code
	where a.rule_code = v_rule_code 
	and a.active_status = 'Y' 
	and a.delete_flag='N';
	
	-- grouping
	select
	b.group_method_flag,
	b.manytomany_match_flag,
	a.rulegrpfield_gid,
	a.rule_code,
	c.recon_field_desc as recon_field_name,
	a.grp_field  as recon_field_code,
	a.active_status,
	case a.active_status when 'Y' then 'Active' else 'Inactive' end as active_status_desc
	from recon_mst_trulegrpfield a
	inner join recon_mst_trule b on b.rule_code = a.rule_code
	inner join recon_mst_treconfield c on b.recon_code=c.recon_code and a.grp_field=c.recon_field_name
	where a.rule_code = v_rule_code and a.active_status = 'Y' and a.delete_flag="N";
	-- source identifier
	select
		ruleselefilter_gid,
		a.rule_code,
		filter_applied_on,
		filter_field as filter_field_code,
		recon_field_desc as filter_field,
		filter_criteria,
		ident_criteria,
		ident_value,
		case open_parentheses_flag when 'Y' then 'true' else 'false' end as open_parentheses_flag,
		case close_parentheses_flag when 'Y' then 'true' else 'false' end as close_parentheses_flag,
		join_condition,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_truleselefilter a
	inner join recon_mst_trule b on b.rule_code = a.rule_code
		and b.delete_flag = 'N' 
	inner join recon_mst_treconfield c on c.recon_field_name = a.filter_field 
		and b.recon_code=c.recon_code
		and c.delete_flag = 'N' 
	where a.rule_code = v_rule_code 
	and filter_applied_on='S' 
	and a.delete_flag="N";
	
	-- comparision identifier
	select
		ruleselefilter_gid,
		a.rule_code,
		filter_applied_on,
		filter_field as filter_field_code,
		recon_field_desc as filter_field,
		filter_criteria,
		ident_criteria,
		ident_value,
		case open_parentheses_flag when 'Y' then 'true' else 'false' end as open_parentheses_flag,
		case close_parentheses_flag when 'Y' then 'true' else 'false' end as close_parentheses_flag,
		join_condition,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_truleselefilter a
	inner join recon_mst_trule b on b.rule_code = a.rule_code
		and b.delete_flag = 'N'
	inner join recon_mst_treconfield c on c.recon_field_name = a.filter_field 
		and b.recon_code=c.recon_code
		and c.delete_flag = 'N' 
	where a.rule_code = v_rule_code 
	and filter_applied_on='C' 
	and a.delete_flag="N";

	-- source order
	SELECT rulerecorder_gid,
    a.rule_code,
    recorder_applied_on,
    recorder_seqno,
    recorder_field as recorder_field_code,
    recon_field_desc as recorder_field,
    a.active_status,
    fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	FROM recon_mst_trulerecorder a
	inner join recon_mst_trule b on b.rule_code = a.rule_code
		and b.delete_flag = 'N'
	inner join recon_mst_treconfield c on c.recon_field_name = a.recorder_field 
		and b.recon_code=c.recon_code
		and c.delete_flag = 'N' 
	where a.rule_code = v_rule_code 
	and a.active_status = 'Y' 
	and recorder_applied_on='S' 
	order by recorder_seqno asc;
	
	-- comparsion order
	SELECT rulerecorder_gid,
    a.rule_code,
    recorder_applied_on,
    recorder_seqno,
    recorder_field as recorder_field_code,
    recon_field_desc as recorder_field,
    a.active_status,
    fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	FROM recon_mst_trulerecorder a
	inner join recon_mst_trule b on b.rule_code = a.rule_code
		and b.delete_flag = 'N'
	inner join recon_mst_treconfield c on c.recon_field_name = a.recorder_field 
		and b.recon_code=c.recon_code
		and c.delete_flag = 'N' 
	where a.rule_code = v_rule_code 
	and a.active_status = 'Y' 
	and recorder_applied_on='C' 
	order by recorder_seqno asc;
END $$

DELIMITER ;