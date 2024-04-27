DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_themefield` $$
CREATE PROCEDURE `pr_get_themefield`
(
	in in_theme_code varchar(32)
)
BEGIN

	-- Header
	select 
		theme_gid,
		theme_code,
		theme_desc as theme_name,
		recon_code,
		theme_order,
		ifnull(theme_type_code,'') as theme_type_code,
		source_dataset_code,
		comparison_dataset_code,
		active_status, 
		fn_get_mastername(active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_ttheme 
	where theme_code = in_theme_code 
	and delete_flag = 'N';

	-- theme condition
	select
		themecondition_gid,
		a.theme_code,
		a.themecondition_seqno,
		a.source_field as source_field_code,
		ifnull(c.recon_field_desc,e.field_alias_name) as source_field_desc,
		a.comparison_field,
		ifnull(d.recon_field_desc,f.field_alias_name) as comparison_field_desc,
		extraction_criteria,
		comparison_criteria,
		open_parentheses_flag,
		close_parentheses_flag,
		if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_tthemecondition a
	inner join recon_mst_ttheme b on b.theme_code = a.theme_code
		and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_field_name = a.source_field 
		and b.recon_code = c.recon_code
		and c.delete_flag = 'N' 
	left join recon_mst_treconfield d on d.recon_field_name = a.comparison_field 
		and b.recon_code = d.recon_code
		and d.delete_flag = 'N' 
  left join recon_mst_tfieldstru e on a.source_field = e.field_name 
		and e.delete_flag = 'N'
  left join recon_mst_tfieldstru f on a.comparison_field = f.field_name 
		and f.delete_flag = 'N'
	where a.theme_code = in_theme_code 
	and a.active_status = 'Y' 
	and a.delete_flag='N'
	order by a.themecondition_seqno;
	
    -- source identifier
	select
		a.themefilter_gid,
		a.theme_code,
    themefilter_seqno,
		filter_applied_on,
		filter_field as filter_field_code,
		ifnull(c.recon_field_desc,e.field_alias_name) as filter_field,
		filter_criteria,		
		filter_value,
		open_parentheses_flag,
		close_parentheses_flag,
		if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_tthemefilter a
	inner join recon_mst_ttheme b on b.theme_code = a.theme_code
		and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_field_name = a.filter_field 
		and b.recon_code=c.recon_code
		and c.delete_flag = 'N' 
  left join recon_mst_tfieldstru e on a.filter_field=e.field_name 
		and e.delete_flag = 'N'
	where a.theme_code = in_theme_code 
	and filter_applied_on='S' 
	and a.delete_flag='N'
	order by a.themefilter_seqno;
    
  -- comparision identifier
	select
		a.themefilter_gid,
		a.theme_code,
		filter_applied_on,
    themefilter_seqno,
		filter_field as filter_field_code,
		ifnull(c.recon_field_desc,e.field_alias_name) as filter_field,
		filter_criteria,		
		filter_value,
		open_parentheses_flag,
		close_parentheses_flag,
		if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_tthemefilter a
	inner join recon_mst_ttheme b on b.theme_code = a.theme_code
		and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_field_name = a.filter_field 
		and b.recon_code=c.recon_code
		and c.delete_flag = 'N' 
  left join recon_mst_tfieldstru e on a.filter_field=e.field_name 
		and e.delete_flag = 'N'
	where a.theme_code = in_theme_code 
	and filter_applied_on = 'C' 
	and a.delete_flag = 'N'
	order by themefilter_seqno;
    
  -- grouping
	select	
		a.themegrpfield_gid,
    a.grpfield_seqno,
		ifnull(c.recon_field_desc,e.field_alias_name) as grp_field,
		a.grp_field  as grp_field_code,
    a.grpfield_applied_on as grpfield_applied_on_code,
    fn_get_mastername(a.grpfield_applied_on, 'QCD_Appiled_on') as grpfield_applied_on,
		a.active_status,
		case a.active_status 
			when 'Y' then 'Active' 
			else 'Inactive' 
		end as active_status_desc
	from recon_mst_tthemegrpfield a
	inner join recon_mst_ttheme b on b.theme_code = a.theme_code
		and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on b.recon_code = c.recon_code 
		and a.grp_field=c.recon_field_name
		and c.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.grp_field = e.field_name 
		and e.delete_flag = 'N'
	where a.theme_code = in_theme_code 
	and a.active_status = 'Y' 
	and a.delete_flag = 'N'
	order by a.grpfield_seqno;
    
  select 
		themeaggfield_gid,
		themeaggfield_seqno,
		themeaggfield_applied_on as themeaggfield_applied_on_code,
		fn_get_mastername(a.themeaggfield_applied_on, 'QCD_Appiled_on') as themeaggfield_applied_on,
		themeaggfield_name,
		ifnull(c.recon_field_desc,e.field_alias_name) as recon_field,
		a.recon_field as recon_field_code,
		themeagg_function,
		themeagg_field_type
	from recon_mst_tthemeaggfield a
	inner join recon_mst_ttheme b on b.theme_code = a.theme_code
		and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on b.recon_code = c.recon_code 
		and a.recon_field = c.recon_field_name
		and c.delete_flag = 'N' 
  left join recon_mst_tfieldstru e on a.recon_field=e.field_name 
		and e.delete_flag = 'N'
	where a.theme_code = in_theme_code 
	and a.active_status = 'Y' 
	and a.delete_flag = 'N'
	order by a.themeaggfield_seqno;
    
  select * from recon_mst_tthemeaggcondition
	where theme_code = in_theme_code 
	and active_status = 'Y' 
	and delete_flag = 'N'
	order by themeaggcondition_seqno;
END $$

DELIMITER ;