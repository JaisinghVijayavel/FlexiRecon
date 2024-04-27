DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_preprocessfetch` $$
CREATE PROCEDURE `pr_get_preprocessfetch`
(
	in in_preprocess_code varchar(32), 
	in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
	select 
		preprocess_gid,
		preprocess_code,
		preprocess_desc,
		a.recon_code,
		b.recon_name,
		process_method,
		fn_get_mastername(a.process_method, 'QCD_PROCESSM') as process_method_desc,
		a.active_status, 
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
		lookup_dataset_code,
		lookup_return_field,
		preprocess_order,
		process_query,
		process_function,
		get_recon_field,
		set_recon_field,
		a.hold_flag, 
		fn_get_mastername(a.hold_flag, 'QCD_YN') as hold_flag_desc
	from recon_mst_tpreprocess  a 
	inner join recon_mst_trecon b on a.recon_code=b.recon_code 
		and b.delete_flag = 'N' 
	where a.preprocess_code=in_preprocess_code
	and a.delete_flag = 'N';

	SELECT 
	preprocessfilter_gid,a.preprocess_code,
	ifnull(c.recon_field_desc,e.field_alias_name) as filter_field,filter_field as filter_field_code,
	filter_criteria,
	filter_value as ident_value,filter_seqno,
	open_parentheses_flag,
	close_parentheses_flag,
	if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition	
	FROM recon_mst_tpreprocessfilter a
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
		and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_code = b.recon_code  
		and a.filter_field = c.recon_field_name
		and c.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.filter_field=e.field_name 
		and e.delete_flag = 'N'
	where a.preprocess_code=in_preprocess_code 
	and a.active_status ='Y'
	and a.delete_flag = 'N';

	SELECT 
	preprocesscondition_gid,a.preprocess_code,condition_seqno,
	recon_field as source_field_code,
	ifnull(c.recon_field_desc,e.field_alias_name) as source_field_desc,
	 extraction_criteria,extraction_filter,lookup_field as comparison_field,
	-- ifnull(d.recon_field_desc,f.field_alias_name) as comparison_field_desc,
	ifnull(f.field_name,d.recon_field_desc) as comparison_field_desc,
	 comparison_criteria,
		 comparison_filter,
	open_parentheses_flag,
	close_parentheses_flag,  if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition
	FROM recon_mst_tpreprocesscondition a 
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
		and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_code = b.recon_code  
		and a.recon_field = c.recon_field_name
		and c.delete_flag = 'N' 
	left join recon_mst_treconfield d on d.recon_code = b.recon_code  
		and a.lookup_field = d.recon_field_name
		and d.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.recon_field = e.field_name 
		and e.delete_flag = 'N'
	left join recon_mst_tdatasetfield f on a.lookup_field = f.dataset_table_field 
		and b.lookup_dataset_code = f.dataset_code
		and f.delete_flag = 'N' 
	where a.preprocess_code=in_preprocess_code 
	and a.active_status ='Y'
	and a.delete_flag = 'N';
END $$

DELIMITER ;