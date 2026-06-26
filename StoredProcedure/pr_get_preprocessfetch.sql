DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_preprocessfetch` $$
CREATE PROCEDURE `pr_get_preprocessfetch`(
	in in_preprocess_code varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
  /*
    Created By : vinoth
    Created Date :

    Updated By : Vijayavel
    Updated Date : 15-06-2026

    Version : 2
  */

	declare v_processmethod text default '';
	
	set v_processmethod = (select process_method from recon_mst_tpreprocess 
												 where preprocess_code = in_preprocess_code 
												 and delete_flag='N');
												 
	if ifnull(in_preprocess_code,'') != '' then
		select 
			preprocess_gid,
			preprocess_code,
			preprocess_desc,
			a.recon_code,
			b.recon_name,
			process_method,
			process_expression,
			fn_get_mastername(a.process_method, 'QCD_PROCESSM') as process_method_desc,
			a.active_status, 
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
			lookup_dataset_code,
			lookup_multi_return_flag,
			lookup_return_field,
			lookup_group_flag,
			postprocess_flag,
			fn_get_mastername(a.postprocess_flag, 'QCD_YN') as postprocess_flag_desc,
			cumulative_flag,
			fn_get_mastername(a.cumulative_flag, 'QCD_YN') as cumulative_flag_desc,
			preprocess_order,
			process_query,
			process_function,
			get_recon_field,
			set_recon_field,
			a.hold_flag, 
			fn_get_mastername(a.hold_flag, 'QCD_YN') as hold_flag_desc,
			opening_flag,
			fn_get_mastername(a.opening_flag, 'QCD_YN') as opening_flag_desc,
			agg_flag,
			fn_get_mastername(a.agg_flag, 'QCD_YN') as agg_flag_desc,
			group_flag,
			fn_get_mastername(a.group_flag, 'QCD_YN') as group_flag_desc,
			ifnull(source_dataset_code,'') as source_dataset_code,
			ifnull(comparison_dataset_code,'') as comparison_dataset_code,
			recorderby_type
		from recon_mst_tpreprocess  a 
		inner join recon_mst_trecon b on a.recon_code=b.recon_code and b.delete_flag = 'N' 
		where a.preprocess_code=in_preprocess_code and a.delete_flag = 'N';
	else
	  select
			0 as preprocess_gid,
			'' as preprocess_code,
			'' as preprocess_desc,
			'' as recon_code,
			'' as recon_name,
			'' as process_method,
			'' as process_expression,
			'' as  process_method_desc,
			'' as active_status, 
			'' as active_status_desc,
			'' as lookup_dataset_code,
			'' as lookup_multi_return_flag,
			'' as lookup_return_field,
			'' as lookup_group_flag,
			'' as postprocess_flag,
			'' as postprocess_flag_desc,
			'' as cumulative_flag,
			'' as cumulative_flag_desc,
			0 as preprocess_order,
			'' as process_query,
			'' as process_function,
			'' as get_recon_field,
			'' as set_recon_field,
			'N' as hold_flag, 
			'' as hold_flag_desc,
			'' as opening_flag,
			'' as opening_flag_desc,
			'' as agg_flag,
			'' as agg_flag_desc,
			'' as group_flag,
			'' as group_flag_desc,
			'' as source_dataset_code,
			'' as comparison_dataset_code,
			'' as recorderby_type;
	end if;

	SELECT 
		preprocessfilter_gid,
		a.preprocess_code,
		ifnull(c.recon_field_desc,e.field_alias_name) as filter_field,filter_field as filter_field_code,
		filter_criteria,
		filter_value as ident_value_code,
		ifnull(filter_value_flag,'N') as ident_value_flag,
		case when ( filter_value_flag ='N' )then ifnull(v.recon_field_desc,f.field_alias_name) else filter_value end  as ident_value,
		filter_seqno,
		open_parentheses_flag,
		close_parentheses_flag,
		if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition	
	FROM recon_mst_tpreprocessfilter a
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_code = b.recon_code and a.filter_field = c.recon_field_name	and c.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.filter_field=e.field_name	and e.delete_flag = 'N'
	left join recon_mst_treconfield v on v.recon_field_name = a.filter_value and b.recon_code=v.recon_code and v.delete_flag = 'N' 
	left join recon_mst_tfieldstru f on a.filter_value=f.field_name and f.delete_flag = 'N'
	where a.preprocess_code=in_preprocess_code and a.filter_applied_on='RECON'
	and a.active_status ='Y'
	and a.delete_flag = 'N';

	SELECT 
		preprocesscondition_gid,
		a.preprocess_code,
		condition_seqno,
		recon_field as source_field_code,
		ifnull(c.recon_field_desc,e.field_alias_name) as source_field_desc,
		extraction_criteria,extraction_filter,lookup_field as comparison_field,
        source_field_type,
		ifnull(f.field_name,d.recon_field_desc) as comparison_field_desc,
		comparison_criteria,
		comparison_filter,
		open_parentheses_flag,
		close_parentheses_flag,  
		if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition
	FROM recon_mst_tpreprocesscondition a 
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_code = b.recon_code and a.recon_field = c.recon_field_name	and c.delete_flag = 'N' 
	left join recon_mst_treconfield d on d.recon_code = b.recon_code and a.lookup_field = d.recon_field_name and d.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.recon_field = e.field_name and e.delete_flag = 'N'
	left join recon_mst_tdatasetfield f 
		on a.lookup_field = f.dataset_table_field 
		and (b.lookup_dataset_code = f.dataset_code 
		or f.dataset_code = 'system')
		and f.delete_flag = 'N'
	where a.preprocess_code=in_preprocess_code and a.source_field_type='RECON'
	and a.active_status ='Y'
	and a.delete_flag = 'N';

	select 
		preprocesslookup_gid,
		lookup_seqno,
		a.lookup_return_field,
		case when (ifnull(value_flag,'N') ='N' ) OR (ifnull(reverse_update_flag,'N') ='Y' ) then ifnull(c.field_name,'') else a.lookup_return_field end as lookup_return_field_desc,
		a.set_recon_field,
		case when ((ifnull(reverse_update_flag,'N') ='N' ) OR ifnull(value_flag,'N') ='N' ) then ifnull(d.recon_field_desc,e.field_alias_name) else a.set_recon_field end as set_recon_field_desc,
		reverse_update_flag,
		value_flag,
		a.active_status
	from recon_mst_tpreprocesslookup a
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_tdatasetfield c 
		on a.lookup_return_field = c.dataset_table_field 
		and (b.lookup_dataset_code=c.dataset_code 
		or c.dataset_code = 'system') 
		and c.delete_flag = 'N' 
	left join recon_mst_treconfield d on d.recon_code = b.recon_code and a.set_recon_field = d.recon_field_name and d.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.set_recon_field = e.field_name and e.delete_flag = 'N'
	where a.preprocess_code=in_preprocess_code 
	and a.active_status ='Y'
	and a.delete_flag = 'N';
 
	SELECT 
		preprocessfilter_gid,
		a.preprocess_code,
		ifnull(c.field_name,'') as filter_field,
		filter_field as filter_field_code,
		filter_criteria,
		filter_value as ident_value_code,
		ifnull(filter_value_flag,'N') as ident_value_flag,
		case when ( filter_value_flag ='N' )then ifnull(d.field_name,'') else filter_value end  as ident_value,
		filter_seqno,
		open_parentheses_flag,
		close_parentheses_flag,
		if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition	
	FROM recon_mst_tpreprocessfilter a
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_tdatasetfield c 
		on a.filter_field = c.dataset_table_field 
		and (b.lookup_dataset_code=c.dataset_code 
		or c.dataset_code = 'system')
		and c.delete_flag = 'N' 
	left join recon_mst_tdatasetfield d 
		on a.filter_value = d.dataset_table_field 
		and (b.lookup_dataset_code=d.dataset_code 
		or d.dataset_code = 'system')
		and c.delete_flag = 'N' 
	where a.preprocess_code=in_preprocess_code and a.filter_applied_on='LOOKUP'
	and a.active_status ='Y'
	and a.delete_flag = 'N';
       
	SELECT 
		preprocesscondition_gid,
		a.preprocess_code,
		condition_seqno,
		recon_field as source_field_code,
		ifnull(c.recon_field_desc,e.field_alias_name) as source_field_desc,
		extraction_criteria,extraction_filter,lookup_field as comparison_field,
        source_field_type,
		ifnull(f.field_name,d.recon_field_desc) as comparison_field_desc,
		comparison_criteria,
		comparison_filter,
		open_parentheses_flag,
		close_parentheses_flag,  
		if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition
	FROM recon_mst_tpreprocesscondition a 
	left join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_code = b.recon_code and a.recon_field = c.recon_field_name	and c.delete_flag = 'N' 
	left join recon_mst_treconfield d on d.recon_code = b.recon_code and a.lookup_field = d.recon_field_name and d.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.recon_field = e.field_name and e.delete_flag = 'N'
	left join recon_mst_tdatasetfield f 
		on a.lookup_field = f.dataset_table_field 
		and (b.lookup_dataset_code = f.dataset_code 
		or f.dataset_code = 'system')
		and f.delete_flag = 'N' 
	where a.preprocess_code=in_preprocess_code and a.source_field_type='LOOKUP'
	and a.active_status ='Y'
	and a.delete_flag = 'N';
      
	SELECT 
		preprocessrecorder_gid,
		a.preprocess_code,
		recorder_seqno,
		recorder_field as recorder_field_code,
		ifnull(c.recon_field_desc,e.field_alias_name) as recorder_field,
		a.active_status
	FROM recon_mst_tpreprocessrecorder a
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_field_name = a.recorder_field and b.recon_code=c.recon_code and c.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.recorder_field=e.field_name and e.delete_flag = 'N'    
	where a.preprocess_code=in_preprocess_code and a.active_status ='Y'	and a.delete_flag = 'N' and a.recorder_on = 'RECON';
  
	SELECT 
		preprocessgrpfield_gid,
		a.preprocess_code,
		grpfield_seqno,
		grp_field as grp_field_code,
		ifnull(c.recon_field_desc,e.field_alias_name) as grp_field,
		a.active_status
	FROM recon_mst_tpreprocessgrpfield a
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_field_name = a.grp_field and b.recon_code=c.recon_code and c.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.grp_field=e.field_name and e.delete_flag = 'N'    
	where a.preprocess_code=in_preprocess_code 
	and a.grpfield_on = 'RECON'
	and a.active_status ='Y'	
	and a.delete_flag = 'N';
    
	if v_processmethod != 'QCD_LOOKUP_COMPARISON' then
		SELECT 
			preprocesscondition_gid,
			a.preprocess_code,
			condition_seqno,
			source_field as source_field_code,
			-- ifnull(c.recon_field_desc,e.field_alias_name) as source_field_desc,
			concat(ifnull(c.recon_field_desc,e.field_alias_name),' - ', ifnull(h.field_name,'ummapped')) as source_field_desc,
			extraction_criteria,
			extraction_filter,
			comparison_field as comparison_field,
			source_field_type,
			-- ifnull(f.field_name,d.recon_field_desc) as comparison_field_desc,
			concat(ifnull(d.recon_field_desc,e.field_alias_name),' - ', ifnull(j.field_name,'ummapped')) as comparison_field_desc,
			comparison_criteria,
			comparison_filter,
			open_parentheses_flag,
			close_parentheses_flag,  
			if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition
		FROM recon_mst_tpreprocesscondition a 
		inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
		left join recon_mst_treconfield c on c.recon_code = b.recon_code and a.source_field = c.recon_field_name	and c.delete_flag = 'N' 
		left join recon_mst_treconfield d on d.recon_code = b.recon_code and a.comparison_field = d.recon_field_name and d.delete_flag = 'N' 
		left join recon_mst_tfieldstru e on a.recon_field = e.field_name and e.delete_flag = 'N'
		left join recon_mst_tdatasetfield f 
			on a.lookup_field = f.dataset_table_field 
			and (b.lookup_dataset_code = f.dataset_code
			or f.dataset_code = 'system')
			and f.delete_flag = 'N'
		left join recon_mst_treconfieldmapping g on c.recon_field_name=g.recon_field_name and g.recon_code = c.recon_code and g.delete_flag = 'N'and g.dataset_code=b.source_dataset_code
		left join recon_mst_tdatasetfield h 
			on h.dataset_table_field=g.dataset_field_name 
			and (h.dataset_code = g.dataset_code 
			or g.dataset_code = 'system') 
			and h.delete_flag = 'N'
		left join recon_mst_treconfieldmapping i on d.recon_field_name=i.recon_field_name and i.recon_code = d.recon_code and i.delete_flag = 'N'and i.dataset_code=b.comparison_dataset_code
		left join recon_mst_tdatasetfield j 
			on j.dataset_table_field=i.dataset_field_name 
			and (j.dataset_code = i.dataset_code 
			or j.dataset_code = 'system') 
			and j.delete_flag = 'N'
		where a.preprocess_code=in_preprocess_code and a.source_field_type='COMPARISON'	and a.active_status ='Y'
		and a.delete_flag = 'N';
	else 
		SELECT 
			preprocesscondition_gid,
			a.preprocess_code,
			condition_seqno,
			source_field as source_field_code,		
			ifnull(f.field_name,'') as source_field_desc,
			extraction_criteria,
			extraction_filter,
			comparison_field as comparison_field,
			source_field_type,
			-- ifnull(f.field_name,d.recon_field_desc) as comparison_field_desc,
			ifnull(h.field_name,'') as comparison_field_desc,
			comparison_criteria,
			comparison_filter,
			open_parentheses_flag,
			close_parentheses_flag,  
			if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition
		FROM recon_mst_tpreprocesscondition a 
		inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 	
		left join recon_mst_tdatasetfield f 
			on a.source_field = f.dataset_table_field 
			and (b.source_dataset_code = f.dataset_code 
			or f.dataset_code = 'system') 
			and f.delete_flag = 'N'     
		left join recon_mst_tdatasetfield h 
			on h.dataset_table_field=a.comparison_field 
			and (h.dataset_code = b.comparison_dataset_code 
			or h.dataset_code = 'system') 
			and h.delete_flag = 'N'    
		where a.preprocess_code = in_preprocess_code and a.source_field_type='COMPARISON' and a.active_status ='Y'
		and a.delete_flag = 'N';
	end if;
    
	if v_processmethod != 'QCD_LOOKUP_COMPARISON' then
		SELECT 
			preprocessfilter_gid,
			a.preprocess_code,
			-- ifnull(c.recon_field_desc,e.field_alias_name) as filter_field,
			concat(ifnull(c.recon_field_desc,e.field_alias_name),' - ', ifnull(h.field_name,'ummapped')) as filter_field,
			filter_field as filter_field_code,
			filter_criteria,
			filter_value as filter_value_code,
			ifnull(filter_value_flag,'N') as filter_value_flag,
			case when ( filter_value_flag ='N' )then
			-- ifnull(v.recon_field_desc,f.field_alias_name) 
			concat(ifnull(v.recon_field_desc,f.field_alias_name),' - ', ifnull(j.field_name,'ummapped')) 
			else filter_value end  as filter_value,
			filter_seqno,
			open_parentheses_flag,
			close_parentheses_flag,
			if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition	
		FROM recon_mst_tpreprocessfilter a
		inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
		left join recon_mst_treconfield c on c.recon_code = b.recon_code and a.filter_field = c.recon_field_name	and c.delete_flag = 'N' 
		left join recon_mst_tfieldstru e on a.filter_field=e.field_name	and e.delete_flag = 'N'
		left join recon_mst_treconfield v on v.recon_field_name = a.filter_value and b.recon_code=v.recon_code and v.delete_flag = 'N' 
		left join recon_mst_tfieldstru f on a.filter_value=f.field_name and f.delete_flag = 'N'
		left join recon_mst_treconfieldmapping g on c.recon_field_name=g.recon_field_name and g.recon_code = c.recon_code and g.delete_flag = 'N'and g.dataset_code=b.source_dataset_code
		left join recon_mst_tdatasetfield h 
			on h.dataset_table_field=g.dataset_field_name 
			and (h.dataset_code = g.dataset_code 
			or h.dataset_code = 'system') 
			and h.delete_flag = 'N'
		left join recon_mst_treconfieldmapping i on v.recon_field_name=i.recon_field_name and i.recon_code = v.recon_code and i.delete_flag = 'N'and i.dataset_code=b.source_dataset_code
		left join recon_mst_tdatasetfield j 
			on j.dataset_table_field=i.dataset_field_name 
			and (j.dataset_code = i.dataset_code 
			or j.dataset_code = 'system') 
			and j.delete_flag = 'N'
		where a.preprocess_code=in_preprocess_code and a.filter_applied_on='SOURCE'
		and a.active_status ='Y' and a.delete_flag = 'N';
	else 
		SELECT 
			preprocessfilter_gid,
			a.preprocess_code,
			-- ifnull(c.recon_field_desc,e.field_alias_name) as filter_field,
			ifnull(h.field_name,'') as filter_field,
			filter_field as filter_field_code,
			filter_criteria,
			filter_value as filter_value_code,
			ifnull(filter_value_flag,'N') as filter_value_flag,
			case when ( filter_value_flag ='N' )then
			ifnull(h.field_name,'')
			else filter_value end  as filter_value,
			filter_seqno,
			open_parentheses_flag,
			close_parentheses_flag,
			if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition	
		FROM recon_mst_tpreprocessfilter a
		inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 	
		left join recon_mst_tdatasetfield h 
			on h.dataset_table_field=a.filter_field 
			and (h.dataset_code = b.source_dataset_code 
			or h.dataset_code = 'system') 
			and h.delete_flag = 'N'    
		where a.preprocess_code=in_preprocess_code and a.filter_applied_on='SOURCE'
		and a.active_status ='Y' and a.delete_flag = 'N';
	end if;
    
	if v_processmethod != 'QCD_LOOKUP_COMPARISON' then
		SELECT 
			preprocessfilter_gid,
			a.preprocess_code,
			-- ifnull(c.recon_field_desc,e.field_alias_name) as filter_field,
			concat(ifnull(c.recon_field_desc,e.field_alias_name),' - ', ifnull(h.field_name,'ummapped')) as filter_field,
			filter_field as filter_field_code,
			filter_criteria,
			filter_value as filter_value_code,
			ifnull(filter_value_flag,'N') as filter_value_flag,
			-- case when ( filter_value_flag ='N' )then ifnull(v.recon_field_desc,f.field_alias_name) else filter_value end  as filter_value,
			case when ( filter_value_flag ='N' )then
			-- ifnull(v.recon_field_desc,f.field_alias_name) 
			concat(ifnull(v.recon_field_desc,f.field_alias_name),' - ', ifnull(j.field_name,'ummapped')) 
			else filter_value end  as filter_value,
			filter_seqno,
			open_parentheses_flag,
			close_parentheses_flag,
			if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition	
		FROM recon_mst_tpreprocessfilter a
		inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
		left join recon_mst_treconfield c on c.recon_code = b.recon_code and a.filter_field = c.recon_field_name	and c.delete_flag = 'N' 
		left join recon_mst_tfieldstru e on a.filter_field=e.field_name	and e.delete_flag = 'N'
		left join recon_mst_treconfield v on v.recon_field_name = a.filter_value and b.recon_code=v.recon_code and v.delete_flag = 'N' 
		left join recon_mst_tfieldstru f on a.filter_value=f.field_name and f.delete_flag = 'N'
		left join recon_mst_treconfieldmapping g on c.recon_field_name=g.recon_field_name and g.recon_code = c.recon_code and g.delete_flag = 'N'and g.dataset_code=b.comparison_dataset_code
		left join recon_mst_tdatasetfield h 
			on h.dataset_table_field=g.dataset_field_name 
			and (h.dataset_code = g.dataset_code 
			or h.dataset_code = 'system') 
			and h.delete_flag = 'N'
		left join recon_mst_treconfieldmapping i on v.recon_field_name=i.recon_field_name and i.recon_code = v.recon_code and i.delete_flag = 'N'and i.dataset_code=b.comparison_dataset_code
		left join recon_mst_tdatasetfield j 
			on j.dataset_table_field = i.dataset_field_name 
			and (j.dataset_code = i.dataset_code 
			or j.dataset_code = 'system') 
			and j.delete_flag = 'N'
		where a.preprocess_code=in_preprocess_code and a.filter_applied_on='COMPARISON'
		and a.active_status ='Y' and a.delete_flag = 'N';
	else
		SELECT 
			preprocessfilter_gid,
			a.preprocess_code,
			-- ifnull(c.recon_field_desc,e.field_alias_name) as filter_field,
			ifnull(h.field_name,'') as filter_field,
			filter_field as filter_field_code,
			filter_criteria,
			filter_value as filter_value_code,
			ifnull(filter_value_flag,'N') as filter_value_flag,
			case when ( filter_value_flag ='N' )then
			ifnull(h.field_name,'')
			else filter_value end  as filter_value,
			filter_seqno,
			open_parentheses_flag,
			close_parentheses_flag,
			if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition	
		FROM recon_mst_tpreprocessfilter a
		inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 	
		left join recon_mst_tdatasetfield h 
			on h.dataset_table_field=a.filter_field 
			and (h.dataset_code = b.comparison_dataset_code 
			or h.dataset_code = 'system') 
			and h.delete_flag = 'N'    
		where a.preprocess_code = in_preprocess_code and a.filter_applied_on='COMPARISON'
		and a.active_status ='Y' and a.delete_flag = 'N';
	end if;
    
	 SELECT 
		preprocessgrpfield_gid,
		a.preprocess_code,
		grpfield_seqno,
		grp_field as grp_field_code,
		ifnull(c.field_name,'')  as grp_field,
		a.active_status
	FROM recon_mst_tpreprocessgrpfield a
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N'   
	left join recon_mst_tdatasetfield c 
		on a.grp_field = c.dataset_table_field 
		and (b.lookup_dataset_code=c.dataset_code 
		or c.dataset_code = 'system') 
		and c.delete_flag = 'N' 	
	where a.preprocess_code=in_preprocess_code 
	and a.grpfield_on = 'LOOKUP'
	and a.active_status ='Y'	
	and a.delete_flag = 'N';
    
	SELECT 
		preprocessrecorder_gid,
		a.preprocess_code,
		recorder_seqno,
		recorder_field as recorder_field_code,
		ifnull(c.field_name,'')  as recorder_field,
		a.active_status
	FROM recon_mst_tpreprocessrecorder a
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_tdatasetfield c 
		on a.recorder_field = c.dataset_table_field 
		and (b.lookup_dataset_code=c.dataset_code 
		or c.dataset_code = 'system') 
		and c.delete_flag = 'N' 	  
	where a.preprocess_code=in_preprocess_code 
	and a.recorder_on = 'LOOKUP'
	and a.active_status ='Y'	
	and a.delete_flag = 'N';
    
	SELECT 
		preprocessexp_gid,
		a.preprocess_code,
		preprocessexp_on,
		preprocessexp_sno,
		preprocessexp_update_field as preprocessexp_update_field_code,
		ifnull(c.recon_field_desc,e.field_alias_name)  as preprocessexp_update_field,
		preprocess_expression
	FROM recon_mst_tpreprocessexp a    
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_treconfield c on c.recon_field_name = a.preprocessexp_update_field and b.recon_code=c.recon_code and c.delete_flag = 'N' 
	left join recon_mst_tfieldstru e on a.preprocessexp_update_field = e.field_name and e.delete_flag = 'N' 
	where a.preprocess_code=in_preprocess_code 
	and preprocessexp_on = 'RECON' 
	and a.active_status='Y' 
	and a.delete_flag = 'N' 
	order by preprocessexp_sno ;
    
	SELECT 
		preprocessexp_gid,
		a.preprocess_code,
		preprocessexp_on,
		preprocessexp_sno,
		preprocessexp_update_field as preprocessexp_update_field_code,
		ifnull(c.field_name,'')  as preprocessexp_update_field,
		preprocess_expression
	FROM recon_mst_tpreprocessexp a    
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_tdatasetfield c 
		on a.preprocessexp_update_field = c.dataset_table_field 
		and (b.lookup_dataset_code = c.dataset_code 
		or c.dataset_code = 'system') 
		and c.delete_flag = 'N' 
	where a.preprocess_code = in_preprocess_code 
	and preprocessexp_on = 'LOOKUP' 
	and a.active_status='Y'
	and a.delete_flag = 'N'; 

	SELECT 
		preprocessaggfield_gid,
		a.preprocess_code,
		preprocessaggfield_seqno,
		fn_get_mastername(a.preprocessfield_applied_on, 'QCD_Appiled_on') as preprocessfield_applied_on,
		preprocessaggfield_name,
		ifnull(c.recon_field_desc,e.field_alias_name) as recon_field,
		a.recon_field as recon_field_code,
		preprocessagg_function,
		preprocessagg_field,
		preprocessagg_field_sno,
		preprocessagg_field_type
	FROM recon_mst_tpreprocessaggfield a
	inner join recon_mst_tpreprocess b on b.preprocess_code = a.preprocess_code and b.delete_flag="N"
	left join recon_mst_treconfield c on b.recon_code=c.recon_code and a.recon_field=c.recon_field_name and c.delete_flag="N"
	left join recon_mst_tfieldstru e on a.recon_field=e.field_name and e.delete_flag = 'N'
	where a.preprocess_code = in_preprocess_code 
	and a.active_status = 'Y' 
	and a.delete_flag='N' 
	order by preprocessaggfield_seqno asc;
    
	SELECT 
		preprocessaggcondition_gid,		
		preprocessaggcondition_seqno,
		preprocessagg_applied_on,
		a.preprocessagg_field as preprocessagg_field_code,
		b.preprocessaggfield_name as preprocessagg_field,
		preprocessagg_criteria,
		preprocessagg_value_flag,		
		open_parentheses_flag,
		close_parentheses_flag,
		if(ifnull(join_condition,'') != '',join_condition,'AND') as join_condition,	
        a.preprocessagg_value as preprocessagg_value_code,
        case when ( preprocessagg_value_flag ='N' )
        then  c.preprocessaggfield_name
        else preprocessagg_value end  as preprocessagg_value		
	from recon_mst_tpreprocessaggcondition a
	left join recon_mst_tpreprocessaggfield b on a.preprocessagg_field=b.preprocessagg_field and a.preprocess_code=b.preprocess_code and b.delete_flag="N"
	left join recon_mst_tpreprocessaggfield c on a.preprocessagg_value=c.preprocessagg_field and a.preprocess_code=c.preprocess_code and c.delete_flag="N"
	where a.preprocess_code = in_preprocess_code 
	and a.active_status = 'Y'
	and a.delete_flag = 'N' 
	order by preprocessaggcondition_seqno;
    
	SELECT 
		preprocessdsupdate_gid,
		a.preprocess_code,
		rec_seqno,
		source_field,
       case when (ifnull(value_flag,'N') ='Y' and ifnull(reverse_update_flag,'N') ='N')  then ifnull(c.field_name,'')  
			 when (ifnull(reverse_update_flag,'N') ='Y'and ifnull(value_flag,'N') ='N') then ifnull(c.field_name,'')
              when (ifnull(reverse_update_flag,'N') ='Y' and ifnull(value_flag,'N') ='Y') then a.source_field   
              when (ifnull(reverse_update_flag,'N') ='N' and ifnull(value_flag,'N') ='N') then ifnull(c.field_name,'')
              else a.source_field end as source_field_desc,
		comparison_field,
        case when (ifnull(value_flag,'N') ='N' ) OR (ifnull(reverse_update_flag,'N') ='Y' ) then ifnull(d.field_name,'') else a.comparison_field end as comparison_field_desc,
		reverse_update_flag,
		value_flag,
		a.active_status		
	FROM recon_mst_tpreprocessdsupdate a
	inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N' 
	left join recon_mst_tdatasetfield c 
		on a.source_field = c.dataset_table_field 
		and (b.source_dataset_code=c.dataset_code 
		or c.dataset_code = 'system') 
		and c.delete_flag = 'N' 
	left join recon_mst_tdatasetfield d 
		on a.comparison_field = d.dataset_table_field 
		and (b.comparison_dataset_code = d.dataset_code 
		or d.dataset_code = 'system') 
		and d.delete_flag = 'N' 
	where a.preprocess_code = in_preprocess_code 
	and a.active_status = 'Y' 
	and a.delete_flag = 'N' 
	order by rec_seqno	;
END $$

DELIMITER ;