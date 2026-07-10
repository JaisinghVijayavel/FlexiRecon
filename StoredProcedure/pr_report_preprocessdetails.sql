DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_report_preprocessdetails` $$
CREATE PROCEDURE `pr_report_preprocessdetails`
(
  in in_preprocess_code varchar(32),
  in in_recon_code varchar(32),
  in in_version_code varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  /*
    Created By : Hema
    Created Date :

    Updated By : Vijayavel
    Updated Date : 26-06-2026

    Version : 2
  */

	declare v_lookup_multi_return_flag char(1);
	declare v_process_method text default '';

	select
		process_method,lookup_multi_return_flag
  into
    v_process_method,v_lookup_multi_return_flag
	from recon_mst_tpreprocesshistory
	where recon_code = in_recon_code
	and preprocess_code = in_preprocess_code
  and recon_version = in_version_code
	and active_status = 'Y'
	and delete_flag = 'N';

  /*Header Table 1 */
	select
    preprocess_code,
    preprocess_desc,
		process_method,
    fn_get_mastername(a.process_method, 'QCD_PROCESSM') as process_method_desc,
    CAST(preprocess_order AS DECIMAL(10, 3)) as 'preprocess_order',
		case postprocess_flag when 'Y' then 'YES' else 'NO' end as 'Post Process',
    fn_get_datasetname(source_dataset_code) as 'Source Dataset',
    fn_get_datasetname(comparison_dataset_code) as 'Comparison Dataset',
    fn_get_datasetname(a.lookup_dataset_code) as 'Lookup Dataset',
    process_query,
    a.hold_flag,
    fn_get_mastername(a.hold_flag, 'QCD_YN') as hold_flag_desc
	from recon_mst_tpreprocesshistory  a
  where a.recon_code = in_recon_code
  and a.preprocess_code = in_preprocess_code
  and a.recon_version = in_version_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N'
  order by preprocess_order;
    
	if (v_lookup_multi_return_flag = 'Y') then
	/* Table 2 */
		set @row_number = 0;

    select a.* from(
			select
				0 as 'Id',
				'10f' as 'Seq No',
				'35f' as 'Update Recon Field',
				'35f' as 'Lookup Return Field',
				'10f' as 'Value Flag',
				'10f' as 'Reverse Update Flag'
			union all
			select (@row_number := @row_number + 1) as 'Id', b.* from (
				select
					a.lookup_seqno as 'Seq No',
					ifnull(d.recon_field_desc,e.field_alias_name) as 'Update Recon Field',
					ifnull(c.field_name,a.lookup_return_field) as 'Lookup Return Field',
          a.value_flag as 'Value Flag',
          a.reverse_update_flag as 'Reverse Update Flag'
				from recon_mst_tpreprocesslookuphistory a
				inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
					and b.delete_flag = 'N'
				left join recon_mst_tdatasetfield c on  a.lookup_return_field = c.dataset_table_field
					and b.lookup_dataset_code=c.dataset_code
					and c.delete_flag = 'N'
				left join recon_mst_treconfield d on d.recon_code = b.recon_code
					and a.set_recon_field = d.recon_field_name
					and d.delete_flag = 'N'
				left join recon_mst_tfieldstru e on a.set_recon_field = e.field_name
					and e.delete_flag = 'N'
				where a.preprocess_code=in_preprocess_code
        and a.recon_version = in_version_code
				and a.active_status ='Y'
				and a.delete_flag = 'N'
				order by a.lookup_seqno
			) as b
		) as a order by a.Id;
	else
		set @row_number = 0;

		select a.* from(
			select
				0 as 'Id',
				'50f' as 'Update Recon Field',
				'50f' as 'Lookup Return Field'
			union all
			select (@row_number := @row_number + 1) as 'Id', b.* from (
				select
					ifnull(d.recon_field_desc,e.field_alias_name) as 'Update Recon Field',
					ifnull(c.field_name,a.lookup_return_field) as 'Lookup Return Field'
				from recon_mst_tpreprocesshistory a
				inner join recon_mst_tpreprocesshistory b on a.preprocess_code = b.preprocess_code
          and a.recon_version = b.recon_version
          and b.delete_flag = 'N'
				left join recon_mst_tdatasetfield c on  a.lookup_return_field = c.dataset_table_field and b.lookup_dataset_code=c.dataset_code and c.delete_flag = 'N'
				left join recon_mst_treconfield d on d.recon_code = b.recon_code and a.set_recon_field = d.recon_field_name and d.delete_flag = 'N'
				left join recon_mst_tfieldstru e on a.set_recon_field = e.field_name and e.delete_flag = 'N'
				where a.preprocess_code = in_preprocess_code
        and a.recon_version = in_version_code
				and a.active_status ='Y'
				and a.delete_flag = 'N'
			) as b
		) as a order by a.Id;
	end if;

	/* Table 3 */
	set @row_number = 0;

	select a.* from(
    select
			 0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
			'15f' as 'Recon Field',
			'20f' as 'Extraction Criteria',
			'15f' as 'Lookup Field',
			'20f' as 'Comparision Criteria',
			'5f' as '--',
			'10f' as 'Join'
		union all
		select
			(@row_number := @row_number + 1) as 'Id',b.* from
		(
			select     
				condition_seqno as 'Seq No',
				case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Recon Field',
				extraction_criteria as 'Extraction Criteria',
				ifnull(f.field_name,d.recon_field_desc) as 'Lookup Field',
				comparison_criteria as 'Comparision Criteria',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Join'
			FROM recon_mst_tpreprocessconditionhistory a
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
      and a.recon_version = in_version_code
      and a.source_field_type='RECON'
			and a.active_status ='Y'
			and a.delete_flag = 'N'
			order by a.condition_seqno
		)as b
	) as a order by a.Id;
    
	/* Table 4*/
	set @row_number = 0;
	
	select a.* from
	(
		select 
			0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
			'15f' as 'Lookup Field',
			'20f' as 'Extraction Criteria',
			'15f' as 'Recon Field',
			'20f' as 'Comparision Criteria',
			'5f' as '--',
			'10f' as 'Join'
		union all
    select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select
				condition_seqno as 'Seq No',
        case open_parentheses_flag when 'Y' then '(' else '' end as '-',
        ifnull(f.field_name,d.recon_field_desc) as 'Lookup Field',
        extraction_criteria as 'Extraction Criteria',
        ifnull(c.recon_field_desc,e.field_alias_name) as 'Recon Field',		
				comparison_criteria as 'Comparision Criteria',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Join'
			FROM recon_mst_tpreprocessconditionhistory a
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
      and a.recon_version = in_version_code
			and a.source_field_type='LOOKUP'
			and a.active_status ='Y'
			and a.delete_flag = 'N' order by a.condition_seqno
		) as b
	) as a order by a.Id;
	
	/* Table 5 */
	set @row_number = 0;
	
	select a.* from(
		select 
			0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
			'20f' as 'Filter Field',
			'30f' as 'Filter Criteria',
			'20f' as 'Filter Value',
			'5f' as '--',
			'10f' as 'Join'
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select
				filter_seqno as 'Seq No',
				case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				ifnull(c.field_name,'') as 'Filter Field',
				filter_criteria as 'Filter Criteria',
				case when ( filter_value_flag ='N') then
					ifnull(d.field_name,'')
				else
					filter_value end as 'Filter Value',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Join'
			FROM recon_mst_tpreprocessfilterhistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	and b.delete_flag = 'N'
			left join recon_mst_tdatasetfield c on  a.filter_field = c.dataset_table_field and b.lookup_dataset_code=c.dataset_code and c.delete_flag = 'N'
			left join recon_mst_tdatasetfield d on  a.filter_value = d.dataset_table_field and b.lookup_dataset_code=d.dataset_code and c.delete_flag = 'N'
			where a.preprocess_code=in_preprocess_code
      and a.recon_version = in_version_code
      and a.filter_applied_on in ('LOOKUP')
			and a.active_status ='Y'
			and a.delete_flag = 'N'
			order by a.filter_seqno
		)as b
	) as a order by a.Id;
    
	/* Table 6*/
	set @row_number = 0;
	
	select a.* from(
		select 
			0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
			'20f' as 'Filter Field',
			'30f' as 'Filter Criteria',
			'20f' as 'Filter Value',
			'5f' as '--',
			'10f' as 'Join'
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select
				filter_seqno as 'Seq No',
        case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Filter Field',
				filter_criteria as 'Filter Criteria',
				case when ( filter_value_flag ='N' )then ifnull(v.recon_field_desc,f.field_alias_name) else filter_value end  as 'Filter Value',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Join'
			FROM recon_mst_tpreprocessfilterhistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
				and b.delete_flag = 'N'
			left join recon_mst_treconfield c on c.recon_code = b.recon_code
				and a.filter_field = c.recon_field_name
				and c.delete_flag = 'N'
			left join recon_mst_tfieldstru e on a.filter_field=e.field_name
				and e.delete_flag = 'N'
			left join recon_mst_treconfield v on v.recon_field_name = a.filter_value
				and b.recon_code=v.recon_code
				and v.delete_flag = 'N'
			left join recon_mst_tfieldstru f on a.filter_value=f.field_name
				and f.delete_flag = 'N'
			where a.preprocess_code=in_preprocess_code
      and a.recon_version = in_version_code
			and a.filter_applied_on in ('RECON')
			and a.active_status ='Y'
			and a.delete_flag = 'N'
			order by a.filter_seqno
		) as b
	) as a order by a.Id;

	/*Table 7 */
	set @row_number = 0;
	
	select a.* from(
    select 
			 0 as 'Id',
			'100f' as 'Query'
    union all
    select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select 
				process_query as 'Query'
			from recon_mst_tpreprocesshistory  a
			where a.recon_code = in_recon_code
			and a.preprocess_code = in_preprocess_code
      and a.recon_version = in_version_code
			and a.delete_flag= 'N'
		) as b
	) as a order by a.Id;
    
	/*Table 8*/

	set @row_number = 0;
	
  select a.* from(
    select 
			 0 as 'Id',
			'50f' as 'Update Recon Field',
			'50f' as 'Preprocess Expression'
    union all
    select (@row_number := @row_number + 1) as 'Id', b.* from
		(
			select
				ifnull(d.recon_field_desc,e.field_alias_name) as 'Update Recon Field',
				a.process_expression as 'Preprocess Expression'
			from recon_mst_tpreprocesshistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
        and a.recon_version = b.recon_version
				and b.delete_flag = 'N'
			left join recon_mst_tdatasetfield c on  a.lookup_return_field = c.dataset_table_field
				and b.lookup_dataset_code=c.dataset_code
				and c.delete_flag = 'N'
			left join recon_mst_treconfield d on d.recon_code = b.recon_code
				and a.set_recon_field = d.recon_field_name
				and d.delete_flag = 'N'
			left join recon_mst_tfieldstru e on a.set_recon_field = e.field_name
				and e.delete_flag = 'N'
			where a.preprocess_code =  in_preprocess_code
      and a.recon_version = in_version_code
      and a.set_recon_field <> ''
			and a.active_status ='Y'
			and a.delete_flag = 'N'
		) as b
    union all
    select (@row_number := @row_number + 1) as 'Id', c.* from
		(
			select
        case when a.preprocessexp_on = 'LOOKUP' then ifnull(c.field_name,a.preprocessexp_update_field)
        else ifnull(d.recon_field_desc,e.field_alias_name)
        end as 'Update Recon Field',
				a.preprocess_expression as 'Preprocess Expression'
			from recon_mst_tpreprocessexphistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
				and b.delete_flag = 'N'
			left join recon_mst_tdatasetfield c on  a.preprocessexp_update_field = c.dataset_table_field
				and b.lookup_dataset_code=c.dataset_code
				and c.delete_flag = 'N'
			left join recon_mst_treconfield d on d.recon_code = b.recon_code
				and a.preprocessexp_update_field = d.recon_field_name
				and d.delete_flag = 'N'
			left join recon_mst_tfieldstru e on a.preprocessexp_update_field = e.field_name
				and e.delete_flag = 'N'
			where a.preprocess_code =  in_preprocess_code
      and a.recon_version = in_version_code
			and a.active_status ='Y'
			and a.delete_flag = 'N'
      order by a.preprocessexp_sno
		) as c
	) as a order by a.Id;

	/*Table 9 */
	set @row_number = 0;
	
	select a.* from 
	(
		select 
			 0 as 'Id',
			'50f' as 'Recon Order',
			'50f' as 'Seq No'
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select 
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Recon Order',
				recorder_seqno as 'Seq No'
			FROM recon_mst_tpreprocessrecorderhistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
				and b.delete_flag = 'N'
			left join recon_mst_treconfield c on c.recon_field_name = a.recorder_field
				and b.recon_code=c.recon_code
				and c.delete_flag = 'N'
			left join recon_mst_tfieldstru e on a.recorder_field=e.field_name
				and e.delete_flag = 'N'
			where a.preprocess_code=in_preprocess_code
      and a.recon_version = in_version_code
			and a.active_status ='Y'
			and a.delete_flag = 'N'
		) as b
	) as a order by a.Id;
		
	/*Table 10 */
	set @row_number = 0;
	
	select a.* from 
	(
    select 
			0 as 'Id',
			'10f' as 'Seq No',
			'90f' as 'Group Field' 
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select 
				grpfield_seqno as 'Seq No',
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Group Field'		
			FROM recon_mst_tpreprocessgrpfieldhistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	
				and b.delete_flag = 'N' 
			left join recon_mst_treconfield c on c.recon_field_name = a.grp_field 
				and b.recon_code=c.recon_code 
				and c.delete_flag = 'N' 
			left join recon_mst_tfieldstru e on a.grp_field=e.field_name 
				and e.delete_flag = 'N'    
			where a.preprocess_code=in_preprocess_code 
      and a.recon_version = in_version_code
			and a.active_status ='Y'
			and a.delete_flag = 'N'
			order by a.grpfield_seqno
		)as b
	) as a order by a.Id;
    
	/*Table 11*/
	-- if(v_process_method = 'QCD_FUNCTION') then
  set @row_number = 0;
	
  select a.* from 
	(
    select
			0 as 'Id',
			'30f' as 'Lookup Return Field',
			'40f' as 'Lookup Function',
			'30f' as 'Update Recon Field'
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select 
				b.recon_field_desc as 'Lookup Return Field',
				a.process_function as 'Lookup Function',
				c.recon_field_desc  as 'Update Recon Field'
			from recon_mst_tpreprocesshistory as a
			inner join recon_mst_treconfield as b on a.recon_code = b.recon_code 
				and b.recon_field_name = a.get_recon_field
				and b.delete_flag = 'N'
			inner join recon_mst_treconfield  as c on a.recon_code = c.recon_code 
				and c.recon_field_name = a.set_recon_field
				and c.delete_flag = 'N'
			where a.preprocess_code = in_preprocess_code
      and a.recon_version = in_version_code
			and a.active_status ='Y'
			and a.delete_flag = 'N'
		)as b
	) as a order by a.Id;

	/*Table 12 */  
	-- Lookup Expression
	-- elseif(v_process_method = 'QCD_LOOKUP_EXPRESSION') then
	set @row_number = 0;
	
	select a.* from
	(
		select
			0 as 'Id',
			'30f' as 'Lookup Dataset',
			'10f' as 'Seq No',
			'30f' as 'Lookup Field',
			'30' as 'Expression' 
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select
				fn_get_datasetname(b.lookup_dataset_code) as 'Lookup Dataset',
				preprocessexp_sno as 'Seq No',
				ifnull(c.field_name,'')  as 'Lookup Field',
				preprocess_expression as 'Expression'
				-- from recon_mst_tpreprocessexphistory a
			from recon_mst_tpreprocessexphistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
				and b.delete_flag = 'N'
			left join recon_mst_tdatasetfield c on a.preprocessexp_update_field = c.dataset_table_field
				and b.lookup_dataset_code = c.dataset_code
				and c.delete_flag = 'N'
			where a.preprocess_code = in_preprocess_code
			-- and preprocessexp_on = 'LOOKUP'
      and a.recon_version = in_version_code
			and a.delete_flag= 'N'
			order by a.preprocessexp_sno
		) as b
	) as a order by a.Id;
		
	/*Source Filter Table 13 */  
	set @row_number = 0;
	
	select a.* from
	(
    select 
			0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
			'10f' as 'Filter Field',
			'10f' as 'Filter Criteria',
			'10f' as 'Value',
			'10f' as 'Filter Value',
			'5f' as '--',
			'10f' as 'Joins'	
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
    	select
        filter_seqno as 'Seq No',
        case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				concat(ifnull(c.recon_field_desc,e.field_alias_name),' - ', ifnull(h.field_name,'ummapped')) as 'Filter Field',
				filter_criteria as 'Filter Criteria',
        ifnull(filter_value_flag,'N') as 'Value',
        case when ( filter_value_flag ='N' )then
        concat(ifnull(v.recon_field_desc,f.field_alias_name),' - ', ifnull(j.field_name,'ummapped'))
        else filter_value end  as 'Filter Value',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Joins'
			FROM recon_mst_tpreprocessfilterhistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
				and b.delete_flag = 'N'
			left join recon_mst_treconfield c on c.recon_code = b.recon_code
				and a.filter_field = c.recon_field_name
				and c.delete_flag = 'N'
			left join recon_mst_tfieldstru e on a.filter_field=e.field_name
				and e.delete_flag = 'N'
			left join recon_mst_treconfield v on v.recon_field_name = a.filter_value
				and b.recon_code=v.recon_code
				and v.delete_flag = 'N'
			left join recon_mst_tfieldstru f on a.filter_value=f.field_name
				and f.delete_flag = 'N'
			left join recon_mst_treconfieldmapping g on c.recon_field_name=g.recon_field_name
				and g.recon_code = c.recon_code
				and g.dataset_code=b.source_dataset_code
				and g.delete_flag = 'N'
			left join recon_mst_tdatasetfield h on h.dataset_table_field=g.dataset_field_name
				and h.dataset_code = g.dataset_code
				and h.delete_flag = 'N'
			left join recon_mst_treconfieldmapping i on v.recon_field_name=i.recon_field_name
				and i.recon_code = v.recon_code
				and i.dataset_code=b.source_dataset_code
				and i.delete_flag = 'N'
			left join recon_mst_tdatasetfield j on j.dataset_table_field=i.dataset_field_name
				and j.dataset_code = i.dataset_code
				and j.delete_flag = 'N'
			where a.preprocess_code=in_preprocess_code
      and a.recon_version = in_version_code
			and a.filter_applied_on='SOURCE'
			and a.active_status ='Y'
			and a.delete_flag = 'N'
			order by a.filter_seqno
		)as b
	) as a order by a.Id;
    
	/*comparision Filter Table 14 */  
	set @row_number = 0;
	
	select a.* from
	(
    select 
			0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
			'10f' as 'Filter Field',
			'10f' as 'Filter Criteria',
			'10f' as 'Value',
			'10f' as 'Filter Value',
			'5f' as '--',
			'10f' as 'Joins'	
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
    	select
        filter_seqno as 'Seq No',
        case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				concat(ifnull(c.recon_field_desc,e.field_alias_name),' - ', ifnull(h.field_name,'ummapped')) as 'Filter Field',
				filter_criteria as 'Filter Criteria',
        ifnull(filter_value_flag,'N') as 'Value',
        case when ( filter_value_flag ='N' )then
        concat(ifnull(v.recon_field_desc,f.field_alias_name),' - ', ifnull(j.field_name,'ummapped')) 
        else filter_value end  as 'Filter Value',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Joins'	
			FROM recon_mst_tpreprocessfilterhistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	
				and b.delete_flag = 'N' 
			left join recon_mst_treconfield c on c.recon_code = b.recon_code 
				and a.filter_field = c.recon_field_name	
				and c.delete_flag = 'N' 
			left join recon_mst_tfieldstru e on a.filter_field=e.field_name	
				and e.delete_flag = 'N'
			left join recon_mst_treconfield v on v.recon_field_name = a.filter_value 
				and b.recon_code=v.recon_code 
				and v.delete_flag = 'N' 
			left join recon_mst_tfieldstru f on a.filter_value=f.field_name 
				and f.delete_flag = 'N'
			left join recon_mst_treconfieldmapping g on c.recon_field_name=g.recon_field_name 
				and g.recon_code = c.recon_code 
				and g.dataset_code=b.source_dataset_code
				and g.delete_flag = 'N'
			left join recon_mst_tdatasetfield h on h.dataset_table_field=g.dataset_field_name 
				and h.dataset_code = g.dataset_code 
				and h.delete_flag = 'N'
			left join recon_mst_treconfieldmapping i on v.recon_field_name=i.recon_field_name 
				and i.recon_code = v.recon_code 
				and i.dataset_code=b.source_dataset_code
				and i.delete_flag = 'N'
			left join recon_mst_tdatasetfield j on j.dataset_table_field=i.dataset_field_name 
				and j.dataset_code = i.dataset_code 
				and j.delete_flag = 'N'
			where a.preprocess_code=in_preprocess_code 
      and a.recon_version = in_version_code
			and a.filter_applied_on='COMPARISON'
			and a.active_status ='Y'
			and a.delete_flag = 'N'
			order by filter_seqno
		) as b
	) as a order by a.Id;
				
	/*PreProcess Condition Table 15 */  
  set @row_number = 0;
	
	select a.* from
	(
    select 
			0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
			'10f' as 'Source Field',
			'10f' as 'Extraction Criteria',
			'10f' as 'Comparision Field',
			'10f' as 'Comparision Criteria',
			'5f' as '--',
			'10f' as 'Joins'
    union all
    select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select
				condition_seqno as 'Seq No',
				case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				concat(ifnull(c.recon_field_desc,e.field_alias_name),' - ', ifnull(h.field_name,'ummapped')) as 'Source Field',
				extraction_criteria as 'Extraction Criteria',
				concat(ifnull(d.recon_field_desc,e.field_alias_name),' - ', ifnull(j.field_name,'ummapped')) as 'Comparision Field',
				comparison_criteria as 'Comparision Criteria',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Joins'	
			FROM recon_mst_tpreprocessconditionhistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	
				and b.delete_flag = 'N' 
			left join recon_mst_treconfield c on c.recon_code = b.recon_code 
				and a.source_field = c.recon_field_name	
				and c.delete_flag = 'N' 
			left join recon_mst_treconfield d on d.recon_code = b.recon_code 
				and a.comparison_field = d.recon_field_name 
				and d.delete_flag = 'N' 
			left join recon_mst_tfieldstru e on a.recon_field = e.field_name 
				and e.delete_flag = 'N'
			left join recon_mst_tdatasetfield f on a.lookup_field = f.dataset_table_field 
				and b.lookup_dataset_code = f.dataset_code 
				and f.delete_flag = 'N' 
			left join recon_mst_treconfieldmapping g on c.recon_field_name=g.recon_field_name 
				and g.recon_code = c.recon_code 
				and g.dataset_code=b.source_dataset_code
				and g.delete_flag = 'N' 
			left join recon_mst_tdatasetfield h on h.dataset_table_field=g.dataset_field_name 
				and h.dataset_code = g.dataset_code 
				and h.delete_flag = 'N'
			left join recon_mst_treconfieldmapping i on d.recon_field_name=i.recon_field_name 
				and i.recon_code = d.recon_code 
				and i.dataset_code=b.comparison_dataset_code
				and i.delete_flag = 'N' 
			left join recon_mst_tdatasetfield j on j.dataset_table_field=i.dataset_field_name 
				and j.dataset_code = i.dataset_code 
				and j.delete_flag = 'N'
			where a.preprocess_code=in_preprocess_code 
      and a.recon_version = in_version_code
			and a.source_field_type='COMPARISON'
			and a.active_status ='Y'
			and a.delete_flag = 'N'
			order by a.condition_seqno
		) as b
	) as a order by a.Id;
    
	/* Lookup Expression Table 16*/
	set @row_number = 0;
	
	select a.* from(
    select 
			0 as 'Id',
			'20f' as 'Lookup Dataset', 
			'20f' as 'Lookup Field',
			'20f' as 'Expression',
			'10f' as 'Cumulative Flag',
			'10f' as 'Opening Flag',
			'10f' as 'Group Flag',
      '10f' as 'Agg Group'
		union all
    select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select 
				fn_get_datasetname(a.lookup_dataset_code) as 'Lookup Dataset', 
				ifnull(c.field_name,'') as 'Lookup Field',
				a.process_expression as 'Expression',
        fn_get_mastername(a.cumulative_flag, 'QCD_YN') as 'Cumulative Flag',
        fn_get_mastername(a.opening_flag, 'QCD_YN') as 'Opening Flag',
        fn_get_mastername(a.group_flag, 'QCD_YN') as 'Group Flag',
        fn_get_mastername(a.agg_flag, 'QCD_YN') as 'Agg Group'
			from recon_mst_tpreprocesshistory a
			inner join recon_mst_tpreprocesshistory b on a.preprocess_code = b.preprocess_code
        and a.recon_version = b.recon_version
				and b.delete_flag = 'N' 
			left join recon_mst_tdatasetfield c on  a.lookup_return_field = c.dataset_table_field 
				and b.lookup_dataset_code=c.dataset_code 
				and c.delete_flag = 'N' 
			left join recon_mst_treconfield d on d.recon_code = b.recon_code 
				and a.set_recon_field = d.recon_field_name 
				and d.delete_flag = 'N' 
			left join recon_mst_tfieldstru e on a.set_recon_field = e.field_name 
				and e.delete_flag = 'N'
			where a.preprocess_code = in_preprocess_code
      and a.recon_version = in_version_code
			and a.active_status ='Y'
			and a.delete_flag = 'N'
		) as b
	) as a order by a.Id;

	/* Aggregation Lookup Grouping Table 17*/
	set @row_number = 0;
	
	select a.* from
	(
		select 
			0 as 'Id',
			'20f' as 'Group Field Seq No',
			'80f' as 'Group Field'
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select 
				grpfield_seqno as 'Group Field Seq No',
				ifnull(c.field_name,'')  as 'Group Field'
			FROM recon_mst_tpreprocessgrpfieldhistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code
				and b.delete_flag = 'N'
			left join recon_mst_tdatasetfield c on  a.grp_field = c.dataset_table_field
				and b.lookup_dataset_code=c.dataset_code
				and c.delete_flag = 'N'
			where a.preprocess_code= in_preprocess_code
      and a.recon_version = in_version_code
			and a.active_status ='Y'
			and a.grpfield_on = 'LOOKUP'
			and a.delete_flag = 'N'
			order by a.grpfield_seqno
		) as b
	) as a order by a.Id;

	/* Lookup Order Table 18 */
	set @row_number = 0;
	
  select a.* from
	(
		select
			0 as 'Id',
			'10f' as 'Order Seq No',
			'50f'  as 'Lookup Field'
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select 
				recorder_seqno as 'Order Seq No',
				ifnull(c.field_name,'')  as 'Lookup Field'
			FROM recon_mst_tpreprocessrecorderhistory a
			inner join recon_mst_tpreprocess b on a.preprocess_code = b.preprocess_code	
				and b.delete_flag = 'N' 
			left join recon_mst_tdatasetfield c on a.recorder_field = c.dataset_table_field 
				and b.lookup_dataset_code=c.dataset_code 
				and c.delete_flag = 'N' 	  
			where a.preprocess_code=in_preprocess_code 
      and a.recon_version = in_version_code
			and a.active_status ='Y'
			and a.recorder_on = 'LOOKUP' 
			and a.delete_flag = 'N'
			order by a.recorder_seqno
		)as b
	) as a order by a.Id;


  /*PreProcess Condition Table 19 */
	SET @row_number = 0;

  SELECT a.*
  FROM
  (
    SELECT
      0 AS 'Id',
      '10f' AS 'Seq No',
      '35f' AS 'Source Field',
      '35f' AS 'Comparison Field',
      '10f' AS 'Value',
      '10f' AS 'Reverse Update'
    UNION ALL
    SELECT
      (@row_number := @row_number + 1) AS 'Id',
       b.*
    FROM
    (
			SELECT
				a.rec_seqno AS 'Seq No',
				CASE
					WHEN IFNULL(a.value_flag,'N') = 'Y'
						AND IFNULL(a.reverse_update_flag,'N') = 'N'
						THEN IFNULL(c.field_name,'')

					WHEN IFNULL(a.reverse_update_flag,'N') = 'Y'
						AND IFNULL(a.value_flag,'N') = 'N'
						THEN IFNULL(c.field_name,'')

					WHEN IFNULL(a.reverse_update_flag,'N') = 'Y'
						AND IFNULL(a.value_flag,'N') = 'Y'
						THEN a.source_field

					WHEN IFNULL(a.reverse_update_flag,'N') = 'N'
						AND IFNULL(a.value_flag,'N') = 'N'
						THEN IFNULL(c.field_name,'')

					ELSE a.source_field
				END AS 'Source Field',

        CASE
					WHEN IFNULL(a.value_flag,'N') = 'N'
            OR IFNULL(a.reverse_update_flag,'N') = 'Y'
            THEN IFNULL(d.field_name,'')
         ELSE a.comparison_field
         END AS 'Comparison Field',
				 
        CASE
					WHEN a.value_flag = 'Y' THEN 'Yes'
					WHEN a.value_flag = 'N' THEN 'No'
				ELSE ''
				END AS 'Value',

				CASE
					WHEN a.reverse_update_flag = 'Y' THEN 'Yes'
					WHEN a.reverse_update_flag = 'N' THEN 'No'
				ELSE ''
				END AS 'Reverse Update'
      FROM recon_mst_tpreprocessdsupdatehistory a
      INNER JOIN recon_mst_tpreprocesshistory b ON a.preprocess_code = b.preprocess_code 
				AND b.recon_version = in_version_code 
				AND b.delete_flag = 'N'
      LEFT JOIN recon_mst_tdatasetfield c ON a.source_field = c.dataset_table_field
				AND (b.source_dataset_code = c.dataset_code OR c.dataset_code = 'system' )
        AND c.delete_flag = 'N'
			LEFT JOIN recon_mst_tdatasetfield d ON a.comparison_field = d.dataset_table_field
				AND (b.comparison_dataset_code = d.dataset_code OR d.dataset_code = 'system')
				AND d.delete_flag = 'N'
      WHERE a.preprocess_code = in_preprocess_code
      AND a.active_status = 'Y'
      AND a.delete_flag = 'N'
		  AND a.recon_version = in_version_code
			ORDER BY a.rec_seqno
    ) b
	) a
  ORDER BY a.Id;

END $$

DELIMITER ;