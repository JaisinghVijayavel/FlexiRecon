DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_report_rulecondition` $$
CREATE PROCEDURE `pr_report_rulecondition`
(
	in in_rule_code varchar(32),
  in in_recon_code varchar(32),
  in in_rule_version varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)    
)
BEGIN
  /*
    Created By :
    Created Date :

    Updated By : Vijayavel
    updated Date : 17-07-2026

    Version : 2
  */

	/* Rule Header */
  select 
    a.rule_code as 'Rule Code', 
    a.rule_name as 'Rule Name', 
    fn_get_mastername(a.rule_apply_on, 'QCD_RS_RULE_APPLLIED') as 'Rule Applied On',
    a.rule_order as 'Rule Order',
    DATE_FORMAT(a.period_from,'%d/%m/%Y') as 'Period From',
		ifnull(DATE_FORMAT(a.period_to,'%d/%m/%Y'),'') as 'Period To',
		case a.probable_match_flag when 'Y' then 'YES' else 'NO' end as 'Probable Flag',
    fn_get_datasetname(a.source_dataset_code) as 'Source Dataset',
    fn_get_datasetname(a.comparison_dataset_code) as 'Comparison Dataset',
    fn_get_mastername(a.source_acc_mode, 'QCD_RS_ACC_MODE') as 'Source Acc Mode',
		fn_get_mastername(a.comparison_acc_mode, 'QCD_RS_ACC_MODE') as 'Comparison Acc Mode', 
    fn_get_mastername(a.group_flag, 'QCD_RULE_GRP') as 'Group Flag',
    a.rule_remark as 'Rule Remark',
    fn_get_mastername(a.rule_apply_on, 'QCD_RS_RULE_APPLLIED') as 'Rule Applied On'
  from recon_mst_trulehistory a
	inner join recon_mst_tdataset b on a.source_dataset_code = b.dataset_code 
		and b.delete_flag='N'
  inner join recon_mst_tdataset c on a.comparison_dataset_code = c.dataset_code 
		and c.delete_flag='N'
  where a.recon_code = in_recon_code
	and a.recon_version = in_rule_version
	and a.rule_code = in_rule_code
	and a.active_status = 'Y' 
	and a.delete_flag = 'N' 
	order by `Rule Order`;  
    
	/* Rule Condition */
	set @row_number = 0;
	
	select a.* from 
	(
		select 
			 0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
					'15f' as 'Source Field',
			'20f' as 'Extraction',
			'15f' as 'Comparision Field',
					'20f' as 'Comparision',
			'5f' as '--',
			'10f' as 'Join'
		union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select		
				a.rulecondition_seqno as 'Seq No',
				case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Source Field',
				extraction_criteria as 'Extraction',
				ifnull(d.recon_field_desc,f.field_alias_name) as 'Comparison Field',
				comparison_criteria as 'Comparision',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Join'		
			from recon_mst_truleconditionhistory a
			inner join recon_mst_trule b on b.rule_code = a.rule_code 
				and b.delete_flag='N' 
			left join recon_mst_treconfield c on c.recon_field_name = a.source_field 
				and b.recon_code = c.recon_code 
				and c.delete_flag='N'
			left join recon_mst_treconfield d on d.recon_field_name = a.comparison_field 
				and b.recon_code = d.recon_code 
				and d.delete_flag='N'
			left join recon_mst_tfieldstru e on a.source_field = e.field_name 
				and e.delete_flag = 'N'
			left join recon_mst_tfieldstru f on a.comparison_field=f.field_name 
				and f.delete_flag = 'N'
			where b.rule_code = in_rule_code
			and b.recon_code = in_recon_code
			and a.recon_version = in_rule_version
			and b.active_status = 'Y' 
			and b.delete_flag = 'N' 
			order by rulecondition_seqno
		) as b
	) as a order by a.Id;
    
  set @row_number = 0;
	
	select a. * from 
	(
		select 
			0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
      '25f' as 'Source Field',
			-- '20f' as 'Filter Criteria',
			'20f' as 'Ident Criteria',
			'25f' as 'Ident Value',
			'5f' as '--',
			'10f' as 'Join'
    union all
		select (@row_number := @row_number + 1) as 'Id', b.* from
		(
			select
				ruleselefilter_seqno as 'Seq No',
				case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Source Field',
				-- filter_criteria as 'Filter Criteria',
				ident_criteria as 'Ident Criteria',
				ident_value as 'Ident Value',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Join'
			from recon_mst_truleselefilterhistory a
			inner join recon_mst_trule b on b.rule_code = a.rule_code
				and b.delete_flag = 'N'
			left join recon_mst_treconfield c on c.recon_field_name = a.filter_field
				and b.recon_code=c.recon_code
				and c.delete_flag = 'N'
			left join recon_mst_tfieldstru e on a.filter_field = e.field_name
				and e.delete_flag = 'N'
			where a.rule_code = in_rule_code
			and filter_applied_on='S'
			and a.recon_version = in_rule_version
			and a.delete_flag = 'N'
			order by ruleselefilter_seqno
		) as b
	) as a order by a.Id;

	set @row_number = 0;
  select a.* from
	(
    select
			0 as 'Id',
			'10f' as 'Seq No',
			'5f' as '-',
			'25f' as 'Comparison Field',
			-- '20f' as 'Filter Criteria',
			'20f' as 'Ident Criteria',
			'25f' as 'Ident Value',
			'5f' as '--',
			'10f' as 'Join'
    union all
    select (@row_number := @row_number + 1) as 'Id', b.* from
		(
			select
				ruleselefilter_seqno as 'Seq No',
				case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Comparison Field',
				-- filter_criteria as 'Filter Criteria',
				ident_criteria as 'Ident Criteria',
				ident_value as 'Ident Value',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Join'
			from recon_mst_truleselefilterhistory a
			inner join recon_mst_trule b on b.rule_code = a.rule_code
				and b.delete_flag = 'N'
			left join recon_mst_treconfield c on c.recon_field_name = a.filter_field
				and b.recon_code=c.recon_code
				and c.delete_flag = 'N'
			left join recon_mst_tfieldstru e on a.filter_field = e.field_name
				and e.delete_flag = 'N'
			where a.rule_code = in_rule_code
			and a.recon_version = in_rule_version
			and filter_applied_on = 'C'
			and a.delete_flag = 'N'
			order by ruleselefilter_seqno
		) as b
	) as a order by a.Id;

  set @row_number = 0;
  select a.* from
	(
    select
			0 as 'Id',
			'50f' as 'Seq No',
			'50f' as 'Filter Field'
    union all
		select (@row_number := @row_number + 1) as 'Id', b.* from
		(
			select
				a.rulegrpfield_seqno as 'Seq No',
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Filter Field'
			from recon_mst_trulegrpfieldhistory a
			inner join recon_mst_trule b on b.rule_code = a.rule_code
				and b.delete_flag = 'N'
			left join recon_mst_treconfield c on b.recon_code = c.recon_code
				and a.grp_field = c.recon_field_name
				and c.delete_flag = 'N'
			left join recon_mst_tfieldstru e on a.grp_field = e.field_name
				and e.delete_flag = 'N'
			where a.rule_code = in_rule_code
			and a.recon_version = in_rule_version
			and a.active_status = 'Y'
			and a.delete_flag= 'N'
			order by a.rulegrpfield_seqno
		)as b
	) as a order by a.Id;

  set @row_number = 0;

  select a.* from
	(
		select 
			0 as 'Id',
			'50f' as 'Seq No',
			'50f' as 'Source Field'
    union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select 
				a.recorder_seqno as 'Seq No',
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Source Field'	
			from recon_mst_trulerecorderhistory a
			inner join recon_mst_trule b on b.rule_code = a.rule_code 
				and b.delete_flag = 'N'
			left join recon_mst_treconfield c on c.recon_field_name = a.recorder_field 
				and b.recon_code = c.recon_code 
				and c.delete_flag = 'N' 
			left join recon_mst_tfieldstru e on a.recorder_field = e.field_name 
				and e.delete_flag = 'N'
			where a.rule_code = in_rule_code 
			and a.recon_version = in_rule_version
			and a.active_status = 'Y' 
			and recorder_applied_on = 'S' 
			and a.delete_flag = 'N' 
			order by recorder_seqno 
		)as b
	) as a order by a.Id;
    
  set @row_number = 0;
	
  select a.* from
	(
		select 
			0 as 'Id',
			'50f' as 'Seq No',
			'50f' as 'Target Field'
    union all
    select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select  
				a.recorder_seqno as 'Seq No',
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Target Field'	
			from recon_mst_trulerecorder a
			inner join recon_mst_trule b on b.rule_code = a.rule_code 
				and b.delete_flag = 'N'
			left join recon_mst_treconfield c on c.recon_field_name = a.recorder_field 
				and b.recon_code=c.recon_code 
				and c.delete_flag = 'N' 
			left join recon_mst_tfieldstru e on a.recorder_field = e.field_name 
				and e.delete_flag = 'N'
			where a.rule_code = in_rule_code 
			and a.recon_version = in_rule_version
			and a.active_status = 'Y' 
			and recorder_applied_on = 'C' 
			and a.delete_flag= 'N' 
			order by a.recorder_seqno
		)as b
	) as a order by a.Id;
    
	set @row_number = 0;
	
  select a.* from
	(
    select 
			0 as 'Id',
			'10f' as 'Seq No',
			'25f' as 'Aggregate Function Desc',
      '20f'  as 'Applied On',
      '20f' as 'Recon Field',
      '25f' as 'Aggregate Function'
    union all
    select (@row_number := @row_number + 1) as 'Id', b.* from
		(
			select 
				ruleaggfield_seqno as 'Seq No',
				ruleaggfield_desc as 'Aggregate Function Desc',
				fn_get_mastername(a.ruleaggfield_applied_on, 'QCD_Appiled_on') as 'Applied On',	
				ifnull(c.recon_field_desc,e.field_alias_name) as 'Recon Field',
				ruleagg_function as 'Aggregate Function'
			from recon_mst_truleaggfield a
			inner join recon_mst_trule b on b.rule_code = a.rule_code 
				and b.delete_flag = "N"
			left join recon_mst_treconfield c on b.recon_code = c.recon_code 
				and a.recon_field = c.recon_field_name 
				and c.delete_flag = "N"
			left join recon_mst_tfieldstru e on a.recon_field = e.field_name 
				and e.delete_flag = 'N'
			where a.rule_code = in_rule_code 
			and a.active_status = 'Y' 
			and a.delete_flag = "N" 
			order by a.ruleaggfield_seqno
		)as b
	) as a order by a.Id;
     
	set @row_number = 0;
	
  select a.* from
	(
		select 
			0 as 'Id',
      '10f' as 'Seq No',
			'5f' as '-',
			'30f' as 'Condition Field',
			'20f' as 'Condition Criteria',
			'20f' as 'Comparison Field/Value',
			'5f' as '--',
			'10f' as 'Join'
    union all
		select (@row_number := @row_number + 1) as 'Id', b.* from 
		(
			select
				ruleaggcondition_seqno as 'Seq No',
				case open_parentheses_flag when 'Y' then '(' else '' end as '-',
				b.ruleaggfield_desc as 'Condition Field',
				ruleagg_criteria as 'Condition Criteria',
				case when ( c.ruleagg_field= a.ruleagg_value )then c.ruleaggfield_desc else ruleagg_value end  as 'Comparison Field/Value',
				case close_parentheses_flag when 'Y' then ')' else '' end as '--',
				if(ifnull(join_condition,'') != '',join_condition,'AND') as 'Join'
			from recon_mst_truleaggcondition a
			left join recon_mst_truleaggfield b on a.ruleagg_field = b.ruleagg_field 
				and a.rule_code = b.rule_code 
				and b.delete_flag="N"
			left join recon_mst_truleaggfield c on a.ruleagg_value = c.ruleagg_field 
				and a.rule_code = c.rule_code 
				and c.delete_flag="N"
			where a.rule_code = in_rule_code 
			and a.active_status = 'Y' 
			and a.delete_flag="N" 
			order by a.ruleaggcondition_seqno
		) as b
	) as a order by a.Id;
        
END $$

DELIMITER ;