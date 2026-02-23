DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_recondetail` $$
CREATE PROCEDURE `pr_fetch_recondetail`
(
	in in_recon_code varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 18-02-2026

    Version : 1
  */

	declare v_app_date_format text default '';
	declare v_dataset_count int default 0;
	declare v_recon_code text default ''; 
	declare v_clone_reconcode text default '';

  set v_clone_reconcode = (select clone_recon_code from recon_mst_trecon where recon_code = in_recon_code);
	set v_app_date_format = fn_get_configvalue('app_date_format');

	select
		a.recon_gid,
		a.recon_code,
		a.recon_name,
		a.recontype_code,
		fn_get_mastername(a.recontype_code, 'QCD_RC_RCON_TYPE') as recontype_desc,
		DATE_FORMAT(a.period_from,'%d/%m/%Y') as  period_from,
		ifnull(DATE_FORMAT(a.period_to,'%d/%m/%Y'),'') as period_to,
    ifnull(DATE_FORMAT(a.recon_closure_date,'%d/%m/%Y'),'') as recon_closure_date,
    ifnull(DATE_FORMAT(a.recon_cycle_date,'%d/%m/%Y'),'') as recon_cycle_date,
		a.until_active_flag,
		a.recon_value_flag,
		a.recon_value_field,
		a.recon_date_flag,
		a.recon_date_field,
		a.recon_automatch_partial,
		a.threshold_code,
		fn_get_mastername(a.threshold_code, 'QCD_TV') as threshold_desc,
		a.threshold_plus_value,
		a.threshold_minus_value,
		a.clone_recon_code,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
    processing_method,
    fn_get_mastername(processing_method, 'QCD_komethod') as processing_method_desc
	from recon_mst_trecon a
	where a.recon_code =in_recon_code and a.delete_flag = 'N';

	set v_recon_code = (select recon_code from recon_mst_trecon where recon_code=in_recon_code);

	select
		a.recondataset_gid,
		a.recon_code,
		a.dataset_code,
		b.dataset_name,
		a.dataset_type,
		fn_get_mastername(a.dataset_type, 'QCD_DS_TYPE') as dataset_type_desc,
		a.parent_dataset_code,
		ifnull(c.dataset_name,'') as parent_dataset_name,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_trecondataset a
	inner join recon_mst_tdataset b on a.dataset_code = b.dataset_code
	left join recon_mst_tdataset c on a.parent_dataset_code = c.dataset_code and a.dataset_type='S' and c.delete_flag = 'N'
	where a.recon_code = v_recon_code and  a.active_status = 'Y'
    and a.delete_flag = 'N' and b.delete_flag = 'N' ;

	select  count(*) into v_dataset_count from recon_mst_treconfield where recon_code=v_recon_code and delete_flag = 'N';
	set v_dataset_count = ifnull(v_dataset_count,0);

	if(v_dataset_count>0) then
		SET @sql = NULL;
		SELECT
			GROUP_CONCAT(DISTINCT
				CONCAT(
					'MAX(CASE WHEN dataset_name = ''',
					dataset_name,
					''' THEN field_desc END) AS ''',
					dataset_name ,''''
				)
			) INTO @dataset
		FROM
		(
			select distinct
				recon_name,a.recon_field_name,c.field_name,
				dataset_name,display_order,e.dataset_code,                
				c.field_name as field_desc
			from recon_mst_treconfield a
			left join recon_mst_treconfieldmapping b on a.recon_field_name=b.recon_field_name and b.recon_code = a.recon_code	and b.delete_flag = 'N'
			left join recon_mst_tdatasetfield c on c.dataset_table_field=b.dataset_field_name and c.dataset_code = b.dataset_code	and c.delete_flag = 'N'
			left join recon_mst_trecon d on a.recon_code=d.recon_code and d.delete_flag = 'N'
			left join recon_mst_tdataset e on b.dataset_code=e.dataset_code	and e.delete_flag = 'N'
			inner join recon_mst_trecondataset f on b.dataset_code=f.dataset_code and f.delete_flag = 'N' and b.recon_code = f.recon_code
			where a.recon_code= v_recon_code and a.delete_flag = 'N'
			order by dataset_name,display_order
		) a;

    set @dataset = ifnull(@dataset,'');

    if @dataset <> '' then
      set @dataset = concat(',',@dataset);
    end if;

	SET @sql = CONCAT('SELECT
      b.reconfield_gid,
      a.dataset_code,
      b.recon_code,
      a.recon_name,
      b.recon_field_desc as ''Recon Field Name'',
      b.display_order as ''Display Order'',
      b.recon_field_name,
      b.recon_field_type,
      b.recon_field_length',
      @dataset,
      ' from recon_mst_treconfield b
      left join recon_data_mapping as a on a.recon_field_name = b.recon_field_name
        and a.recon_code = b.recon_code
      where b.recon_code=''',v_recon_code,'''
      and b.delete_flag = ''N''
      GROUP BY a.recon_name, b.recon_field_name, b.display_order
      order by b.display_order;');

		PREPARE dynamic_sql FROM @sql;
		EXECUTE dynamic_sql;
	else
		SELECT
			b.reconfield_gid,
			a.dataset_code,
			b.recon_code,
			b.recon_name,
			b.recon_field_desc as 'Recon Field Name',
			b.display_order as 'Display Order',
			b.recon_field_name,
      b.recon_field_type,
      b.recon_field_length
		from recon_mst_treconfield b
		left join recon_data_mapping a on a.recon_field_name = b.recon_field_name
      and a.recon_code = b.recon_code
		where b.recon_code=v_recon_code
    and b.delete_flag = 'N'
		GROUP BY recon_name, b.recon_field_name, b.display_order
		order by b.display_order;
	end if;

	select
		recon_field_name,
		recon_field_desc,
		ifnull(recon_field_type,'') as recon_field_type
	from recon_mst_treconfield where recon_code=v_recon_code and delete_flag = 'N';

	select
		a.rule_gid,
		a.rule_code,
    a.recon_code,
		a.rule_name,
		a.rule_order,
		a.rule_apply_on,
		fn_get_mastername(a.rule_apply_on, 'QCD_RS_RULE_APPLLIED') as rule_apply_on_desc,
		a.rule_order,
		a.group_flag,
		fn_get_mastername(a.group_flag, 'QCD_RULE_GRP') as  group_flag_desc,
		a.rule_order as ruleorder,
        a.recon_version,
		a.source_dataset_code,
		a.comparison_dataset_code,
		a.parent_acc_mode,
		fn_get_mastername(a.parent_acc_mode, 'QCD_RS_ACC_MODE') as parent_acc_mode_desc,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
		b.dataset_name as source_dataset_desc,
		c.dataset_name as comparison_dataset_desc,
		b.dataset_name as dataset_name
	from recon_mst_trule a
	inner join recon_mst_tdataset b on a.source_dataset_code = b.dataset_code and b.delete_flag = 'N' 
	inner join recon_mst_tdataset c on a.comparison_dataset_code = c.dataset_code and c.delete_flag = 'N' 
	where a.recon_code = in_recon_code and a.delete_flag = 'N' and a.active_status='Y'
	ORDER BY a.rule_order;

	select 
		theme_gid,
		theme_code,
		theme_desc,
		a.recon_code,
    theme_order,
		b.recon_name,
		a.active_status,
    ifnull(a.theme_type_code,'') as theme_type_code,
		fn_get_mastername(a.theme_type_code, 'QCD_THEME_TYPE') as theme_type_desc,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
		a.hold_flag,
		fn_get_mastername(a.hold_flag, 'QCD_YN') as hold_flag_desc
	from recon_mst_ttheme  a 
	inner join recon_mst_trecon b on a.recon_code=b.recon_code where a.recon_code=in_recon_code and a.active_status='Y' and a.delete_flag = 'N' and b.delete_flag = 'N' 
	ORDER BY a.theme_order;

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
		preprocess_order,
		process_query,
		process_function,
		get_recon_field,
		set_recon_field,
		a.hold_flag,
		fn_get_mastername(a.hold_flag, 'QCD_YN') as hold_flag_desc
	from recon_mst_tpreprocess  a 
	inner join recon_mst_trecon b on a.recon_code=b.recon_code where a.recon_code=in_recon_code and a.active_status='Y' and a.delete_flag = 'N' and b.delete_flag = 'N' 
	ORDER BY a.preprocess_order;


		select
			a.reporttemplate_gid,
			a.reporttemplate_code,
			a.reporttemplate_name,
      a.recon_code,
			a.report_code,
			b.report_desc,
			a.system_flag,
			case a.system_flag when 'Y' then 'Standard'	else 'Custom' end as system_flag_desc,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treporttemplate a
		inner join recon_mst_treport b on a.report_code = b.report_code and b.delete_flag = 'N' and a.active_status ='Y'
		where a.recon_code = in_recon_code and a.delete_flag = 'N';
END $$

DELIMITER ;