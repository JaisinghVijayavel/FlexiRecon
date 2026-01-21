DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_reporttemplate_multidataset` $$
CREATE PROCEDURE `pr_fetch_reporttemplate_multidataset`(
	in in_reporttemplate_code varchar(32),
	in in_reporttemplateresultset_code varchar(32) ,
	in in_recon_code varchar(32),
	in in_report_code varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
me:BEGIN
  /*
    Created By :
    Created Date :

    Updated By : Vijayavel
    updated Date : 21-01-2026

    Version : 2
  */

	declare v_report_code text default '';
	declare v_recon_code text default ''; 
	declare v_report_exec_type text default '';
	declare v_file_path text default '';
	declare v_file_name text default '';

  if in_reporttemplate_code = '' then
    set in_reporttemplate_code = null;
  end if;

	select 
    a.recon_code,
    b.src_report_code,
    c.report_exec_type
  into
    v_recon_code,
    v_report_code,
    v_report_exec_type
  from recon_mst_treporttemplate as a
  inner join recon_mst_treporttemplateresultset as b on a.reporttemplate_code = b.reporttemplate_code
    and b.active_status = 'Y'
    and b.delete_flag = 'N'
  inner join recon_mst_treport as c on b.src_report_code = c.report_code
    and c.active_status = 'Y'
    and c.delete_flag = 'N'
  where a.reporttemplate_code = in_reporttemplate_code
  and b.reporttemplateresultset_code = ifnull(in_reporttemplateresultset_code,b.reporttemplateresultset_code)
  -- and a.active_status = 'Y'
  and a.delete_flag = 'N'
  limit 1;

  set v_report_code = ifnull(v_report_code,'');
  set v_recon_code = ifnull(v_recon_code,'');
  set v_report_exec_type = ifnull(v_report_exec_type,'M');

  if v_report_code = '' then
    set v_report_code = in_report_code;
    set v_recon_code = in_recon_code;
  end if;

  if in_reporttemplate_code <> '' then
		select
			a.reporttemplate_gid,
			a.reporttemplate_code,
			a.reporttemplate_name,
			a.recon_code,
			a.report_code,
			a.reporttemplate_name as report_desc,-- b.report_desc,
			a.active_status,
			a.system_flag,
			a.recon_code,
			a.sortby_code,
			a.file_name,
			a.file_path,
			fn_get_mastername(a.system_flag, 'QCD_YN') as system_flag_desc,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treporttemplate a
		where a.reporttemplate_code = in_reporttemplate_code
		-- and a.active_status = 'Y'
		and a.delete_flag = 'N';
  else
    set v_file_path = fn_get_configvalue('temp_file_folder_path');
    set v_file_path = ifnull(v_file_path,'');

    if v_file_path <> '' then
      set v_file_name = SPLIT(v_file_path,char(92),-1);
    end if;

		select
			0 as reporttemplate_gid,
			'' as reporttemplate_code,
			a.report_desc as reporttemplate_name,
			v_recon_code as recon_code,
			a.report_code,
			a.report_desc,
			a.active_status,
			'Y' as system_flag,
			v_recon_code as recon_code,
			'asc' as sortby_code,
			v_file_name as file_name,
			v_file_path as file_path,
			fn_get_mastername('Y', 'QCD_YN') as system_flag_desc,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treport as a
		where a.report_code = in_report_code
		and a.active_status = 'Y'
		and a.delete_flag = 'N';
  end if;

  if (in_reporttemplate_code <> '' and v_report_exec_type = 'M') then
		select
			a.reporttemplatefilter_gid,
			a.filter_seqno,
			a.report_field,
      if(v_report_exec_type = 'D',
         fn_get_datasetfieldname(in_report_code,a.report_field),
		-- fn_get_reconfieldname(v_recon_code,a.report_field)) as reportparam_value,
			concat(fn_get_reconfieldname(v_recon_code,a.report_field), '-',
        fn_get_fieldtype(in_recon_code, a.report_field))) as reportparam_value,
			a.filter_criteria,
			fn_get_mastername(a.filter_criteria, 'QCD_RP_CONSTRAINT') as filter_criteria_desc,
			fn_get_reportfiltervalue(in_recon_code,'',a.filter_value,in_user_code) as filter_value,
      -- a.filter_value,
			a.open_parentheses_flag,
			a.close_parentheses_flag,
			a.join_condition,
      a.system_flag,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
			rr.resultset_name as report_name,
			case
				when rr.resultset_exec_type = 'R' then src_report_code
				else rr.resultset_name
			end as report_code,
			v_report_exec_type as exec_type_code,
			rt.reporttemplate_code,
			rr.resultset_name
		from recon_mst_treporttemplatefilter a
		inner join recon_mst_treporttemplate as rt on a.reporttemplate_code  = rt.reporttemplate_code
			and rt.report_code = v_report_code
      -- and rt.active_status = 'Y'
			and rt.delete_flag  = 'N'
		inner join recon_mst_treporttemplateresultset as rr on a.report_code = rr.src_report_code
			and rr.report_code = v_report_code
			-- and rr.active_status = 'Y'
			and rr.delete_flag = 'N'
		where a.reporttemplate_code = in_reporttemplate_code
			and a.reporttemplateresultset_code =ifnull(in_reporttemplateresultset_code,a.reporttemplateresultset_code)
			and a.active_status = 'Y'
			and a.delete_flag = 'N'
		order by a.filter_seqno;

  elseif in_reporttemplate_code <> '' then
		select
			a.reporttemplatefilter_gid,
			a.filter_seqno,
			a.report_field,
      if(v_report_exec_type = 'D',
         fn_get_datasetfieldname(in_report_code,a.report_field),
         -- fn_get_reconfieldname(v_recon_code,a.report_field)) as reportparam_value,
			concat(fn_get_reconfieldname(v_recon_code,a.report_field), '-',
        fn_get_fieldtype(in_recon_code, a.report_field))) as reportparam_value,
			a.filter_criteria,
			fn_get_mastername(a.filter_criteria, 'QCD_RP_CONSTRAINT') as filter_criteria_desc,
			-- fn_get_reportfiltervalue(in_recon_code,'',a.filter_value,in_user_code) as filter_value,
      a.filter_value,
			a.open_parentheses_flag,
			a.close_parentheses_flag,
			a.join_condition,
			a.system_flag,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
			rr.resultset_name as report_name,
      'M' as exec_type_code,
			case
				when rr.resultset_exec_type = 'R' then src_report_code
				else ''
			end as report_code,
      a.reporttemplateresultset_code
		from recon_mst_treporttemplatefilter a
		left join recon_mst_treporttemplateresultset as rr on a.reporttemplate_code = rr.reporttemplate_code
			and rr.reporttemplateresultset_code=a.reporttemplateresultset_code
			and rr.active_status='Y'
			and rr.delete_flag='N'
		where a.reporttemplate_code = in_reporttemplate_code
    and a.reporttemplateresultset_code = ifnull(in_reporttemplateresultset_code,a.reporttemplateresultset_code)
    and rr.resultset_name is not null
    and a.active_status = 'Y'
		and a.delete_flag = 'N'
		order by a.filter_seqno;
  elseif exists (select * from recon_mst_treportfilter
                  where report_code = v_report_code
                  and active_status = 'Y'
                  and delete_flag = 'N') then
		select
			1 as reporttemplatefilter_gid,
			a.filter_seqno,
			a.report_field,
      if(v_report_exec_type = 'D',
         fn_get_datasetfieldname(in_report_code,a.report_field),
         -- fn_get_reconfieldname(v_recon_code,a.report_field)) as reportparam_value,
			concat(fn_get_reconfieldname(v_recon_code,a.report_field), '-',
        fn_get_fieldtype(in_recon_code, a.report_field))) as reportparam_value,
			a.filter_criteria,
			fn_get_mastername(a.filter_criteria, 'QCD_RP_CONSTRAINT') as filter_criteria_desc,
			fn_get_reportfiltervalue(in_recon_code,'',a.filter_value,in_user_code) as filter_value,
			a.open_parentheses_flag,
			a.close_parentheses_flag,
			a.join_condition,
      'Y' as system_flag,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
      a.report_code,
      '' as reporttemplateresultset_code
		from recon_mst_treportfilter a
		where a.report_code = v_report_code
		and a.active_status = 'Y'
		and a.delete_flag = 'N'
		order by a.filter_seqno;
  else
		select
			1 as reporttemplatefilter_gid,
			1 as filter_seqno,
			'scheduler_gid' as report_field,
			'Scheduler Id-INTEGER' as reportparam_value,
			'QCD_IS_GREATER_THAN' as filter_criteria,
			fn_get_mastername('QCD_IS_GREATER_THAN', 'QCD_RP_CONSTRAINT') as filter_criteria_desc,
			0 as filter_value,
			'N' as open_parentheses_flag,
			'N' as close_parentheses_flag,
			'' as join_condition,
      'Y' as system_flag,
			'Y' as active_status,
			fn_get_mastername('Y', 'QCD_STATUS') as active_status_desc,
      '' as report_code,
      '' as reporttemplateresultset_code;
  end if;

  set in_reporttemplateresultset_code = ifnull(in_reporttemplateresultset_code,'');

  call pr_get_reporttemplatefield(in_reporttemplate_code,in_reporttemplateresultset_code,v_recon_code,v_report_code);

  if in_reporttemplate_code <> '' then
		select
			a.reporttemplatesorting_gid,
			a.reporttemplate_code,
			a.report_field,
			a.sorting_order,
			a.active_status,
      if(v_report_exec_type = 'D',
         fn_get_datasetfieldname(in_report_code,report_field),
			-- fn_get_reconfieldname(v_recon_code,a.report_field)) as reportparam_value,
			concat(fn_get_reconfieldname(v_recon_code,a.report_field), '-',
        fn_get_fieldtype(in_recon_code, a.report_field))) as reportparam_value,
        sorting_type as sort_type_code, sorting_type as sort_type
    from recon_mst_treporttemplatesorting  a
		where reporttemplate_code = in_reporttemplate_code
			and reporttemplateresultset_code = in_reporttemplateresultset_code
			and a.active_status = 'Y'
			and a.delete_flag = 'N'
		order by a.sorting_order;
  else
		select
			0 as reporttemplatesorting_gid,
			'' as reporttemplate_code,
			a.report_field,
			a.sorting_order,
			a.active_status,
      if(v_report_exec_type = 'D',
         fn_get_datasetfieldname(in_report_code,report_field),
		-- fn_get_reconfieldname(v_recon_code,a.report_field)) as reportparam_value,
			concat(fn_get_reconfieldname(v_recon_code,a.report_field), '-',
        fn_get_fieldtype(in_recon_code, a.report_field))) as reportparam_value
		from recon_mst_treportsorting  a
		where report_code = v_report_code
			and a.active_status = 'Y'
			and a.delete_flag = 'N'
		order by a.sorting_order;
  end if;
END $$

DELIMITER ;