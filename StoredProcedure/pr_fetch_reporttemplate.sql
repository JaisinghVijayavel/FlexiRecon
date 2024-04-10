DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_reporttemplate` $$
CREATE PROCEDURE `pr_fetch_reporttemplate`
(
	in in_reporttemplate_code varchar(32),
  in in_recon_code varchar(32),
  in in_report_code varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
	declare v_report_code text default '';
	declare v_recon_code text default '';

  declare v_file_path text default '';
  declare v_file_name text default '';

  set in_reporttemplate_code = ifnull(in_reporttemplate_code,'');

	select
    report_code,
    recon_code
  into
    v_report_code,
    v_recon_code
  from recon_mst_treporttemplate
  where reporttemplate_code = in_reporttemplate_code
  and delete_flag = 'N';

  set v_report_code = ifnull(v_report_code,'');
  set v_recon_code = ifnull(v_recon_code,'');

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
			b.report_desc,
			a.active_status,
			a.system_flag,
			a.recon_code,
			a.sortby_code,
			a.file_name,
			a.file_path,
			fn_get_mastername(a.system_flag, 'QCD_YN') as system_flag_desc,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treporttemplate a
		inner join recon_mst_treport b on a.report_code = b.report_code
			and b.delete_flag = 'N'
		where a.reporttemplate_code = in_reporttemplate_code
		and a.active_status = 'Y'
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

  if in_reporttemplate_code <> '' then
		select
			a.reporttemplatefilter_gid,
			a.filter_seqno,
			a.report_field,
			fn_get_reconfieldname(v_recon_code,a.report_field) as reportparam_value,
			a.filter_criteria,
			fn_get_mastername(a.filter_criteria, 'QCD_RP_CONSTRAINT') as filter_criteria_desc,
			a.filter_value,
			a.open_parentheses_flag,
			a.close_parentheses_flag,
			a.join_condition,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treporttemplatefilter a
		where a.reporttemplate_code = in_reporttemplate_code
		and a.active_status = 'Y'
		and a.delete_flag = 'N'
		order by a.filter_seqno;
  else
		select
			0 as reporttemplatefilter_gid,
			a.filter_seqno,
			a.report_field,
			fn_get_reconfieldname(v_recon_code,a.report_field) as reportparam_value,
			a.filter_criteria,
			fn_get_mastername(a.filter_criteria, 'QCD_RP_CONSTRAINT') as filter_criteria_desc,
			a.filter_value,
			a.open_parentheses_flag,
			a.close_parentheses_flag,
			a.join_condition,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treportfilter a
		where a.report_code = v_report_code
		and a.active_status = 'Y'
		and a.delete_flag = 'N'
		order by a.filter_seqno;
  end if;

  call pr_get_reporttemplatefield(in_reporttemplate_code,v_recon_code,v_report_code);

  if in_reporttemplate_code <> '' then
		select
			a.reporttemplatesorting_gid,
			a.reporttemplate_code,
			a.report_field,
			a.sorting_order,
			a.active_status,
			fn_get_reconfieldname(v_recon_code,report_field) as reportparam_value
		from recon_mst_treporttemplatesorting  a
		where reporttemplate_code = in_reporttemplate_code
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
			fn_get_reconfieldname(v_recon_code,report_field) as reportparam_value
		from recon_mst_treportsorting  a
		where report_code = v_report_code
		and a.active_status = 'Y'
		and a.delete_flag = 'N'
		order by a.sorting_order;
  end if;
END $$

DELIMITER ;