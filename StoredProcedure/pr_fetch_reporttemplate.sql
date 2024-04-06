DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_reporttemplate` $$
CREATE PROCEDURE `pr_fetch_reporttemplate`
(
	in in_reporttemplate_code varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN

	declare v_report_code text default '';

	select
		a.reporttemplate_gid,
		a.reporttemplate_code,
		a.reporttemplate_name,
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

	set v_report_code = (select report_code from recon_mst_treporttemplate
											 where reporttemplate_code = in_reporttemplate_code
											 and delete_flag = 'N' );

	select
		a.reporttemplatefilter_gid,
		a.filter_seqno,
		a.report_field,
		b.reportparam_value,
		a.filter_criteria,
		fn_get_mastername(a.filter_criteria, 'QCD_RP_CONSTRAINT') as filter_criteria_desc,
		a.filter_value,
		a.open_parentheses_flag,
		a.close_parentheses_flag,
		a.join_condition,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_treporttemplatefilter a
	inner join recon_mst_treportparam b on a.report_field = concat(ifnull(reportparam_prefix,''),reportparam_code)
	-- on a.report_field = b.reportparam_code

		and b.report_code = v_report_code
		and b.delete_flag = 'N'
	where a.reporttemplate_code = in_reporttemplate_code
	and a.active_status = 'Y'
	and a.delete_flag = 'N'
	order by a.filter_seqno;

  call pr_get_reporttemplatefield(in_reporttemplate_code);

  select
    a.reporttemplatesorting_gid,
    a.reporttemplate_code,
    a.report_field,
    a.sorting_order,
    a.active_status,
    b.reportparam_value
  from recon_mst_treporttemplatesorting  a
  inner join recon_mst_treportparam b on a.report_field = concat(ifnull(reportparam_prefix,''),reportparam_code)
    and b.report_code = v_report_code
    and b.delete_flag = 'N'
  where reporttemplate_code = in_reporttemplate_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';
END $$

DELIMITER ;