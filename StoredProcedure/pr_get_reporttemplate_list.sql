DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reporttemplate_list` $$
CREATE PROCEDURE `pr_get_reporttemplate_list`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
	select
		a.reporttemplate_gid,
		a.reporttemplate_code,
		a.reporttemplate_name,
		a.report_code,
		b.report_desc,
		a.system_flag,
		case a.system_flag when 'Y' then 'System Templete' else 'Custom Templete' end as system_flag_desc,
		-- fn_get_mastername(a.system_flag, 'QCD_YN') as system_flag_desc,
		a.active_status,
		fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
	from recon_mst_treporttemplate a
	inner join recon_mst_treport b on a.report_code = b.report_code
		and b.delete_flag = 'N'
  where a.recon_code = in_recon_code
  and a.active_status = 'Y'
	and a.delete_flag = 'N'
	order by 1 desc;
END $$

DELIMITER ;