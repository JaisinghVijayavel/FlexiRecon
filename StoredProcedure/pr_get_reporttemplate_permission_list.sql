DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reporttemplate_permission_list` $$
CREATE PROCEDURE `pr_get_reporttemplate_permission_list`
(
	in in_recon_code varchar(32),
	in in_custom_flag boolean,
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
	if in_custom_flag = true then
		select
			a.reporttemplate_gid,
			a.reporttemplate_code,
			a.reporttemplate_name,
			a.recon_code,
			a.report_code,
			ifnull(b.report_desc,a.reporttemplate_name)as report_desc,
			ifnull(b.report_exec_type,'M')as report_exec_type,
			ifnull(b.resultset_count,1)as resultset_count,
			a.system_flag,
			case
				a.system_flag when 'Y' then 'Standard'
				else 'Custom'
			end as system_flag_desc,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treporttemplate a
		left join recon_mst_treport b on a.report_code = b.report_code
			and b.delete_flag = 'N'
		/* inner join admin_mst_tRoleReportPermission rp on rp.role_code=in_role_code and
            rp.reporttemplate_code=a.reporttemplate_code and rp.recon_code=a.recon_code */
		where a.recon_code = in_recon_code
		-- and a.active_status = 'N'
    and a.delete_flag = 'N' 
		order by 1 desc;
	else
		select
			a.reporttemplate_gid,
			a.reporttemplate_code,
			a.reporttemplate_name,
			a.recon_code,
			a.report_code,
			ifnull(b.report_desc,a.reporttemplate_name)as report_desc,
			-- ifnull(b.report_exec_type,'M')as report_exec_type,
			if(b.report_exec_type='C','C','M') as report_exec_type,
			ifnull(b.resultset_count,1)as resultset_count,
			a.system_flag,
			case a.system_flag when 'Y' then 'Standard'
				else 'Custom'
			end as system_flag_desc,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treporttemplate a
		left join recon_mst_treport b on a.report_code = b.report_code 
			and b.delete_flag = 'N'
    inner join admin_mst_trolereportpermission rp on rp.role_code = in_role_code
		and rp.reporttemplate_code = a.reporttemplate_code 
		and rp.recon_code = in_recon_code
		where a.recon_code = in_recon_code 
		and rp.deny_flag = 'N'
		and a.active_status = 'Y'
		and a.delete_flag = 'N'
		
    union
		
		select
			0 as reporttemplate_gid,
			'' as reporttemplate_code,
			a.report_desc as reporttemplate_name,
			'' as recon_code,
			a.report_code,
			a.report_desc,
			a.report_exec_type,
			a.resultset_count,
			'Y' as system_flag,
			'Standard' as system_flag_desc,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treport a
    inner join admin_mst_trolereportpermission rp on rp.role_code = in_role_code
		and rp.report_code = a.report_code 
		and rp.recon_code = '' 
		and rp.deny_flag = 'N'
		where a.active_status = 'Y' 
		and rp.report_exec_type <> 'D' 
		and a.delete_flag = 'N'
		
    union
		
		select distinct
			0 as reporttemplate_gid,
			'' as reporttemplate_code,
			a.report_desc as reporttemplate_name,
			'' as recon_code,
			a.report_code, 
			a.report_desc,
			a.report_exec_type,
			a.resultset_count,
			'Y' as system_flag,
			'Standard' as system_flag_desc,
			a.active_status,
			fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
		from recon_mst_treport a
		inner join recon_mst_trecondataset as b on a.report_code = b.dataset_code 
			and b.active_status = 'Y'
			and b.delete_flag = 'N'
		inner join admin_mst_trolereportpermission rp on rp.role_code = in_role_code
		 and rp.report_code = a.report_code 
		 and rp.recon_code = in_recon_code 
		 and rp.deny_flag = 'N'
		where b.recon_code = in_recon_code 
		and  a.active_status = 'Y' 
		and a.report_exec_type = 'D' 
		and a.delete_flag = 'N';
	end if;
END $$

DELIMITER ;