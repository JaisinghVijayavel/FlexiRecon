DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_role_reporttemplate_list` $$
CREATE PROCEDURE `pr_get_role_reporttemplate_list`
(
	in in_recon_code varchar(32),
  in in_report_flag char(1),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
	declare v_report_permission text default '';
  declare v_role_permission text default '';

  set v_report_permission = ifnull((select distinct recon_code from admin_mst_trolereportpermission where recon_code = in_recon_code),'');
	
  set v_role_permission = ifnull((select distinct role_code from admin_mst_trolereportpermission where role_code = in_role_code),'');
	
  if in_report_flag = 'R' then
		if v_report_permission = '' then 
			select distinct 
				reporttemplate_code as 'Report Code',
				reporttemplate_name as 'Report Name',
				'N' as 'CSV',
				'N' as 'Excel',
				'N' as 'Preview',
				'Y' as 'Deny',
				'M' as 'exec_type'  
			from recon_mst_treporttemplate 
			where active_status = 'Y'  
			and recon_code = in_recon_code 
			and delete_flag = 'N'  
		union
			select distinct 
				rec.dataset_code as 'Report Code',
				dt.dataset_name as 'Report Name',
				'N' as 'CSV',
				'N' as 'Excel',
				'N' as 'Preview',
				'Y' as 'Deny',
				rp.report_exec_type as 'exec_type'  
			from recon_mst_trecondataset rec
			inner join recon_mst_treport rp on rp.report_code = rec.dataset_code 
				and rp.delete_flag = 'N'
			inner join recon_mst_tdataset dt on dt.dataset_code = rec.dataset_code 
				and dt.delete_flag = 'N'
			where rec.active_status = 'Y' 
			and rec.recon_code = in_recon_code 
			and rec.delete_flag = 'N';
		else
			select 
				a.reporttemplate_code as 'Report Code',
				reporttemplate_name as 'Report Name',
				ifnull(csv_flag,'N') as 'CSV',
				ifnull(excel_flag,'N') as 'Excel',
				ifnull(preview_flag,'N') as 'Preview',
				ifnull(deny_flag,'Y') as 'Deny',
				'M' as 'exec_type' 
			from recon_mst_treporttemplate a
			left join admin_mst_trolereportpermission b on a.reporttemplate_code =b.reporttemplate_code
			where a.recon_code = in_recon_code 
			and a.active_status ='Y'
			and a.delete_flag ='N'

		  union

			select 
				a.report_code as 'Report Code',
				report_desc as 'Report Name',
				csv_flag as 'CSV',
				excel_flag as 'Excel',
				preview_flag as 'Preview',
				deny_flag as 'Deny',
				a.report_exec_type as 'exec_type' 
			from admin_mst_trolereportpermission a
		  inner join recon_mst_treport b on a.report_code = b.report_code
      where a.report_exec_type ='D' 
			and a.recon_code = in_recon_code 
			and a.delete_flag = 'N' 
		union         
			select distinct 
				rec.dataset_code as 'Report Code',
				dt.dataset_name as 'Report Name',
				ifnull(csv_flag,'N') as 'CSV',
				ifnull(excel_flag,'N') as 'Excel',
				ifnull(preview_flag,'N') as 'Preview',
				ifnull(deny_flag,'Y') as 'Deny',
				rp.report_exec_type as 'exec_type'  
			from recon_mst_trecondataset rec
			inner join recon_mst_treport rp on rp.report_code = rec.dataset_code 
				and rp.delete_flag = 'N'
			left join admin_mst_trolereportpermission rrp on rec.recon_code=rrp.recon_code 
				and rrp.report_code = rec.dataset_code
			inner join recon_mst_tdataset dt on dt.dataset_code = rec.dataset_code 
				and dt.delete_flag = 'N'
			where rec.active_status = 'Y' 
			and rec.recon_code = in_recon_code
			and rec.delete_flag = 'N';
		end if;
  else
    if v_role_permission = '' then
			select
				report_code as 'Report Code',
				report_desc as 'Report Name',
				'N' as 'CSV',
				'N' as 'Excel',
				'N' as 'Preview',
				'Y' as 'Deny',
				report_exec_type as 'exec_type'  
			from recon_mst_treport 
			where report_exec_type <> 'D'
			and delete_flag = 'N';
    else
			select
				a.report_code as 'Report Code',
				a.report_desc as 'Report Name',
				ifnull(csv_flag,'N') as 'CSV',
				ifnull(excel_flag,'N') as 'Excel',
				ifnull(preview_flag,'N') as 'Preview',
				ifnull(deny_flag,'N') as 'Deny',
				a.report_exec_type as 'exec_type' 
			from recon_mst_treport a 
      left join  admin_mst_trolereportpermission b on a.report_code=b.report_code
				and role_code = in_role_code
      where a.report_exec_type <> 'D' 
			and a.delete_flag = 'N';
		end if;
	end if;
END $$

DELIMITER ;