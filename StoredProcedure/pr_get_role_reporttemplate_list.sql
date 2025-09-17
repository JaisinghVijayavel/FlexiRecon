DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_role_reporttemplate_list` $$

CREATE PROCEDURE `pr_get_role_reporttemplate_list`
(
	in in_custom_flag boolean,
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32))
BEGIN
	-- call recon_flexi_prod_22072025.pr_get_role_reporttemplate_list(false, 'r', 'r', 'r');
	if in_custom_flag = true and 1 = 2 then
		select
			0 as rolereportpermission_gid,
            a.recon_code, rec.recon_name,
			a.reporttemplate_code,
			a.reporttemplate_name,
		  a.report_code,
			b.report_desc 
		from recon_mst_treporttemplate a
        inner join recon_mst_trecon rec on rec.recon_code=a.recon_code
		inner join recon_mst_treport b on a.report_code = b.report_code
			and b.delete_flag = 'N'
		where  a.active_status = 'Y' and a.delete_flag = 'N' 
       
       order by 1 desc;
	else
		  SELECT distinct
				 rp.rolereportpermission_gid,
                 	a.recon_code as 'Recon code',rec.recon_name as 'Recon Name',
					a.reporttemplate_code as 'Template code',
					a.reporttemplate_name as 'Template Name', 
					b.report_desc  as 'Report Desc',  
					ifnull( rp.CSVDownload, 'N') as "CSV",
					ifnull( rp.ExcelDownload, 'N') as "Excel",
					ifnull( rp.Preview, 'N') as "Preview",
                    case
					  when (ifnull(rp.CSVDownload, 'N') = 'N' and
							ifnull(rp.ExcelDownload, 'N') = 'N' and
							ifnull(rp.Preview, 'N') = 'N') then 'Y'
					  else 'N'
					end  as "Deny",  a.report_code  as 'Report code'
				FROM recon_mst_treporttemplate a
                  inner join recon_mst_trecon rec on rec.recon_code=a.recon_code
				INNER JOIN recon_mst_treport b 
					ON a.report_code = b.report_code
					AND b.delete_flag = 'N'
				LEFT JOIN admin_mst_tRoleReportPermission rp
					ON a.reporttemplate_code = rp.reporttemplate_code  and rp.Role_code=in_role_code  
				WHERE a.active_status = 'Y'
				  AND a.delete_flag = 'N'
		 
				UNION

				SELECT distinct
				rp.rolereportpermission_gid,
                	'' AS 'Recon code','' as 'Recon name', 
				 '' AS 'Template code',
					a.report_desc  as 'Template Name', 
					a.report_desc as 'Report Desc', 
				ifnull( rp.CSVDownload, 'N') as "CSV",
					ifnull( rp.ExcelDownload, 'N') as "Excel",
					ifnull( rp.Preview, 'N') as "Preview",
                    case
					  when (ifnull(rp.CSVDownload, 'N') = 'N' and
							ifnull(rp.ExcelDownload, 'N') = 'N' and
							ifnull(rp.Preview, 'N') = 'N') then 'Y'
					  else 'N'
					end  as "Deny",  a.report_code  as  'Report code'
				FROM recon_mst_treport a
				LEFT JOIN admin_mst_tRoleReportPermission rp
					ON a.report_code = rp.report_code  and rp.Role_code=in_role_code  and rp.reporttemplate_code='' and rp.recon_code=''
				WHERE a.active_status = 'Y'
				  AND a.report_exec_type <> 'D'
				  AND a.delete_flag = 'N'

				UNION

				SELECT distinct
				rp.rolereportpermission_gid,
                '' AS 'Recon code','' as 'Recon name', 
					''  AS 'Template code',
					a.report_desc   as 'Template Name', 
					a.report_desc as 'Report Desc', 
					ifnull( rp.CSVDownload, 'N') as "CSV",
					ifnull( rp.ExcelDownload, 'N') as "Excel",
					ifnull( rp.Preview, 'N') as "Preview",
                    case
					  when (ifnull(rp.CSVDownload, 'N') = 'N' and
							ifnull(rp.ExcelDownload, 'N') = 'N' and
							ifnull(rp.Preview, 'N') = 'N') then 'Y'
					  else 'N'
					end  as "Deny",  a.report_code as  'Report code'
				FROM recon_mst_treport a
				INNER JOIN recon_mst_trecondataset b
					ON a.report_code = b.dataset_code
					AND b.active_status = 'Y'
					AND b.delete_flag = 'N'
				LEFT JOIN admin_mst_tRoleReportPermission rp
					ON a.report_code = rp.report_code  and rp.Role_code=in_role_code and rp.reporttemplate_code='' and rp.recon_code=''
				WHERE a.active_status = 'Y'
				  AND a.report_exec_type = 'D';
				  
	end if;
     
END $$

DELIMITER ;