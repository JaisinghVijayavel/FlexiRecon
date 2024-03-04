DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_paginationreport` $$
CREATE PROCEDURE `pr_get_paginationreport`()
BEGIN
  SELECT '-1' as report_gid,"--select--" as report_desc,'N' as delete_flag,'' as recon_gid_field,'' as table_name

  union all

  SELECT
    report_gid as report_gid,report_desc,delete_flag,recon_gid_field,table_name
  FROM recon_mst_treport
  WHERE pagination_flag = 'Y'
  and delete_flag = 'N';
END $$

DELIMITER ;