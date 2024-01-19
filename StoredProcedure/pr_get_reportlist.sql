DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reportlist` $$
CREATE PROCEDURE `pr_get_reportlist`()
BEGIN
  select
    report_gid,
    report_code,
    report_desc
  from recon_mst_treport
  where delete_flag = 'N'
  order by display_order;
END $$

DELIMITER ;