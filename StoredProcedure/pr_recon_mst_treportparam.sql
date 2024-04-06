DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treportparam` $$
CREATE PROCEDURE `pr_recon_mst_treportparam`
(
  in_report_code varchar(32)
)
BEGIN
    SELECT
	    report_code,
      concat(ifnull(reportparam_prefix,''),reportparam_code) as reportparam_code,
      reportparam_value,
      delete_flag
    FROM recon_mst_treportparam
    WHERE report_code = in_report_code
    and delete_flag = 'N'
    order by reportparam_order;
END $$

DELIMITER ;