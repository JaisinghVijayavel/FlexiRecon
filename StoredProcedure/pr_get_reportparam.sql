DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reportparam` $$
CREATE PROCEDURE `pr_get_reportparam`
(
  in_report_code varchar(32),
  in_recon_code varchar(32)
)
BEGIN
    if exists(select recon_flag from recon_mst_treport
      where report_code = in_report_code
      and delete_flag = 'N') then
      select a.* from
      (
        select
          in_report_code as report_code,
          recon_field_name as reportparam_code,
          recon_field_desc as reportparam_value
        from recon_mst_treconfield
        where recon_code = in_recon_code
        and delete_flag = 'N'
        union
        SELECT
	        report_code,
          reportparam_code,
          reportparam_value
        FROM recon_mst_treportparam
        WHERE report_code = in_report_code
        and delete_flag = 'N'
      ) as a;
    else
      SELECT
	      report_code,reportparam_code,reportparam_value
      FROM recon_mst_treportparam
      WHERE report_code = in_report_code
      and delete_flag = 'N'
      order by reportparam_order;
    end if;
END $$

DELIMITER ;