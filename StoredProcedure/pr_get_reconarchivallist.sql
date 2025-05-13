DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reconarchivallist` $$
CREATE PROCEDURE `pr_get_reconarchivallist`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32)
)
BEGIN
  declare v_app_datetime_format text default '';

  set v_app_datetime_format = fn_get_configvalue('app_datetime_format');

  select
    reconarchival_gid,
    archival_code,
    date_format(archival_date,v_app_datetime_format) as archival_date,
    archival_by,
    archival_db_name
  from recon_trn_treconarchival
  where recon_code = in_recon_code
  and active_status ='Y'
  and delete_flag = 'N'
  order by 1 desc;
END $$

DELIMITER ;