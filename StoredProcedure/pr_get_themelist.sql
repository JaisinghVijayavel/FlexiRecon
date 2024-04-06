DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_themelist` $$
CREATE PROCEDURE `pr_get_themelist`
(
  in in_recon_code  varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  select
    theme_gid,
    theme_code,
    theme_desc,
    a.recon_code,
    theme_order,
    b.recon_name,
    a.active_status,
    fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc,
    a.hold_flag,
    fn_get_mastername(a.hold_flag, 'QCD_YN') as hold_flag_desc
  from recon_mst_ttheme  a
  inner join recon_mst_trecon b on a.recon_code=b.recon_code
    and b.delete_flag = 'N'
  where a.recon_code=in_recon_code
  and a.delete_flag = 'N';
END $$

DELIMITER ;