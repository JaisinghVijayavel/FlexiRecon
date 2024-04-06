DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_themeclonelist` $$
CREATE PROCEDURE `pr_get_themeclonelist`
(
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  select
    theme_code,
    concat(theme_code,' - ',theme_desc) as theme_desc
  from recon_mst_ttheme
  where active_status='Y'
  and delete_flag = 'N'
  order by theme_code;
END $$

DELIMITER ;