DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_checkedvalues` $$
CREATE PROCEDURE `pr_get_checkedvalues`
(
  In in_user_gid int,
  in in_user_code varchar(128)
)
BEGIN
  select distinct
    parent_level_code as master_code,
    master_name
  from admin_mst_tuserlevelmapping a
  inner join recon_mst_tmaster b on a.level_code=b.master_code
    and b.delete_flag = 'N'
  where a.user_code=in_user_code
  and a.delete_flag = 'N';
END $$

DELIMITER ;