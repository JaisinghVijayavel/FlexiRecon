DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_roleaccess` $$
CREATE PROCEDURE `pr_fetch_roleaccess`
(
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  select
    b.menu_gid,
    a.menu_name as "Menu",
    b.menu_name as "Sub Menu",
    ifnull(c.add_flag, 'N') as "Add",
    ifnull(c.modify_flag, 'N') as "Modify",
    ifnull(c.view_flag, 'N') as "View",
    ifnull(c.download_flag, 'N') as "Download",
    ifnull(c.deleteflag, 'N') as "Delete",
    ifnull(c.process_flag, 'N') as "Process",
    case
      when (ifnull(c.add_flag, 'N') = 'N' and
            ifnull(c.modify_flag, 'N') = 'N' and
            ifnull(c.view_flag, 'N') = 'N' and
            ifnull(c.download_flag, 'N') = 'N' and
            ifnull(c.deleteflag, 'N') = 'N' and
            ifnull(c.process_flag, 'N') = 'N') then 'Y'
      else 'N'
    end  as "Deny",
    b.menu_code as "menu_code"
  from admin_mst_tmenu as a
  inner join admin_mst_tmenu as b on a.menu_gid = b.parent_menu_code and b.delete_flag = 'N'
  left join admin_mst_trolerights as c on c.menu_code = b.menu_code
    and c.role_code = in_role_code
    and c.delete_flag = 'N'
  where a.delete_flag = 'N'
  order by b.parent_menu_code, b.menu_name;
END $$

DELIMITER ;