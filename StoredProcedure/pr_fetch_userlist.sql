DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_userlist` $$
CREATE PROCEDURE `pr_fetch_userlist`
(
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
me:BEGIN
	SELECT 
		user_gid,
		user_code,
		user_name,
		user_contactno,
		user_emailid,
		a.role_code,
		ifnull(role_name,'') as role_name,
		user_password
	FROM admin_mst_tuser a
	left join admin_mst_trole b on a.role_code=b.role_code
		and b.delete_flag = 'N' 
	where a.user_status='A'
	and a.delete_flag = 'N';
END $$

DELIMITER ;