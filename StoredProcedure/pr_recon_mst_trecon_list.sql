DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_trecon_list` $$
CREATE PROCEDURE `pr_recon_mst_trecon_list`
(
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32)
)
BEGIN
  declare v_master_sys_code text default '';
  set v_master_sys_code = (select master_syscode from admin_mst_tusercontext
    where user_code=in_user_code
    and parent_master_syscode='QCD_L3'
    and delete_flag = 'N'
  );

  select
    a.recon_gid,
    a.recon_code,
    a.recon_name,
    a.recontype_code,
    fn_get_mastername(a.recontype_code, 'QCD_RC_RCON_TYPE') as recontype_desc,
    date_format(a.period_from,'%d/%m/%Y') as period_from,
    date_format( a.period_to,'%d/%m/%Y') as period_to,
    a.until_active_flag,
	  a.active_status,
    fn_get_mastername(a.active_status, 'QCD_STATUS') as active_status_desc
  from recon_mst_trecon a
  inner join admin_mst_treconcontext b on a.recon_code=b.recon_code
    and b.delete_flag = 'N'
  where a.delete_flag = 'N'
    and b.master_syscode=v_master_sys_code
  order by recon_gid desc;
  END $$

DELIMITER ;