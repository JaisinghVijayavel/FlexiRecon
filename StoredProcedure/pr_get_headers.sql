DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_headers` $$
CREATE PROCEDURE `pr_get_headers`
(
  In in_user_gid int,
  in in_user_code varchar(128)
)
BEGIN
  select distinct
    master_code,
    master_name,
    b.depend_master_syscode as depend_code,
    parent_master_syscode as parent_code
  from admin_mst_tuserlevelmapping a
  inner join recon_mst_tmaster b on a.level_code=b.master_code
    and b.parent_master_syscode !='QCD_L1'
    and b.delete_flag = 'N'
  where a.user_code=in_user_code
  and a.delete_flag = 'N'

  union

  select distinct
    master_code,
    master_name,
    ifnull(depend_master_syscode,0) as depend_code,
    parent_master_syscode as parent_code
  from recon_mst_tmaster
  where master_code in
  (
    select depend_master_syscode from recon_mst_tmaster
    where master_code in
    (
      select distinct depend_master_syscode as master_code from admin_mst_tuserlevelmapping a
      inner join recon_mst_tmaster b on a.level_code=b.master_code and b.delete_flag = 'N'
      where a.user_code=in_user_code
      and a.delete_flag = 'N'
    )
  )
  and delete_flag = 'N'

  union

  select distinct
    master_code,
    master_name,
    depend_master_syscode as depend_code,
    parent_master_syscode as parent_code
  from recon_mst_tmaster
  where master_code in
  (
    select distinct depend_master_syscode as master_code from admin_mst_tuserlevelmapping a
    inner join recon_mst_tmaster b on a.level_code=b.master_code and b.delete_flag = 'N'
    where a.user_code=in_user_code
    and a.delete_flag = 'N'
  ) and parent_master_syscode !='QCD_L1'
  and delete_flag = 'N';
END $$

DELIMITER ;