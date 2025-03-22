﻿DELIMITER $$
DROP PROCEDURE IF EXISTS `pr_set_themeupdate` $$
CREATE procedure `pr_set_themeupdate`
(
  in in_scheduler_gid int,
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 26-03-2024

    Updated By : Vijayavel
    Updated Date : 19-03-2025

    Version : 2
  */

  declare v_recon_code text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

  declare v_concurrent_ko_flag text default '';

  declare v_sql text default '';

  -- get recon code
  select
    recon_code into v_recon_code
  from recon_trn_tthemeupdate
  where scheduler_gid = in_scheduler_gid
  and recon_code <> ''
  and delete_flag = 'N';

  set v_recon_code = ifnull(v_recon_code,'');

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(v_recon_code,'_tran');
	  set v_tranbrkp_table = concat(v_recon_code,'_tranbrkp');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  -- update the recon_trn_ttran
  set v_sql = concat("
    update recon_trn_tthemeupdate as a
    inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid and b.delete_flag = 'N'
    set b.theme_code = a.theme_desc,
      a.theme_status = 'C'
    where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and a.theme_status = 'P'
    and a.tranbrkp_gid = 0
    and a.delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- update the recon_trn_ttranbrkp
  set v_sql = concat("
    update recon_trn_tthemeupdate as a
    inner join ",v_tranbrkp_table," as b on a.tranbrkp_gid = b.tranbrkp_gid and b.delete_flag = 'N'
    set b.theme_code = a.theme_desc,
      a.theme_status = 'C'
    where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and a.theme_status = 'P'
    and a.delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- update failed cases
  update recon_trn_tthemeupdate
  set theme_status = 'F'
  where scheduler_gid = in_scheduler_gid
  and theme_status = 'P'
  and delete_flag = 'N';

  set out_msg = 'Success';
  set out_result = 0;
end $$

DELIMITER ;