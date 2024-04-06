DELIMITER $$
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
    Updated Date : 26-03-2024

    Version : 1
  */

  -- update the recon_trn_ttran
  update recon_trn_tthemeupdate as a
  inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid and b.delete_flag = 'N'
  set b.theme_code = a.theme_desc,
      a.theme_status = 'C'
  where a.scheduler_gid = in_scheduler_gid
  and a.theme_status = 'P'
  and a.tranbrkp_gid = 0
  and a.delete_flag = 'N';

  -- update the recon_trn_ttranbrkp
  update recon_trn_tthemeupdate as a
  inner join recon_trn_ttranbrkp as b on a.tranbrkp_gid = b.tranbrkp_gid and b.delete_flag = 'N'
  set b.theme_code = a.theme_desc,
      a.theme_status = 'C'
  where a.scheduler_gid = in_scheduler_gid
  and a.theme_status = 'P'
  and a.delete_flag = 'N';

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
