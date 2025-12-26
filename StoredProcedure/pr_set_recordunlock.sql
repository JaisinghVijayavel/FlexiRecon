DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_recordunlock` $$
CREATE PROCEDURE `pr_set_recordunlock`
(
  in in_recon_code varchar(32),
  in in_tran_gid int,
  in in_tranbrkp_gid int,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 22-12-2025

    Updated By :
    updated Date :

    Version : 1
  */

  if exists(select * from recon_trn_trecordlock
    where recon_code = in_recon_code
    and tran_gid = in_tran_gid
    and tranbrkp_gid = in_tranbrkp_gid
    and user_code = in_user_code
    and active_status = 'Y'
    and delete_flag = 'N') then

    delete from recon_trn_trecordlock
    where recon_code = in_recon_code
    and tran_gid = in_tran_gid
    and tranbrkp_gid = in_tranbrkp_gid
    and user_code = in_user_code
    and active_status = 'Y'
    and delete_flag = 'N';

    set out_msg = 'Success';
    set out_result = 1;
  else
    set out_msg = 'Failed';
    set out_result = 0;
  end if;
END $$

DELIMITER ;