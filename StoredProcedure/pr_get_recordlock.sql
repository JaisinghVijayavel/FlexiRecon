DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_recordlock` $$
CREATE PROCEDURE `pr_get_recordlock`
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

	declare v_user_code text default '';
  declare v_record_lock_minutes text default '';
  declare v_lock_expiry_datetime datetime;

  set out_msg = 'Success';
  set out_result = 1;

  -- get record lock minutes
  set v_record_lock_minutes = fn_get_configvalue('record_lock_minutes');

  if v_record_lock_minutes = '' then
    set v_record_lock_minutes = '3';
  end if;

  if exists(select * from recon_trn_trecordlock
    where recon_code = in_recon_code
    and tran_gid = in_tran_gid
    and tranbrkp_gid = in_tranbrkp_gid
    and active_status = 'Y'
    and delete_flag = 'N') then

    select
      user_code,lock_expiry_datetime
    into
      v_user_code,v_lock_expiry_datetime
    from recon_trn_trecordlock
    where recon_code = in_recon_code
    and tran_gid = in_tran_gid
    and tranbrkp_gid = in_tranbrkp_gid
    and active_status = 'Y'
    and delete_flag = 'N';

    set v_user_code = ifnull(v_user_code,'');
    set v_lock_expiry_datetime = ifnull(v_lock_expiry_datetime,sysdate());

    if v_lock_expiry_datetime > sysdate() then
      if v_user_code <> in_user_code then
        set out_msg = concat('Failed, Record was locked by ',v_user_code);
        set out_result = 2;
      end if;
    end if;
  else
    set out_msg = 'Failed';
    set out_result = 0;
  end if;

  -- return the result
  select * from recon_trn_trecordlock
  where recon_code = in_recon_code
  and tran_gid = in_tran_gid
  and tranbrkp_gid = in_tranbrkp_gid
  and delete_flag = 'N';
END $$

DELIMITER ;