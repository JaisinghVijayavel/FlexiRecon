DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_undoiutentry` $$
CREATE PROCEDURE `pr_set_undoiutentry`
(
  in in_recon_code varchar(32),
  in in_iutentryref_no text,
  in in_system_flag char(1),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 02-04-2025

    Version : 2
  */

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

  declare v_entryrefno_field text default '';

  declare v_sql text default '';

	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  set v_entryrefno_field = 'col51';

  -- check system/manual entry
  set in_system_flag = ifnull(in_system_flag,'');

  if in_system_flag <> 'Y' then
    set in_system_flag = 'N';
  end if;

  if in_system_flag = 'N' then
    -- manual entry
    if not exists(select * from recon_trn_tiutentry
      where recon_code = in_recon_code
      and entry_Ref_no = in_iutentryref_no
      and iutentry_status = 'C'
      and delete_flag = 'N') then

      set out_msg = 'Falied';
      set out_result = 0;
      leave me;
    end if;
  else
    -- system entry
    if exists(select * from recon_trn_tiutentry
      where recon_code = in_recon_code
      and entry_Ref_no = in_iutentryref_no
      and iutentry_status = 'C'
      and delete_flag = 'N') then

      set out_msg = 'Falied';
      set out_result = 0;
      leave me;
    end if;
  end if;

  -- undo in tranbrkp table
  set v_sql = concat("update ",v_tranbrkp_table," set
      delete_flag = 'Y'
    where recon_code = '",in_recon_code,"'
    and ",v_entryrefno_field," = '",in_iutentryref_no,"'
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);

  -- col46 = IUT Value
  -- col47 = IUT IP/OP
  -- col51 = Entry Ref No
  -- col53 = Closing Balance

  set v_sql = concat("update ",v_tran_table," set
      col46 = '0.00',
      col47 = '',
      col51 = null,
      col53 = col37
    where recon_code = '",in_recon_code,"'
    and ",v_entryrefno_field," = '",in_iutentryref_no,"'
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);


  if in_system_flag = 'N' then
    -- manual entry
    -- update in iutentry table
    update recon_trn_tiutentry set
      iutentry_status = 'U',
      update_by = in_user_code,
      update_date = sysdate()
    where recon_code = in_recon_code
    and entry_ref_no = in_iutentryref_no
    and iutentry_status = 'C'
    and delete_flag = 'N';
  end if;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;