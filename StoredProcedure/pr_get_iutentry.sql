DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_iutentry` $$
CREATE PROCEDURE `pr_get_iutentry`
(
  in in_recon_code varchar(32),
  in in_iutentryref_no text,
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 19-03-2025

    Updated By : Vijayavel
    updated Date :

    Version : 1
  */

  select
    recon_code as 'Recon Code',
    entry_ref_no as 'IUT Entry No',
    from_unit_name as 'From Unit',
    to_unit_name as 'To Unit',
    dr_amount as 'Debit',
    cr_amount as 'Credit',
    uhid_no as 'UHID No',
    bill_no as 'Bill No',
    ipop_no as 'IP/OP No',
    iut_ipop as 'IUT IP/OP',
    iutentry_status as 'IUT Entry Status',
    entry_date as 'Entry Date',
    ref_tran_gid as 'Tran Id',
    ref_tranbrkp_gid as 'Supporting Tran Id',
    scheduler_gid as 'Scheduler Id',
    iutentry_gid as 'IUT Entry Id'
  from recon_trn_tiutentry
  where recon_code = in_recon_code
  and entry_ref_no = in_iutentryref_no
  and delete_flag = 'N';

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;