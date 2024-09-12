DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_linecategory` $$
CREATE PROCEDURE `pr_set_linecategory`
(
  in in_recon_code varchar(32)
)
me:begin
  declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';
	declare v_ds_table text default '';

	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  leave me;

  -- clear line category
  set v_sql = concat("
    update ",v_tran_table,"
    set col13 = ''
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("
    update ",v_tranbrkp_table,"
    set col13 = ''
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- line category update
  -- Credit Memos
  set v_sql = concat("
    update ",v_tran_table,"
    set col13 = 'Credit Memos'
    where recon_code = '",in_recon_code,"'
    and col1 = 'Credit Memos'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- Debit Memos
  set v_sql = concat("
    update ",v_tran_table,"
    set col13 = 'Debit Memos'
    where recon_code = '",in_recon_code,"'
    and col1 = 'Debit Memos'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- Sales Invoice
  set v_sql = concat("
    update ",v_tran_table,"
    set col13 = 'Sales Invoices'
    where recon_code = '",in_recon_code,"'
    and col1 = 'Sales Invoices'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- Misc Receipts
  set v_sql = concat("
    update ",v_tran_table,"
    set col13 = 'Misc Receipts'
    where recon_code = '",in_recon_code,"'
    and col1 in ('Misc Receipts','Opening Balance IP Deposits','Opening Balance IP Refund','Opening Balance UHID Deposits','Opening Balance Bill Realisation')
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- update in tran table
  set v_sql = concat("
    update ",v_tran_table," set
      theme_code = 'Unreconciled',
      col13 = 'Unreconciled OB'
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and col1 = 'Opening Balance Unreconciled'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- CMH Manual
  set v_sql = concat("
    update ",v_tran_table," set
      col13 = 'Not Related to PD'
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and col1 = 'CMH Manual'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- Opening Balance CMH Manual
  set v_sql = concat("
    update ",v_tran_table," set
      col13 = 'Not Related to PD'
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and col1 = 'Opening Balance CMH Manual'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- Entry to be Passed in OF
  -- update in tran table
  set v_sql = concat("
    update ",v_tran_table," set
      col13 = 'Entry to be Passed in OF'
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and col1 = 'Deposits Ledger to be corrected in Oracle'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- Payroll
  -- update in tran table
  set v_sql = concat("
    update ",v_tran_table," set
      col13 = 'Not Related to PD'
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and col1 = 'Excelity_Payroll_Salary'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);
end $$

DELIMITER ;