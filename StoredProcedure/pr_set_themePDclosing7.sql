DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_themePDclosing7` $$
CREATE PROCEDURE `pr_set_themePDclosing7`(
  in in_recon_code varchar(32),
  in in_dataset_code varchar(32),
  in in_unit_name varchar(255)
)
me:begin
  declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';
	declare v_ds_table text default '';

	declare v_ds_dbname text default '';
  declare v_recon_cycle_date date;

  -- get recon_cycle_date
  set v_recon_cycle_date = fn_get_reconcycledate(in_recon_code);

	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  set v_ds_dbname = ifnull(fn_get_configvalue('dataset_db_name'),'');

  if v_ds_dbname = '' then
    set v_ds_table = in_dataset_code;
  else
    set v_ds_table = concat(v_ds_dbname,'.',in_dataset_code);
  end if;

  -- Recon Field
  -- col2 - Bill No
  -- col3 - Registration No
  -- col4 - IP/OP No

  -- not tallied based on uhid and ip
  set v_sql = concat("update ",v_tran_table," as a
    inner join ",v_ds_table," as b on a.col3 = b.col3
      and a.col4 = b.col5
      and b.col1 = '",in_unit_name,"'
      and b.col9 <> 'TALLIED'
    ",if(v_recon_cycle_date is null,"",concat("and b.col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
      and b.delete_flag = 'N'
    set
      a.theme_code = b.col2
    where a.recon_code = '",in_recon_code,"'
    and (a.theme_code = '' or a.theme_code = 'Consider for CB IP Refund')
    and a.delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("update ",v_tranbrkp_table," as a
    inner join ",v_ds_table," as b on a.col3 = b.col3
      and a.col4 = b.col5
      and b.col1 = '",in_unit_name,"'
      and b.col9 <> 'TALLIED'
    ",if(v_recon_cycle_date is null,"",concat("and b.col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
      and b.delete_flag = 'N'
    set
      a.theme_code = b.col2
    where a.recon_code = '",in_recon_code,"'
    and (a.theme_code = '' or a.theme_code = 'Consider for CB IP Refund')
    and a.tran_gid > 0
    and a.delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- not tallied based on uhid only - first preference UHID Deposits
  set v_sql = concat("update ",v_tran_table," as a
    inner join ",v_ds_table," as b on a.col3 = b.col3
      and b.col1 = '",in_unit_name,"'
      and b.col2 = 'UHID - Deposit CB'
      and b.col9 <> 'TALLIED'
    ",if(v_recon_cycle_date is null,"",concat("and b.col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
      and b.delete_flag = 'N'
    set
      a.theme_code = b.col2
    where a.recon_code = '",in_recon_code,"'
    and (a.col4 = a.col3 or a.col4 = '')
    and (a.theme_code = '' or a.theme_code = 'Consider for CB IP Refund')
    and a.delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("update ",v_tranbrkp_table," as a
    inner join ",v_ds_table," as b on a.col3 = b.col3
      and b.col1 = '",in_unit_name,"'
      and b.col2 = 'UHID - Deposit CB'
      and b.col9 <> 'TALLIED'
    ",if(v_recon_cycle_date is null,"",concat("and b.col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
      and b.delete_flag = 'N'
    set
      a.theme_code = b.col2
    where a.recon_code = '",in_recon_code,"'
    and (a.col4 = a.col3 or a.col4 = '')
    and (a.theme_code = '' or a.theme_code = 'Consider for CB IP Refund')
    and a.tran_gid > 0
    and a.delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- not tallied based on uhid only
  set v_sql = concat("update ",v_tran_table," as a
    inner join ",v_ds_table," as b on a.col3 = b.col3
      and b.col1 = '",in_unit_name,"'
      and b.col9 <> 'TALLIED'
    ",if(v_recon_cycle_date is null,"",concat("and b.col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
      and b.delete_flag = 'N'
    set
      a.theme_code = b.col2
    where a.recon_code = '",in_recon_code,"'
    and (a.theme_code = '' or a.theme_code = 'Consider for CB IP Refund')
    and a.delete_flag = 'N'");

    -- and (a.col4 = a.col3 or a.col4 = '')

  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("update ",v_tranbrkp_table," as a
    inner join ",v_ds_table," as b on a.col3 = b.col3
      and b.col1 = '",in_unit_name,"'
      and b.col9 <> 'TALLIED'
    ",if(v_recon_cycle_date is null,"",concat("and b.col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
      and b.delete_flag = 'N'
    set
      a.theme_code = b.col2
    where a.recon_code = '",in_recon_code,"'
    and (a.theme_code = '' or a.theme_code = 'Consider for CB IP Refund')
    and a.tran_gid > 0
    and a.delete_flag = 'N'");

    -- and (a.col4 = a.col3 or a.col4 = '')

  call pr_run_sql(v_sql,@msg,@result);
end $$

DELIMITER ;