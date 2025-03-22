DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_themePDclosing` $$
CREATE PROCEDURE `pr_set_themePDclosing`(
  in in_recon_code varchar(32),
  in in_dataset_code varchar(32),
  in in_unit_name varchar(255)
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 27-02-2025

    Updated By : Vijayavel
    updated Date : 20-03-2025

    Version : 3
  */

  declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';
	declare v_ds_table text default '';

	declare v_ds_dbname text default '';
  declare v_recon_cycle_date date;

  declare v_concurrent_ko_flag text default '';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(in_recon_code,'_tran');
	  set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  -- get recon_cycle_date
  set v_recon_cycle_date = fn_get_reconcycledate(in_recon_code);

  set v_ds_dbname = ifnull(fn_get_configvalue('dataset_db_name'),'');

  if v_ds_dbname = '' then
    set v_ds_table = in_dataset_code;
  else
    set v_ds_table = concat(v_ds_dbname,'.',in_dataset_code);
  end if;

  /*
  -- blank theme
  -- tran table
  set v_sql = concat("
    update ",v_tran_table,"
    set theme_code = ''
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);


  -- tranbrkp table
  set v_sql = concat("
    update ",v_tranbrkp_table,"
    set theme_code = ''
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);
  */

  -- clear status in dataset table
  set v_sql = concat("update ",v_ds_table,"
    set
      col8 = '',
      col9 = ''
    where col1 = '",in_unit_name,"'
    ",if(v_recon_cycle_date is null,"",concat("and col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- not posted cases
  call pr_set_themePDclosing4(in_recon_code,in_dataset_code,in_unit_name);
  call pr_set_themePDclosing5(in_recon_code,in_dataset_code,in_unit_name);
  call pr_set_themePDclosing6(in_recon_code,in_dataset_code,in_unit_name);

  drop temporary table if exists recon_tmp_ttranwithbrkp1;
  drop temporary table if exists recon_tmp_tuhidoutstanding;

  -- PD Recon Exception Table
  CREATE temporary TABLE recon_tmp_ttranwithbrkp1(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    bill_no varchar(255) default null,
    ipop_no varchar(255) default null,
    uhid_no varchar(255) default null,
    unit_name varchar(255) default null,
    balance_type varchar(255) default null,
    excp_value decimal(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    tally_status varchar(255) default null,
    PRIMARY KEY (tran_gid,tranbrkp_gid),
    key idx_bill_no(bill_no),
    key idx_ipop_no(ipop_no),
    key idx_uhid_no(uhid_no)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tuhidoutstanding(
    uhid_no varchar(255) not null,
    ipop_no varchar(255) not null,
    os_amount decimal(15,2) not null default 0,
    balance_type varchar(32) default null,
    tally_status varchar(255) default null,
    PRIMARY KEY (uhid_no,ipop_no)
  ) ENGINE = MyISAM;

  -- insert records from tran table
  set v_sql = concat("insert into recon_tmp_ttranwithbrkp1
    (tran_gid,tranbrkp_gid,bill_no,ipop_no,uhid_no,excp_value,tran_mult,unit_name)
    select tran_gid,0,col2,col4,col3,excp_value,tran_mult,'",in_unit_name,"' from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and (theme_code = '' or theme_code = 'Consider for CB IP Refund' or theme_code like 'Consider for CB IP Refund,%')
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("insert into recon_tmp_ttranwithbrkp1
    (tran_gid,tranbrkp_gid,bill_no,ipop_no,uhid_no,excp_value,tran_mult,unit_name)
    select tran_gid,tranbrkp_gid,col2,col4,col3,excp_value,tran_mult,'",in_unit_name,"' from ",v_tranbrkp_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and tran_gid > 0

    /*
    and col1 <> 'DIGITAL  TESTING'
    and col5 <> 'CREDIT NOTE REFUND'
    and col5 <> 'DEBIT NOTE'
    and col5 <> 'BILL REALIZATION'
    and (col16 is null or col16 = '')
    and cast(col19 as unsigned) = 0
    and cast(col18 as unsigned) >= cast(col19 as unsigned)
    */

    and (theme_code = '' or theme_code = 'Consider for CB IP Refund' or theme_code like 'Consider for CB IP Refund,%')
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  update recon_tmp_ttranwithbrkp1 set uhid_no = '' where uhid_no is null;
  update recon_tmp_ttranwithbrkp1 set ipop_no = '' where ipop_no is null;

  -- uhid outstanding
  insert into recon_tmp_tuhidoutstanding (uhid_no,ipop_no,os_amount)
    select uhid_no,ipop_no,sum(excp_value*tran_mult) from recon_tmp_ttranwithbrkp1
    group by uhid_no,ipop_no;

  -- update in dataset table based on uhid no and unit
  -- col2 = balance_type col3 = uhid_no, col5 = ipop_no, col6 = outstanding amount,col9 - Status Update
  set v_sql = concat("update recon_tmp_tuhidoutstanding as a
    inner join ",v_ds_table," as b on a.uhid_no = b.col3
    and a.ipop_no = b.col5
    and a.os_amount = cast(b.col6 as decimal(15,2))
    and cast(b.col6 as decimal(15,2)) <> 0
    and b.col1 = '",in_unit_name,"'
    and b.col2 <> 'UHID - Deposit CB'
    and b.col9 <> 'TALLIED'
    ",if(v_recon_cycle_date is null,"",concat("and b.col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
    and b.delete_flag = 'N'
    set
      b.col8 = cast(os_amount as nchar),
      b.col9 = 'TALLIED',
      a.balance_type = b.col2,
      a.tally_status = 'TALLIED'");

  call pr_run_sql(v_sql,@msg,@result);

  -- not tallied
  set v_sql = concat("update recon_tmp_tuhidoutstanding as a
    inner join ",v_ds_table," as b on a.uhid_no = b.col3
    and a.ipop_no = b.col5
    and a.os_amount <> cast(b.col6 as decimal(15,2))
    and cast(b.col6 as decimal(15,2)) <> 0
    and b.col1 = '",in_unit_name,"'
    and b.col2 <> 'UHID - Deposit CB'
    and b.col9 <> 'TALLIED'
    and b.col9 <> 'NOT POSTED'
    ",if(v_recon_cycle_date is null,"",concat("and b.col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
    and b.delete_flag = 'N'
    set
      b.col8 = cast(os_amount as nchar),
      b.col9 = 'NOT TALLIED',
      a.balance_type = b.col2,
      a.tally_status = 'NOT TALLIED'");

  call pr_run_sql(v_sql,@msg,@result);

  -- update tallied
  -- update in tranwithbrkp table
  update recon_tmp_tuhidoutstanding as a
  inner join recon_tmp_ttranwithbrkp1 as b on a.uhid_no = b.uhid_no and a.ipop_no = b.ipop_no
  set
    b.tally_status = a.tally_status,
    b.balance_type = a.balance_type
  where a.tally_status = 'TALLIED';

  -- update in tran table
  set v_sql = concat("
    update ",v_tran_table," as a
    inner join recon_tmp_ttranwithbrkp1 as b on a.tran_gid=b.tran_gid and b.tranbrkp_gid = 0
    set a.theme_code = b.balance_type
    where 1 = 1
    and b.tally_status = 'TALLIED'
    and (a.theme_code = '' or a.theme_code = 'Consider for CB IP Refund' or theme_code like 'Consider for CB IP Refund,%')");

  call pr_run_sql(v_sql,@msg,@result);

  -- update in tranbrkp table
  set v_sql = concat("
    update ",v_tranbrkp_table," as a
    inner join recon_tmp_ttranwithbrkp1 as b on a.tranbrkp_gid = b.tranbrkp_gid
      and a.tran_gid=b.tran_gid
    set a.theme_code = b.balance_type
    where 1 = 1
    and b.tally_status = 'TALLIED'
    and (a.theme_code = '' or a.theme_code = 'Consider for CB IP Refund' or theme_code like 'Consider for CB IP Refund,%') ");

  call pr_run_sql(v_sql,@msg,@result);

  call pr_set_themePDclosing20(in_recon_code,in_dataset_code,in_unit_name);
  call pr_set_themePDclosing2(in_recon_code,in_dataset_code,in_unit_name);
  call pr_set_themePDclosing3(in_recon_code,in_dataset_code,in_unit_name);

  call pr_set_themePDclosing7(in_recon_code,in_dataset_code,in_unit_name);

  -- update net exception
  -- update in tran table
  set v_sql = concat("
    update ",v_tran_table,"
    set col12 = cast(excp_value*tran_mult as nchar)
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- update in tranbrkp table
  set v_sql = concat("
    update ",v_tranbrkp_table,"
    set col12 = cast(excp_value*tran_mult as nchar)
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);
end $$

DELIMITER ;