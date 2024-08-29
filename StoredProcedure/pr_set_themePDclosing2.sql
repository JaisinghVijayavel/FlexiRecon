DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_themePDclosing2` $$
CREATE PROCEDURE `pr_set_themePDclosing2`
(
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

	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  set v_ds_dbname = ifnull(fn_get_configvalue('dataset_db_name'),'');

  if v_ds_dbname = '' then
    set v_ds_table = in_dataset_code;
  else
    set v_ds_table = concat(v_ds_dbname,'.',in_dataset_code);
  end if;

  drop temporary table if exists recon_tmp_ttranwithbrkp;
  drop temporary table if exists recon_tmp_tuhidoutstanding;

  -- PD Recon Exception Table
  CREATE temporary TABLE recon_tmp_ttranwithbrkp(
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
  set v_sql = concat("insert into recon_tmp_ttranwithbrkp
    (tran_gid,tranbrkp_gid,bill_no,ipop_no,uhid_no,excp_value,tran_mult,unit_name)
    select tran_gid,0,col2,col4,col3,excp_value,tran_mult,'",in_unit_name,"' from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and theme_code = ''
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("insert into recon_tmp_ttranwithbrkp
    (tran_gid,tranbrkp_gid,bill_no,ipop_no,uhid_no,excp_value,tran_mult,unit_name)
    select tran_gid,tranbrkp_gid,col2,col4,col3,excp_value,tran_mult,'",in_unit_name,"' from ",v_tranbrkp_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and tran_gid > 0
    and theme_code = ''

    and col1 <> 'DIGITAL  TESTING'
    and col5 <> 'CREDIT NOTE REFUND'
    and col5 <> 'DEBIT NOTE'
    and col5 <> 'BILL REALIZATION'

    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- uhid outstanding
  insert into recon_tmp_tuhidoutstanding (uhid_no,ipop_no,os_amount)
    select ifnull(uhid_no,''),'',sum(excp_value*tran_mult) from recon_tmp_ttranwithbrkp
    group by uhid_no;

  -- update in dataset table based on uhid no and unit
  -- col2 = balance_type col3 = uhid_no, col5 = ipop_no, col6 = outstanding amount
  set v_sql = concat("update recon_tmp_tuhidoutstanding as a
    inner join ",v_ds_table," as b on a.uhid_no = b.col3
    and a.os_amount = cast(col6 as decimal(15,2))
    and b.col1 = '",in_unit_name,"'
    and b.col2 = 'UHID - Deposit CB'
    and b.col9 <> 'TALLIED'
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
    and a.os_amount <> cast(col6 as decimal(15,2))
    and b.col1 = '",in_unit_name,"'
    and b.col2 = 'UHID - Deposit CB'
    and b.col9 <> 'TALLIED'
    and b.col9 <> 'NOT POSTED'
    and b.delete_flag = 'N'
    set
      b.col8 = cast(os_amount as nchar),
      b.col9 = 'NOT TALLIED',
      a.balance_type = b.col2,
      a.tally_status = 'NOT TALLIED'");

  call pr_run_sql(v_sql,@msg,@result);

  -- update in tranwithbrkp table
  update recon_tmp_tuhidoutstanding as a
  inner join recon_tmp_ttranwithbrkp as b on a.uhid_no = b.uhid_no
  set
    b.tally_status = a.tally_status,
    b.balance_type = a.balance_type
  where a.tally_status = 'TALLIED';

  -- update in tran table
  set v_sql = concat("
    update ",v_tran_table," as a
    inner join recon_tmp_ttranwithbrkp as b on a.tran_gid=b.tran_gid and b.tranbrkp_gid = 0
    set a.theme_code = b.balance_type
    where b.tally_status = 'TALLIED'
    and a.theme_code = ''");

  call pr_run_sql(v_sql,@msg,@result);

  -- update in tranbrkp table
  set v_sql = concat("
    update ",v_tranbrkp_table," as a
    inner join recon_tmp_ttranwithbrkp as b on a.tranbrkp_gid = b.tranbrkp_gid
      and a.tran_gid=b.tran_gid
    set a.theme_code = b.balance_type
    where b.tally_status = 'TALLIED'
    and a.theme_code = ''");

  call pr_run_sql(v_sql,@msg,@result);

  /*
  select * from recon_tmp_tuhidoutstanding;
  select * from recon_tmp_ttranwithbrkp;
  */

  drop temporary table recon_tmp_ttranwithbrkp;
  drop temporary table recon_tmp_tuhidoutstanding;
end $$

DELIMITER ;