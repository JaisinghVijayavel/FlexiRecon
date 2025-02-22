DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_themePDclosing5` $$
CREATE PROCEDURE `pr_set_themePDclosing5`
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

  drop temporary table if exists recon_tmp_ttranwithbrkp1;
  drop temporary table if exists recon_tmp_tuhidoutstanding;
  drop temporary table if exists recon_tmp_tnotposted;

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

  CREATE temporary TABLE recon_tmp_tnotposted(
    uhid_no varchar(255) not null,
    ipop_no varchar(255) not null,
    PRIMARY KEY (uhid_no,ipop_no)
  ) ENGINE = MyISAM;

  -- not posted cases
  set v_sql = concat("insert into recon_tmp_tnotposted
    (ipop_no,uhid_no)
    select distinct col4,col3 from ",v_tranbrkp_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and tran_gid = 0
    and col4 <> ''
    and col3 <> ''
    and (theme_code = '' or theme_code = 'Consider for CB IP Refund')

    /*
    and col1 <> 'DIGITAL  TESTING'
    and col5 <> 'CREDIT NOTE REFUND'
    and col5 <> 'DEBIT NOTE'
    and col5 <> 'BILL REALIZATION'
    and (col16 is null or col16 = '')
    and cast(col19 as unsigned) = 0
    and cast(col18 as unsigned) >= cast(col19 as unsigned)
    */

    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  if not exists(select * from recon_tmp_tnotposted) then
    leave me;
  end if;

  -- insert records from tran table
  set v_sql = concat("insert into recon_tmp_ttranwithbrkp1
    (tran_gid,tranbrkp_gid,bill_no,ipop_no,uhid_no,excp_value,tran_mult,unit_name)
    select tran_gid,0,col2,col4,col3,excp_value,tran_mult,'",in_unit_name,"' from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and (col3,col4) in (select uhid_no,ipop_no from recon_tmp_tnotposted)
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("insert into recon_tmp_ttranwithbrkp1
    (tran_gid,tranbrkp_gid,bill_no,ipop_no,uhid_no,excp_value,tran_mult,unit_name)
    select tran_gid,tranbrkp_gid,col2,col4,col3,excp_value,tran_mult,'",in_unit_name,"' from ",v_tranbrkp_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and tran_gid > 0
    and (col3,col4) in (select uhid_no,ipop_no from recon_tmp_tnotposted)
    and (theme_code = '' or theme_code = 'Consider for CB IP Refund') 

    /*
    and col1 <> 'DIGITAL  TESTING'
    and col5 <> 'CREDIT NOTE REFUND'
    and col5 <> 'DEBIT NOTE'
    and col5 <> 'BILL REALIZATION'
    and (col16 is null or col16 = '')
    and cast(col19 as unsigned) = 0
    and cast(col18 as unsigned) >= cast(col19 as unsigned)
    */

    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- not posted tranbrkp
  set v_sql = concat("insert into recon_tmp_ttranwithbrkp1
    (tran_gid,tranbrkp_gid,bill_no,ipop_no,uhid_no,excp_value,tran_mult,unit_name)
    select tran_gid,tranbrkp_gid,col2,col4,col3,excp_value,tran_mult,'",in_unit_name,"' from ",v_tranbrkp_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and tran_gid = 0
    and (col3,col4) in (select uhid_no,ipop_no from recon_tmp_tnotposted)
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- uhid outstanding
  insert into recon_tmp_tuhidoutstanding (uhid_no,ipop_no,os_amount)
    select ifnull(uhid_no,''),'',sum(excp_value*tran_mult) from recon_tmp_ttranwithbrkp1
    group by uhid_no;

  -- update in dataset table based on uhid no and unit
  -- col2 = balance_type col3 = uhid_no, col5 = ipop_no, col6 = outstanding amount
  set v_sql = concat("update recon_tmp_tuhidoutstanding as a
    inner join ",v_ds_table," as b on a.uhid_no = b.col3
    and a.os_amount = cast(col6 as decimal(15,2))
    and b.col1 = '",in_unit_name,"'
    and b.col2 = 'UHID - Deposit CB'
    and b.col9 = ''
    ",if(v_recon_cycle_date is null,"",concat("and b.col12 = '",cast(v_recon_cycle_date as nchar),"' ")),"
    and b.delete_flag = 'N'
    set
      b.col8 = cast(os_amount as nchar),
      b.col9 = 'NOT POSTED',
      a.balance_type = b.col2,
      a.tally_status = 'NOT POSTED'");

  call pr_run_sql(v_sql,@msg,@result);

  drop temporary table recon_tmp_tnotposted;
  drop temporary table recon_tmp_ttranwithbrkp1;
  drop temporary table recon_tmp_tuhidoutstanding;
end $$

DELIMITER ;