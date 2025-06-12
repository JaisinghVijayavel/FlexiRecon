DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_IUT9_IP $$
CREATE PROCEDURE pr_set_IUT9_IP(in_recon_code varchar(32))
me:begin
  /*
    Created By : Vijayavel
    Created Date : 04-06-2025

    Updated By : Vijayavel
    updated Date :

    Version : 1
  */

  declare v_tran_gid int default 0;
  declare v_uhid_no text default '';
  declare v_ip_no text default '';
  declare v_dr_amount decimal(15,2) default 0;
  declare v_cr_amount decimal(15,2) default 0;
  declare v_recon_code text default '';
  declare v_loc_code text default '';
  declare v_iut_loc_code text default '';
  declare v_entry_ref_no text default '';

  declare v_dr_uhid_no text default '';
  declare v_dr_grp_recon_code text default '';
  declare v_dr_grp_loc_code text default '';

  declare v_dr_recon_code text default '';
  declare v_cr_recon_code text default '';

  declare v_dr_loc_code text default '';
  declare v_cr_loc_code text default '';

  declare v_tran_dr_gid int default 0;
  declare v_tran_cr_gid int default 0;

  declare v_tran_dr_min_gid int default 0;
  declare v_tran_cr_min_gid int default 0;

  declare v_recon_view text default '';
  declare v_recon_view1 text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';
  declare v_transfer_table text default '';
  declare v_tranbrkp_ds_code text default '';
  declare v_ds_code text default '';
  declare v_tran_date date;

  declare v_ref_no text default '';

  declare v_from_unit text default '';
  declare v_to_unit text default '';

  declare v_dataset_db_name text default '';
  declare v_sql text default '';
  declare v_succ_flag boolean default false;

  declare v_concurrent_ko_flag text default '';

  -- get recon view
  set v_recon_view = 'recon_tmp_treconview';
  set v_recon_view1 = concat(in_recon_code,'_view');

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
    set v_tran_table = concat(in_recon_code,'_tran');
    set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  else
    set v_tran_table = 'recon_trn_ttran';
    set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  drop temporary table if exists recon_tmp_tuhid;
  drop temporary table if exists recon_tmp_tuhidrecon;
  drop temporary table if exists recon_tmp_treconuhid;
  drop temporary table if exists recon_tmp_tuhiddr;
  drop temporary table if exists recon_tmp_tuhiddr9;
  drop temporary table if exists recon_tmp_tuhidcr;
  drop temporary table if exists recon_tmp_tuhidcr9;
  drop temporary table if exists recon_tmp_tgid1;
  drop temporary table if exists recon_tmp_treconview;

  CREATE temporary TABLE recon_tmp_tuhid(
    uhid_no varchar(255),
    PRIMARY KEY (uhid_no)
  ) ENGINE = MyISAM;

  -- unid recon table
  CREATE temporary TABLE recon_tmp_tuhidrecon(
    uhid_no varchar(255),
    recon_code varchar(32),
    PRIMARY KEY (uhid_no,recon_code)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_treconuhid(
    uhid_no varchar(255),
    recon_code varchar(32),
    cr_amount double(15,2) not null default 0,
    excp_cr_amount double(15,2) not null default 0,
    PRIMARY KEY (uhid_no,recon_code)
  ) ENGINE = MyISAM;

  -- unid dr table
  CREATE temporary TABLE recon_tmp_tuhiddr(
    tran_gid integer not null default 0,
    uhid_no varchar(255),
    recon_code varchar(32),
    loc_code varchar(32),
    tran_date date,
    dr_amount double(15,2) not null default 0,
    iut_tran_gid int not null default 0,
    iut_recon_code varchar(32) default null,
    PRIMARY KEY (tran_gid),
    key idx_iut_tran_gid(iut_tran_gid)
  ) ENGINE = MyISAM;

  -- unid cr table
  CREATE temporary TABLE recon_tmp_tuhidcr(
    tran_gid integer not null default 0,
    uhid_no varchar(255),
    ip_no varchar(32),
    recon_code varchar(32),
    loc_code varchar(32),
    ref_no varchar(32),
    tran_date date,
    cr_amount double(15,2) not null default 0,
    iut_tran_gid int not null default 0,
    iut_recon_code varchar(32) default null,
    PRIMARY KEY (tran_gid),
    key idx_iut_tran_gid(iut_tran_gid)
  ) ENGINE = MyISAM;

  -- unid cr table
  CREATE temporary TABLE recon_tmp_tuhidcr9(
    uhid_no varchar(255),
    recon_code varchar(32),
    loc_code varchar(32),
    tran_date date,
    cr_amount double(15,2) not null default 0,
    min_tran_gid int not null default 0,
    PRIMARY KEY (uhid_no,recon_code,loc_code)
  ) ENGINE = MyISAM;

  -- unid dr table
  CREATE temporary TABLE recon_tmp_tuhiddr9(
    uhid_no varchar(255),
    recon_code varchar(32),
    loc_code varchar(32),
    dr_amount double(15,2) not null default 0,
    min_tran_gid int not null default 0,
    PRIMARY KEY (uhid_no,recon_code,loc_code)
  ) ENGINE = MyISAM;

  -- unid table
  CREATE temporary TABLE recon_tmp_tgid1(
    gid integer,
    PRIMARY KEY (gid)
  ) ENGINE = MyISAM;

  -- create recon_tmp_treconview table
  set v_sql = concat("create temporary table recon_tmp_treconview  select * from ",v_recon_view1);
  call pr_run_sql2(v_sql,@msg,@result);

  alter table recon_tmp_treconview ENGINE = MyISAM;
  alter table recon_tmp_treconview add primary key(`Tran Id`,`Supporting Tran Id`);
  create index idx_reg_no on recon_tmp_treconview(`Registration No_`(255));
  create index idx_ipop_no on recon_tmp_treconview(`Registration No_`(255),`IP/OP No_`(255));
  create index idx_theme on recon_tmp_treconview(`Theme_`(255));
  create index idx_recon_code on recon_tmp_treconview(`Registration No_`(255),`Recon Code_`(255));

  set v_ds_code = 'DS508';
  set v_tranbrkp_ds_code = 'DS277';

  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  -- iUT - IP Entry Generation
  -- col2  - support tran id
  -- col4  - Tran Date
  -- col8  - Tran Value
  -- col9  - Exception Value
  -- col11 - Dr/Cr
  -- col12 - Dr/Cr Mult
  -- col13 - Theme
  -- col16 - Particulars
  -- col17 - Debit
  -- col18 - Credit
  -- col19 - Bill No
  -- col20 - uhid
  -- col21 - IP/OP No
  -- col22 - Event
  -- col23 - PayMode
  -- col29 - Line Category
  -- col37 - Net Exception_
  -- col38 - Source Recon Code
  -- col41 - IUT Entry Flag
  -- col42 - IUT Location
  -- col43 - Location Code
  -- col44 - UHID Multi Location Flag
  -- col45 - IUT Recon Code
  -- col46 - IUT amount
  -- col47 - IUT IP/OP
  -- col50 - IUT Loc Code
  -- col51 - Entry Ref No
  -- col75 - IUT CB Type

  -- Scenario 9

  -- find agg negative values
  set v_sql = concat("insert into recon_tmp_tuhiddr9 (uhid_no,recon_code,loc_code,dr_amount)
    select
      `Registration No_`,
      `Recon Code_`,
      `Location Code`,
      sum(cast(`Exception Value_` as decimal(15,2))*cast(`Dr/Cr Mult_` as signed))
    from ",v_recon_view,"
    where true
    and (`Theme_` = ''
    or `Theme_` like '%IUT%'
    or `Theme_` = 'UHID - Deposit CB'
    or `Theme_` = 'IP Deposit'
    or `Theme_` = 'IP Refund')
    and `Registration No_` <> ''
    and `Registration No_` <> 'AC01.0005284627'
    and `Recon Code_` <> ''
    and `UHID Multi Location Flag` = 'Y'
    and (`IP/OP No_` like '%IP%'
    or `IP/OP No_` = `Registration No_`
    or `IP/OP No_` = '')
    and `IUT IP/OP` is null
    group by `Registration No_`,`Recon Code_`,`Location Code`
    having sum(cast(`Exception Value_` as decimal(15,2))*cast(`Dr/Cr Mult_` as signed)) < 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- calculate uhid cr unit wise
  set v_sql = concat("insert into recon_tmp_tuhidcr9 (uhid_no,recon_code,loc_code,cr_amount,min_tran_gid)
    select
      `Registration No_`,
      `Recon Code_`,
      `Location Code`,
      sum(cast(`Exception Value_` as decimal(15,2))*cast(`Dr/Cr Mult_` as signed)),
      min(`Tran Id`)
    from ",v_recon_view,"
    where true
    and (`Theme_` = ''
    or `Theme_` like '%IUT%'
    or `Theme_` = 'UHID - Deposit CB'
    or `Theme_` = 'IP Deposit'
    or `Theme_` = 'IP Refund')
    and `Registration No_` <> ''
    and `Registration No_` <> 'AC01.0005284627'
    and `Recon Code_` <> ''
    and `UHID Multi Location Flag` = 'Y'
    and (`IP/OP No_` like '%IP%'
    or `IP/OP No_` = `Registration No_`
    or `IP/OP No_` = '')
    and `IUT IP/OP` is null
    group by `Registration No_`,`Recon Code_`,`Location Code`
    having sum(cast(`Exception Value_` as decimal(15,2))*cast(`Dr/Cr Mult_` as signed)) > 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

	-- dr block
	cr9_block:begin
		declare cr9_done int default 0;
		declare cr9_cursor cursor for
		  select uhid_no,cr_amount,recon_code,loc_code from recon_tmp_tuhidcr9;
		declare continue handler for not found set cr9_done=1;

		open cr9_cursor;

		cr9_loop: loop
			fetch cr9_cursor into v_uhid_no,v_cr_amount,v_cr_recon_code,v_cr_loc_code;

			if cr9_done = 1 then leave cr9_loop; end if;

      set v_cr_amount = abs(v_cr_amount);
      set v_succ_flag = false;

      select
        uhid_no,group_concat(loc_code),group_concat(recon_code)
      into
        v_dr_uhid_no,v_dr_grp_loc_code,v_dr_grp_recon_code
      from recon_tmp_tuhiddr9
      where uhid_no = v_uhid_no
      and recon_code <> v_cr_recon_code
      group by uhid_no
      and sum(abs(dr_amount)) = v_cr_amount;

      set v_dr_uhid_no = ifnull(v_dr_uhid_no,'');
      set v_dr_grp_loc_code = ifnull(v_dr_grp_loc_code,'');
      set v_dr_grp_recon_code = ifnull(v_dr_grp_recon_code,'');

      if v_dr_uhid_no <> '' then
        set v_ref_no = fn_get_autocode('IUT');

        -- update in credit line
				set v_sql = concat("update ",v_tran_table," set
						col41 = 'Y',
						col45 = '", v_dr_grp_recon_code ,"',
						col46 = '",cast(v_cr_amount as nchar),"',
						col47 = 'IUT9 - IP',
						col50 = '", v_dr_grp_loc_code ,"',
						col51 = '",v_ref_no,"'
				where recon_code = '",in_recon_code,"'
				and (col13 = ''
				or col13 like '%IUT%'
				or col13 = 'UHID - Deposit CB'
				or col13 = 'IP Deposit'
				or col13 = 'IP Refund')
				and col20 = '",cast(v_uhid_no as nchar),"'
				and col38 = '",v_cr_recon_code,"'
				and col47 is null
				and col44 = 'Y'
        and (col21 like '%IP%'
        or col21 = col20
        or col21 = '')
				and delete_flag = 'N'
				");

				call pr_run_sql2(v_sql,@msg,@result);

        -- update in debit line
				set v_sql = concat("update ",v_tran_table," set
						col41 = 'Y',
						col45 = '", v_cr_recon_code ,"',
						col46 = '",cast(v_cr_amount as nchar),"',
						col47 = 'IUT9 - IP',
						col50 = '", v_cr_loc_code ,"',
						col51 = '",v_ref_no,"'
				where recon_code = '",in_recon_code,"'
				and (col13 = ''
				or col13 like '%IUT%'
				or col13 = 'UHID - Deposit CB'
				or col13 = 'IP Deposit'
				or col13 = 'IP Refund')
				and col20 = '",cast(v_uhid_no as nchar),"'
				and col38 <> '",v_cr_recon_code,"'
				and col47 is null
				and col44 = 'Y'
        and (col21 like '%IP%'
        or col21 = col20
        or col21 = '')
				and delete_flag = 'N'
				");

				call pr_run_sql2(v_sql,@msg,@result);

				-- dr location
				set v_sql=concat("insert into ",v_tranbrkp_table,"
					(
						scheduler_gid,
						recon_code,
						dataset_code,
						tranbrkp_dataset_code,
						col4,
						col8,
						col9,
						col11,
						col12,
						col16,
						col17,
						col18,
						col20,
						col22,
						col23,
						col29,
						col37,
						col38,
						col43,
						col45,
						col46,
						col47,
						col50,
						col51
					)
					select
						1,
						'",in_recon_code,"',
						'",v_ds_code,"',
						'",v_tranbrkp_ds_code,"',
						cast(sysdate() as nchar),
						cast(abs(dr_amount) as nchar),
						cast(abs(dr_amount) as nchar),
						'D',
						'-1',
						'Entry',
						cast(abs(dr_amount) as nchar),
						'0.00',
						'",v_uhid_no,"',
						'Entry',
						'Entry',
						'Entry',
						cast(dr_amount as nchar),
						'",v_cr_recon_code,"',
						'",v_cr_loc_code,"',
						'",v_dr_grp_recon_code,"',
						cast(dr_amount as nchar),
            'IUT9 - IP',
						'",v_dr_grp_loc_code,"',
						'",v_ref_no,"'
					from recon_tmp_tuhiddr9
          where uhid_no = '",v_uhid_no,"'
          and recon_code <> '",v_cr_recon_code,"'
				");

				call pr_run_sql2(v_sql,@msg,@result);

				-- cr location
				set v_sql=concat("insert into ",v_tranbrkp_table,"
					(
						scheduler_gid,
						recon_code,
						dataset_code,
						tranbrkp_dataset_code,
						col4,
						col8,
						col9,
						col11,
						col12,
						col16,
						col17,
						col18,
						col20,
						col22,
						col23,
						col29,
						col37,
						col38,
						col43,
						col45,
						col46,
						col47,
						col50,
						col51
					)
					select
						1,
						'",in_recon_code,"',
						'",v_ds_code,"',
						'",v_tranbrkp_ds_code,"',
						cast(sysdate() as nchar),
						cast(abs(dr_amount) as nchar),
						cast(abs(dr_amount) as nchar),
						'C',
						'1',
						'Entry',
						'0.00',
						cast(abs(dr_amount) as nchar),
						'",v_uhid_no,"',
						'Entry',
						'Entry',
						'Entry',
						cast(abs(dr_amount) as nchar),
						recon_code,
						loc_code,
						'",v_cr_recon_code,"',
						cast(abs(dr_amount) as nchar),
            'IUT9 - IP',
						'",v_cr_loc_code,"',
						'",v_ref_no,"'
					from recon_tmp_tuhiddr9
          where uhid_no = '",v_uhid_no,"'
          and recon_code <> '",v_cr_recon_code,"'
				");

				call pr_run_sql2(v_sql,@msg,@result);
      end if;
		end loop cr9_loop;

		close cr9_cursor;
	end cr9_block;

  drop temporary table if exists recon_tmp_tgid1;
  drop temporary table if exists recon_tmp_tuhiddr;
  drop temporary table if exists recon_tmp_tuhiddr9;
  drop temporary table if exists recon_tmp_tuhidcr;
  drop temporary table if exists recon_tmp_tuhidcr9;
  drop temporary table if exists recon_tmp_tuhidrecon;
  drop temporary table if exists recon_tmp_tuhid;
  drop temporary table if exists recon_tmp_treconuhid;
  drop temporary table if exists recon_tmp_treconview;
end $$

DELIMITER ;