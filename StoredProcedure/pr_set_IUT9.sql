DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_IUT9 $$
CREATE PROCEDURE pr_set_IUT9(in_recon_code varchar(32))
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

  declare v_adj_amount decimal(15,2) default 0;
  declare v_bal_amount decimal(15,2) default 0;

  declare v_recon_code text default '';
  declare v_loc_code text default '';
  declare v_iut_loc_code text default '';
  declare v_entry_ref_no text default '';

  declare v_dr_uhid_no text default '';

  declare v_dr_recon_code text default '';
  declare v_cr_recon_code text default '';

  declare v_dr_loc_code text default '';
  declare v_cr_loc_code text default '';

  declare v_dr_dataset text default '';
  declare v_cr_dataset text default '';

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
    amount double(15,2) not null default 0,
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
    dataset_name varchar(255),
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
    dataset_name varchar(255),
    dr_amount double(15,2) not null default 0,
    adj_amount double(15,2) not null default 0,
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
  -- col7  - Dataset
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
  -- col54 - IUT Theme
  -- col75 - IUT CB Type

  -- Scenario 9

  -- find agg negative values
  set v_sql = concat("insert into recon_tmp_tuhiddr9 (uhid_no,recon_code,loc_code,dataset_name,dr_amount)
    select
      `Registration No_`,
      `Recon Code_`,
      `Location Code`,
      `Dataset_`,
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
    and `IUT IP/OP` is null
    group by `Registration No_`,`Recon Code_`,`Location Code`,`Dataset_`
    having sum(cast(`Exception Value_` as decimal(15,2))*cast(`Dr/Cr Mult_` as signed)) < 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- remove uhid more than one location
  truncate recon_tmp_tuhid;

  insert into recon_tmp_tuhid(uhid_no)
    select uhid_no from recon_tmp_tuhiddr9
    group by uhid_no
    having count(*) > 1;

  delete from recon_tmp_tuhiddr9 where uhid_no in (select uhid_no from recon_tmp_tuhid);

  -- calculate uhid cr unit wise
  set v_sql = concat("insert into recon_tmp_tuhidcr9 (uhid_no,recon_code,loc_code,dataset_name,cr_amount,min_tran_gid)
    select
      `Registration No_`,
      `Recon Code_`,
      `Location Code`,
      `Dataset_`,
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
    and `IUT IP/OP` is null
    group by `Registration No_`,`Recon Code_`,`Location Code`,`Dataset_`
    having sum(cast(`Exception Value_` as decimal(15,2))*cast(`Dr/Cr Mult_` as signed)) > 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- remove single credit
  truncate recon_tmp_tuhid;

  insert into recon_tmp_tuhid(uhid_no)
    select uhid_no from recon_tmp_tuhidcr9
    group by uhid_no
    having count(*) > 1;

  delete from recon_tmp_tuhidcr9 where uhid_no not in (select uhid_no from recon_tmp_tuhid);

  -- check credit available
  truncate recon_tmp_tuhid;

  insert into recon_tmp_tuhid(uhid_no,amount)
    select a.uhid_no,a.dr_amount from recon_tmp_tuhiddr9 as a
    inner join recon_tmp_tuhidcr9 as b on a.uhid_no = b.uhid_no
    group by a.uhid_no,a.dr_amount
    having abs(a.dr_amount) <= sum(b.cr_amount);

  delete from recon_tmp_tuhiddr9 where uhid_no not in (select uhid_no from recon_tmp_tuhid);
  delete from recon_tmp_tuhidcr9 where uhid_no not in (select uhid_no from recon_tmp_tuhid);

  truncate recon_tmp_tuhid;

	-- cr block
	cr9_block:begin
		declare cr9_done int default 0;
		declare cr9_cursor cursor for
		  select uhid_no,cr_amount,recon_code,loc_code,dataset_name from recon_tmp_tuhidcr9;
		declare continue handler for not found set cr9_done=1;

		open cr9_cursor;

		cr9_loop: loop
			fetch cr9_cursor into v_uhid_no,v_cr_amount,v_cr_recon_code,v_cr_loc_code,v_cr_dataset;

			if cr9_done = 1 then leave cr9_loop; end if;

      set v_uhid_no = ifnull(v_uhid_no,'');
      set v_cr_amount = ifnull(v_cr_amount,0);
      set v_cr_loc_code = ifnull(v_cr_loc_code,'');
      set v_cr_recon_code = ifnull(v_cr_recon_code,'');
      set v_cr_dataset = ifnull(v_cr_dataset,'');

      set v_succ_flag = false;

      select
        uhid_no,loc_code,recon_code,dataset_name,min_tran_gid,
        abs(dr_amount),adj_amount,abs(dr_amount)-adj_amount
      into
        v_dr_uhid_no,v_dr_loc_code,v_dr_recon_code,v_dr_dataset,v_tran_dr_min_gid,
        v_dr_amount,v_adj_amount,v_bal_amount
      from recon_tmp_tuhiddr9
      where uhid_no = v_uhid_no
      and loc_code <> v_cr_loc_code;

      set v_dr_uhid_no = ifnull(v_dr_uhid_no,'');
      set v_dr_loc_code = ifnull(v_dr_loc_code,'');
      set v_dr_recon_code = ifnull(v_dr_recon_code,'');
      set v_dr_dataset = ifnull(v_dr_dataset,'');
      set v_dr_amount = ifnull(v_dr_amount,0);
      set v_bal_amount = ifnull(v_bal_amount,0);
      set v_tran_dr_min_gid = ifnull(v_tran_dr_min_gid,0);

      if v_dr_uhid_no <> '' and v_bal_amount >= v_cr_amount then
        set v_ref_no = fn_get_autocode('IUT');

        -- update in credit line
				set v_sql = concat("update ",v_tran_table," set
						col41 = 'Y',
						col45 = '", v_dr_recon_code ,"',
						col46 = '",cast(v_cr_amount as nchar),"',
						col47 = 'IUT9',
						col50 = '", v_dr_loc_code ,"',
						col51 = '",v_ref_no,"',
						col54 = 'IUT9'
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
				and delete_flag = 'N'
				");

				call pr_run_sql2(v_sql,@msg,@result);

        if v_bal_amount = v_dr_amount then
          -- update in debit line
				  set v_sql = concat("update ",v_tran_table," set
						col41 = 'Y',
						col45 = '", v_cr_recon_code ,"',
						col46 = '",cast(v_cr_amount as nchar),"',
						col47 = 'IUT9',
						col50 = '", v_cr_loc_code ,"',
						col51 = '",v_ref_no,"',
						col54 = 'IUT9'
				  where tran_gid = '",cast(v_tran_dr_min_gid as nchar),"'
				  and col20 = '",cast(v_uhid_no as nchar),"'
				  and col38 = '",v_dr_recon_code,"'
				  and col47 is null
				  and col44 = 'Y'
				  and delete_flag = 'N'
				  ");

				  call pr_run_sql2(v_sql,@msg,@result);

          -- update in debit line
				  set v_sql = concat("update ",v_tran_table," set
						col41 = 'Y',
						col45 = '", v_dr_recon_code ,"',
						col47 = 'IUT9',
						col50 = '", v_dr_loc_code ,"',
						col54 = 'IUT9'
				  where recon_code = '",in_recon_code,"'
				  and (col13 = ''
				  or col13 like '%IUT%'
				  or col13 = 'UHID - Deposit CB'
				  or col13 = 'IP Deposit'
				  or col13 = 'IP Refund')
				  and col20 = '",cast(v_uhid_no as nchar),"'
				  and col38 = '",v_dr_recon_code,"'
				  and col47 is null
				  and col44 = 'Y'
				  and delete_flag = 'N'
				  ");

				  call pr_run_sql2(v_sql,@msg,@result);

        else
          -- Add Adj Entry
					set v_sql = concat("insert into ",v_tranbrkp_table,"
						(
              scheduler_gid,
              tran_gid,
              recon_code,
              dataset_code,
              tranbrkp_dataset_code,
							col1,
							col2,
              col3,
							col4,
							col5,
							col6,
              col7,
              col13,col54,
							col16,
							col19,
							col20,
							col21,
							col22,
							col23,
							col37,
							col38,
							col43,
							col45,
							col46,
							col47,
							col48,
							col50,
							col51,
              col53
						)
						select
              1,
              tran_gid,
              '",in_recon_code,"',
              dataset_code,
              '",v_tranbrkp_ds_code,"',
							col1,
							col2,
							col3,
							col4,
							col5,
							col6,
              col7,
              'IUT9','IUT9',
							'Adj Entry',
							col19,
							col20,
							col21,
							'Adj Entry',
							'Adj Entry',
							'0.00',
							col38,
							col43,
							'",v_dr_recon_code,"',
							'",cast(v_cr_amount as nchar),"',
							'IUT',
							col48,
							'",v_dr_loc_code,"',
							'",v_ref_no,"',
							'",cast(v_cr_amount*-1 as nchar),"'
					  from ",v_tran_table,"
					  where tran_gid = ",cast(v_tran_dr_min_gid as nchar),"
					  and delete_flag = 'N'
					");

					call pr_run_sql2(v_sql,@msg,@result);
        end if;

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
            col13,col54,
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
						cast(cr_amount as nchar),
						cast(cr_amount as nchar),
						'D',
						'-1',
            'IUT9','IUT9',
						'Entry',
						cast(cr_amount as nchar),
						'0.00',
						'",v_uhid_no,"',
						'Entry',
						'Entry',
						'Entry',
						cast(cr_amount as nchar),
						'",v_cr_recon_code,"',
						'",v_cr_loc_code,"',
						'",v_dr_recon_code,"',
						cast(cr_amount as nchar),
            'IUT9',
						'",v_dr_loc_code,"',
						'",v_ref_no,"'
					from recon_tmp_tuhidcr9
          where uhid_no = '",v_uhid_no,"'
          and recon_code = '",v_cr_recon_code,"'
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
            col13,col54,
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
						cast(cr_amount as nchar),
						cast(cr_amount as nchar),
						'C',
						'1',
            'IUT9','IUT9',
						'Entry',
						cast(cr_amount as nchar),
						'0.00',
						'",v_uhid_no,"',
						'Entry',
						'Entry',
						'Entry',
						cast(cr_amount as nchar),
						'",v_dr_recon_code,"',
						'",v_dr_loc_code,"',
						'",v_cr_recon_code,"',
						cast(cr_amount as nchar),
            'IUT9',
						'",v_cr_loc_code,"',
						'",v_ref_no,"'
					from recon_tmp_tuhidcr9
          where uhid_no = '",v_uhid_no,"'
          and recon_code = '",v_cr_recon_code,"'
				");

				call pr_run_sql2(v_sql,@msg,@result);

        -- update adj amount
        update recon_tmp_tuhiddr9 set
          adj_amount = adj_amount + v_cr_amount
        where uhid_no = v_uhid_no;
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