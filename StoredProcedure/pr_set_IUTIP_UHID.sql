DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_IUTIP_UHID $$
CREATE PROCEDURE pr_set_IUTIP_UHID(in_recon_code varchar(32))
me:begin
  declare v_tran_gid int default 0;
  declare v_uhid_no text default '';
  declare v_dr_amount decimal(15,2) default 0;
  declare v_cr_amount decimal(15,2) default 0;
  declare v_recon_code text default '';
  declare v_iut_loc_code text default '';

  declare v_dr_recon_code text default '';
  declare v_cr_recon_code text default '';

  declare v_dr_loc_code text default '';
  declare v_cr_loc_code text default '';

  declare v_tran_dr_gid int default 0;
  declare v_tran_cr_gid int default 0;

  declare v_tran_dr_min_gid int default 0;
  declare v_tran_cr_min_gid int default 0;

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';
  declare v_transfer_table text default '';
  declare v_tranbrkp_ds_code text default '';
  declare v_ds_code text default '';

  declare v_ref_no text default '';

  declare v_dataset_db_name text default '';
  declare v_sql text default '';
  declare v_succ_flag boolean default false;

  set v_tran_table = 'recon_trn_ttran';
  set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  set v_ds_code = 'DS274';
  set v_tranbrkp_ds_code = 'DS277';

  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  /*
  if v_dataset_db_name = '' then
    set v_transfer_table = 'recon_trn_tiutentry';
  else
    set v_transfer_table = concat(v_dataset_db_name,'.recon_trn_tiutentry');
  end if;
  */

  drop temporary table if exists recon_tmp_tuhid;
  drop temporary table if exists recon_tmp_tuhidrecon;
  drop temporary table if exists recon_tmp_treconuhid;
  drop temporary table if exists recon_tmp_tuhiddr;
  drop temporary table if exists recon_tmp_tuhiddr1;
  drop temporary table if exists recon_tmp_tuhidcr;
  drop temporary table if exists recon_tmp_tuhidcr1;
  drop temporary table if exists recon_tmp_tgid1;

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
    recon_code varchar(32),
    loc_code varchar(32),
    tran_date date,
    cr_amount double(15,2) not null default 0,
    iut_tran_gid int not null default 0,
    iut_recon_code varchar(32) default null,
    PRIMARY KEY (tran_gid),
    key idx_iut_tran_gid(iut_tran_gid)
  ) ENGINE = MyISAM;

  -- unid cr table
  CREATE temporary TABLE recon_tmp_tuhidcr1(
    uhid_no varchar(255),
    recon_code varchar(32),
    loc_code varchar(32),
    tran_date date,
    cr_amount double(15,2) not null default 0,
    min_tran_gid int not null default 0,
    PRIMARY KEY (uhid_no,recon_code)
  ) ENGINE = MyISAM;

  -- unid dr table
  CREATE temporary TABLE recon_tmp_tuhiddr1(
    uhid_no varchar(255),
    recon_code varchar(32),
    loc_code varchar(32),
    dr_amount double(15,2) not null default 0,
    min_tran_gid int not null default 0,
    PRIMARY KEY (uhid_no,recon_code)
  ) ENGINE = MyISAM;

  -- unid table
  CREATE temporary TABLE recon_tmp_tgid1(
    gid integer,
    PRIMARY KEY (gid)
  ) ENGINE = MyISAM;

  -- iUT - IP Entry Generation
  -- col9 - Exception Value
  -- col12 - Dr/Cr Mult
  -- col13 - Theme
  -- col19 - Bill No
  -- col20 - uhid
  -- col21 - IP/OP No
  -- col22 - Event
  -- col23 - Pay Mode
  -- col38 - Source Recon Code
  -- col41 - IUT Entry Flag
  -- col45 - IUT Recon Code
  -- col47 - IUT IP/OP
  -- col48 - From Unit
  -- col49 - To Unit
  -- col52 - IP Flag

  -- update IP Flag
  set v_sql = concat("update ",v_tran_table,"
      set col52 = 'Y'
    where recon_code = '",in_recon_code,"'
    and col13 <> 'Current Period Bills not interfaced'
    and col20 <> ''
    and col20 <> 'AC01.0005284627'
    and col38 <> ''
    and (col21 like '%IP%'
    or col19 like '%-ICR-%'
    or col19 like '%-ICS-%')
    and col41 is null
    and col52 is null
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- update IP Flag New Method
  set v_sql = concat("update ",v_tran_table,"
      set col52 = 'Y'
    where recon_code = '",in_recon_code,"'
    and col13 <> 'Current Period Bills not interfaced'
    and col20 <> ''
    and col20 <> 'AC01.0005284627'
    and col38 <> ''
    and col21 = col20
    and (col19 like '%-ICR-%'
    or col19 like '%-ICS-%')
    and col22 = 'REFUND'
    and col23 = 'Deposit Adjustment/Transfer'
    and col41 is null
    and col52 is null
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- update IP Flag Opening Balance prior date entries
  set v_sql = concat("update ",v_tran_table,"
      set col52 = 'Y'
    where recon_code = '",in_recon_code,"'
    and col13 <> 'Current Period Bills not interfaced'
    and col20 <> ''
    and col20 <> 'AC01.0005284627'
    and col38 <> ''
    and col19 = ''
    and col21 = ''
    and col41 is null
    and col47 is null
    and col52 is null
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- update IP Flag Opening Balance prior date entries
  set v_sql = concat("update ",v_tran_table,"
      set col52 = 'Y'
    where recon_code = '",in_recon_code,"'
    and col13 <> 'Current Period Bills not interfaced'
    and col20 <> ''
    and col20 <> 'AC01.0005284627'
    and col38 <> ''
    and col19 = ''
    and col20 = col21
    and col41 is null
    and col47 is null
    and col52 is null
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);


  -- col2  - support tran id
  -- col4  - Tran Date
  -- col19 - Bill No
  -- col20 - uhid
  -- col21 - IP/OP No
  -- col42 - IUT Location
  -- col38 - Recon Code
  -- col44 - UHID Multi Location Flag
  -- col47 - IUT IP/OP

  -- update UHID involved in multiple location
  set v_sql = concat("insert into recon_tmp_tuhid (uhid_no)
    select col20 from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and col13 <> 'Current Period Bills not interfaced'
    and col52 = 'Y'
    and delete_flag = 'N'
    group by col20
    having count(distinct col38) > 1
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- clear IP Flag = null for single unit UHID
  set v_sql = concat("update ",v_tran_table," as a
    set a.col52 = null
    where a.recon_code = '",in_recon_code,"'
    and a.col13 <> 'Current Period Bills not interfaced'
    and a.col52 = 'Y'
    and a.col20 not in (select uhid_no from recon_tmp_tuhid)
    and a.delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- update IP Flag = 'Y' for ip no = uhid no
  set v_sql = concat("update ",v_tran_table," as a
    inner join recon_tmp_tuhid as b on a.col20 = b.uhid_no
    set a.col52 = 'Y'
    where a.recon_code = '",in_recon_code,"'
    and a.col13 <> 'Current Period Bills not interfaced'
    and a.col51 is null
    and a.col52 is null
    and a.col20 = a.col21
    and a.col19 = ''
    and a.delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  set v_sql = concat("insert into recon_tmp_tuhidrecon (uhid_no,recon_code)
    select a.col20,a.col38 from ",v_tran_table," as a
    inner join recon_tmp_tuhid as b on  a.col20 = b.uhid_no
    where a.recon_code = '",in_recon_code,"'
    and a.col13 <> 'Current Period Bills not interfaced'
    and a.col20 <> ''
    and a.col20 <> 'AC01.0005284627'
    and a.col38 <> ''
    and a.col52 = 'Y'
    and a.delete_flag = 'N'
    group by a.col20,a.col38
    having sum(cast(a.col9 as decimal(15,2))*cast(a.col12 as signed)) <= 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  truncate recon_tmp_tuhid;
  insert into recon_tmp_tuhid (uhid_no) select distinct uhid_no from recon_tmp_tuhidrecon;

  -- update multi unit flag in tran table
  set v_sql = concat("update ",v_tran_table," as a
    inner join recon_tmp_tuhid as b on a.col20 = b.uhid_no
      set a.col44 = 'Y'
    where a.recon_code = '",in_recon_code,"'
    and a.col13 <> 'Current Period Bills not interfaced'
    and a.col20 <> ''
    and a.col20 <> 'AC01.0005284627'
    and a.col38 <> ''
    and a.col52 = 'Y'
    and a.delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- case3

  -- col4 - Tran Date
  -- col9 - Exception Value
  -- col12 - Mult
  -- col19 - Bill No
  -- col20 - uhid
  -- col38 - Recon Code
  -- col41 - IUT Entry Flag
  -- col42 - IUT Location
  -- col43 - Location Code
  -- col44 - UHID Multi Location Flag
  -- col45 - IUT recon_code
  -- col46 - IUT amount
  -- col47 - IUT  IP/OP

  -- col23 - PayMode
  -- col22 - Event

  -- find agg negative values uhid wise
  set v_sql = concat("insert into recon_tmp_tuhiddr1 (uhid_no,recon_code,loc_code,dr_amount)
    select
      col20,
      col38,
      col43,
      sum(cast(col9 as decimal(15,2))*cast(col12 as signed))
    from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and col13 <> 'Current Period Bills not interfaced'
    and col20 <> ''
    and col20 <> 'AC01.0005284627'
    and col38 <> ''
    and col44 = 'Y'
    and col47 is null
    and col52 = 'Y'
    and delete_flag = 'N'
    group by col20,col38,col43
    having sum(cast(col9 as decimal(15,2))*cast(col12 as signed)) < 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- calculate uhid cr unit wise
  set v_sql = concat("insert into recon_tmp_tuhidcr1 (uhid_no,recon_code,loc_code,cr_amount,min_tran_gid)
    select col20,col38,col43,sum(cast(col9 as decimal(15,2))*cast(col12 as signed)),min(tran_gid) from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and col13 <> 'Current Period Bills not interfaced'
    and col20 <> ''
    and col20 <> 'AC01.0005284627'
    and col38 <> ''
    and col44 = 'Y'
    and col47 is null
    and col52 = 'Y'
    and delete_flag = 'N'
    group by col20,col38,col43
    having sum(cast(col9 as decimal(15,2))*cast(col12 as signed)) > 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

	-- dr block
	dr3_block:begin
		declare dr3_done int default 0;
		declare dr3_cursor cursor for
		  select uhid_no,dr_amount,recon_code,loc_code from recon_tmp_tuhiddr1;
		declare continue handler for not found set dr3_done=1;

		open dr3_cursor;

		dr3_loop: loop
			fetch dr3_cursor into v_uhid_no,v_dr_amount,v_dr_recon_code,v_dr_loc_code;

			if dr3_done = 1 then leave dr3_loop; end if;

      set v_dr_amount = abs(v_dr_amount);
      set v_succ_flag = false;

      if exists(select * from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount = v_dr_amount) then

        select
          recon_code,loc_code,min_tran_gid into v_cr_recon_code,v_cr_loc_code,v_tran_cr_gid
        from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount = v_dr_amount
        limit 0,1;

        set v_ref_no = fn_get_autocode('IUT');

				set v_sql = concat("update ",v_tran_table," set
          col41 = 'Y',
          col45 = '", v_dr_recon_code ,"',
          col46 = col37,
          col47 = 'IUT - IP',
          col50 = '",v_dr_loc_code,"',
          col51 = '",v_ref_no,"'
				where recon_code = '",in_recon_code,"'
        and col13 <> 'Current Period Bills not interfaced'
				and col20 = '",cast(v_uhid_no as nchar),"'
				and col38 = '",v_cr_recon_code,"'
				and (col22 <> 'CREDIT NOTE REFUND' or col22 is null)
				and col47 is null
				and col44 = 'Y'
				and col52 = 'Y'
				and delete_flag = 'N'
				");

				call pr_run_sql2(v_sql,@msg,@result);

				-- dr side
				set v_sql = concat("update ",v_tran_table," set
					col45 = '",v_cr_recon_code,"',
					col46 = col37,
					col47 = 'IUT - IP',
					col50 = '",v_cr_loc_code,"',
					col51 = '",v_ref_no,"'
				where recon_code = '",in_recon_code,"'
        and col13 <> 'Current Period Bills not interfaced'
				and col20 = '",cast(v_uhid_no as nchar),"'
				and col38 = '",v_dr_recon_code,"'
				and (col22 <> 'CREDIT NOTE REFUND' or col22 is null)
				and col47 is null
				and col44 = 'Y'
				and col52 = 'Y'
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
						col50,
						col51
					)
					select
						1,
						'",in_recon_code,"',
						dataset_code,
						'",v_tranbrkp_ds_code,"',
						cast(sysdate() as nchar),
						col8,
						col9,
						col11,
						col12,
						'Entry',
						col17,
						col18,
						col19,
						col20,
						col21,
						'Entry',
						'Entry',
						col37,
						'",v_dr_recon_code,"',
						'",v_dr_loc_code,"',
						'",v_cr_recon_code,"',
						col37,
						col47,
						'",v_cr_loc_code,"',
						'",v_ref_no,"'
					from ",v_tran_table,"
					where recon_code = '",in_recon_code,"'
					and tran_gid = ",cast(v_tran_cr_gid as nchar),"
					and delete_flag = 'N'
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
						col50,
						col51
					)
					select
						1,
						'",in_recon_code,"',
						dataset_code,
						'",v_tranbrkp_ds_code,"',
						cast(sysdate() as nchar),
						col8,
						col9,
						'D',
						'-1',
						'Entry',
						col18,
						col17,
						col19,
						col20,
						col21,
						'Entry',
						'Entry',
						concat('-',col37),
						'",v_cr_recon_code,"',
						'",v_cr_loc_code,"',
						'",v_dr_recon_code,"',
						concat('-',col37),
						col47,
						'",v_dr_loc_code,"',
						'",v_ref_no,"'
					from ",v_tran_table,"
					where recon_code = '",in_recon_code,"'
					and tran_gid = ",cast(v_tran_cr_gid as nchar),"
					and delete_flag = 'N'
				");

				call pr_run_sql2(v_sql,@msg,@result);

        set v_succ_flag = true;
      end if;

      -- dr breakup
      if exists(select * from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount >= v_dr_amount) and v_succ_flag = false then

        set v_ref_no = fn_get_autocode('IUT');

        select
          recon_code,loc_code,cr_amount,min_tran_gid into v_cr_recon_code,v_cr_loc_code,v_cr_amount,v_tran_cr_min_gid
        from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount >= v_dr_amount
        order by cr_amount
        limit 0,1;

        set v_sql = concat("select col50 into @iut_loc_code from ", v_tran_table,"
          where tran_gid = ",cast(v_tran_cr_min_gid as nchar),"
          and delete_flag = 'N'
          ");

        call pr_run_sql2(v_sql,@msg,@result);

        set v_iut_loc_code = ifnull(@iut_loc_code,'');

				-- cr side
        if v_iut_loc_code = '' then
					if v_cr_amount = v_dr_amount then
						set v_sql = concat("update ",v_tran_table," set
							col45 = '",v_dr_recon_code,"',
							col46 = col37,
							col47 = 'IUT - IP',
							col50 = '",v_dr_loc_code,"',
							col51 = '",v_ref_no,"'
						where recon_code = '",in_recon_code,"'
            and col13 <> 'Current Period Bills not interfaced'
						and col20 = '",cast(v_uhid_no as nchar),"'
						and col38 = '",v_cr_recon_code,"'
            and col29 like '%COLLECTION%'
            /*
						and col2 <> '0'
						and col2 <> ''
            */
						and (col22 <> 'CREDIT NOTE REFUND' or col22 is null)
						and col47 is null
						and col44 = 'Y'
						and col52 = 'Y'
						and delete_flag = 'N'
						");
					else
						set v_sql = concat("update ",v_tran_table," set
								col45 = '",v_dr_recon_code,"',
								col51 = '",v_ref_no,"',
								col50 = '",v_dr_loc_code,"',
								col46 = '",cast(v_dr_amount as nchar),"'
							where tran_gid = ",cast(v_tran_cr_min_gid as nchar),"
							and delete_flag = 'N'
						");

						call pr_run_sql2(v_sql,@msg,@result);

						set v_sql = concat("update ",v_tran_table," set
              col45 = '",v_dr_recon_code,"',
							col47 = 'IUT - IP',
              col50 = '",v_dr_loc_code,"'
						where recon_code = '",in_recon_code,"'
            and col13 <> 'Current Period Bills not interfaced'
						and col20 = '",cast(v_uhid_no as nchar),"'
						and col38 = '",v_cr_recon_code,"'
            and col29 like '%COLLECTION%'
            /*
						and col2 <> '0'
						and col2 <> ''
            */
						and (col22 <> 'CREDIT NOTE REFUND' or col22 is null)
						and col47 is null
						and col44 = 'Y'
						and col52 = 'Y'
						and delete_flag = 'N'
						");
					end if;

					call pr_run_sql2(v_sql,@msg,@result);
        else
					set v_sql = concat("insert into ",v_tranbrkp_table,"
						(
              scheduler_gid,
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
              col53,
              col13
						)
						select
              1,
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
							'",cast(v_dr_amount as nchar),"',
							'IUT - IP',
							col48,
							'",v_dr_loc_code,"',
							'",v_ref_no,"',
							'",cast(v_dr_amount*-1 as nchar),"',
              ''
					from ",v_tran_table,"
					where tran_gid = ",cast(v_tran_cr_min_gid as nchar),"
					and delete_flag = 'N'
					");

					call pr_run_sql2(v_sql,@msg,@result);
        end if;

				-- dr side
				set v_sql = concat("update ",v_tran_table," set
          col41 = 'Y',
          col45 = '", v_cr_recon_code ,"',
          col46 = col37,
					col47 = 'IUT - IP',
          col50 = '",v_cr_loc_code,"',
          col51 = '",v_ref_no,"'
				where recon_code = '",in_recon_code,"'
        and col13 <> 'Current Period Bills not interfaced'
				and col20 = '",cast(v_uhid_no as nchar),"'
				and col38 = '",v_dr_recon_code,"'
        and (col22 <> 'CREDIT NOTE REFUND' or col22 is null)
				and col47 is null
        and col44 = 'Y'
        and col52 = 'Y'
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
              '",cast(v_dr_amount as nchar),"',
              '",cast(v_dr_amount as nchar),"',
							'D',
							'-1',
							'Entry',
              '",cast(v_dr_amount as nchar),"',
							'0.00',
              '",v_uhid_no,"',
							'Entry',
							'Entry',
              '-",cast(v_dr_amount as nchar),"',
							'",v_cr_recon_code,"',
							'",v_cr_loc_code,"',
							'",v_dr_recon_code,"',
              '-",cast(v_dr_amount as nchar),"',
							'IUT - IP',
							'",v_dr_loc_code,"',
              '",v_ref_no,"'
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
              '",cast(v_dr_amount as nchar),"',
              '",cast(v_dr_amount as nchar),"',
							'C',
							'1',
							'Entry',
							'0.00',
              '",cast(v_dr_amount as nchar),"',
              '",v_uhid_no,"',
							'Entry',
							'Entry',
              '",cast(v_dr_amount as nchar),"',
							'",v_dr_recon_code,"',
							'",v_dr_loc_code,"',
							'",v_cr_recon_code,"',
              '",cast(v_dr_amount as nchar),"',
							'IUT - IP',
							'",v_cr_loc_code,"',
              '",v_ref_no,"'
					");

					call pr_run_sql2(v_sql,@msg,@result);
      end if;
		end loop dr3_loop;

		close dr3_cursor;
	end dr3_block;

  -- update IUT - IP based on IP Flag = 'Y'
  set v_sql = concat("update ",v_tran_table," as a
    set a.col47 = 'IUT - IP'
    where a.recon_code = '",in_recon_code,"'
    and a.col13 <> 'Current Period Bills not interfaced'
    and a.col20 <> a.col21
    and a.col47 is null
    and a.col52 = 'Y'
    and a.delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_tgid1;
  drop temporary table if exists recon_tmp_tuhiddr;
  drop temporary table if exists recon_tmp_tuhiddr1;
  drop temporary table if exists recon_tmp_tuhidcr;
  drop temporary table if exists recon_tmp_tuhidcr1;
  drop temporary table if exists recon_tmp_tuhidrecon;
  drop temporary table if exists recon_tmp_tuhid;
  drop temporary table if exists recon_tmp_treconuhid;
end $$

DELIMITER ;