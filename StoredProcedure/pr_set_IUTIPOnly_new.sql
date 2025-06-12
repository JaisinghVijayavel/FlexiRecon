DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_IUTIPOnly_new $$
CREATE PROCEDURE pr_set_IUTIPOnly_new(in_recon_code varchar(32))
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

  declare v_dr_recon_code text default '';
  declare v_cr_recon_code text default '';

  declare v_dr_dataset text default '';
  declare v_cr_dataset text default '';

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
  drop temporary table if exists recon_tmp_tuhiddr1;
  drop temporary table if exists recon_tmp_tuhidcr;
  drop temporary table if exists recon_tmp_tuhidcr1;
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
  CREATE temporary TABLE recon_tmp_tuhidcr1(
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
  CREATE temporary TABLE recon_tmp_tuhiddr1(
    uhid_no varchar(255),
    recon_code varchar(32),
    loc_code varchar(32),
    dataset_name varchar(255),
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
  -- col9 - Exception Value
  -- col12 - Dr/Cr Mult
  -- col13 - Theme
  -- col19 - Bill No
  -- col20 - uhid
  -- col21 - IP/OP No
  -- col22 - Event
  -- col23 - PayMode
  -- col29 - Line Category
  -- col38 - Source Recon Code
  -- col41 - IUT Entry Flag
  -- col42 - IUT Location
  -- col44 - UHID Multi Location Flag
  -- col45 - IUT Recon Code
  -- col47 - IUT IP/OP
  -- col50 - IUT Loc Code
  -- col75 - IUT CB Type

  -- Case1 - Deposit Adjustment Transfer-Refund

  -- calculate uhid dr unit wise
  set v_sql = concat("insert into recon_tmp_tuhiddr
    (
      uhid_no,
      recon_code,
      loc_code,
      tran_gid,
      tran_date,
      dr_amount
    )
    select
      `Registration No_`,
      `Recon Code_`,
      `Location Code`,
      `Tran Id`,
      cast(`Tran Date_` as date),
      cast(`Exception Value_` as decimal(15,2))
    from ",v_recon_view,"
    where true
    and `Dr/Cr Mult_` = '-1'
    and (`Theme_` = ''
    or `Theme_` like '%IUT%'
    or `Theme_` = 'UHID - Deposit CB'
    or `Theme_` = 'IP Deposit'
    or `Theme_` = 'IP Refund')
    and `Registration No_` <> ''
    and `Registration No_` <> 'AC01.0005284627'
    and `Event_` = 'REFUND'
    and `Pay Mode_` = 'Deposit Adjustment/Transfer'
    and `Recon Code_` <> ''
    and `UHID Multi Location Flag` = 'Y'
    and `Line Category_` like '%COLLECTION%'
    and `IP/OP No_` like '%IP%'
    and `IUT IP/OP` is null
    ");

  call pr_run_sql2(v_sql,@msg,@result);

	-- dr block
	dr1_block:begin
		declare dr1_done int default 0;
		declare dr1_cursor cursor for
		  select tran_gid,uhid_no,dr_amount,recon_code,loc_code from recon_tmp_tuhiddr;
		declare continue handler for not found set dr1_done=1;

		open dr1_cursor;

		dr1_loop: loop
			fetch dr1_cursor into v_tran_dr_gid,v_uhid_no,v_dr_amount,v_dr_recon_code,v_dr_loc_code;

			if dr1_done = 1 then leave dr1_loop; end if;

			-- check uhid cr
			set v_sql = concat("select count(*) into @rec_count
				from ",v_recon_view,"
				where true
				and `Dr/Cr Mult_` = '1'
        and (`Theme_` = ''
        or `Theme_` like '%IUT%'
        or `Theme_` = 'UHID - Deposit CB'
        or `Theme_` = 'IP Deposit'
        or `Theme_` = 'IP Refund')
				and `Registration No_` = '",v_uhid_no,"'
				and `Recon Code_` <> '",v_dr_recon_code,"'
        and `Recon Code_` <> ''
        and cast(`Exception Value_` as decimal(15,2)) = ",cast(v_dr_amount as nchar),"
				and `UHID Multi Location Flag` = 'Y'
        and `Line Category_` like '%COLLECTION%'
        and `Event_` <> 'CREDIT NOTE REFUND'
        and `IP/OP No_` like '%IP%'
        and `IUT IP/OP` is null
				");

			call pr_run_sql2(v_sql,@msg,@result);

      set @rec_count = ifnull(@rec_count,0);

      if @rec_count > 0 then
        set v_tran_cr_gid = 0;
        set v_cr_recon_code = '';

        set @v_tran_cr_gid = 0;
        set @v_cr_recon_code = '';

				-- check uhid cr
				set v_sql = concat("select
            `Tran Id`,`Recon Code_`,`Location Code`
          into
            @v_tran_cr_gid,@v_cr_recon_code,@v_cr_loc_code
					from ",v_recon_view,"
					where `Dr/Cr Mult_` = '1'
          and (`Theme_` = ''
          or `Theme_` like '%IUT%'
          or `Theme_` = 'UHID - Deposit CB'
          or `Theme_` = 'IP Deposit'
          or `Theme_` = 'IP Refund')
					and `Registration No_` = '",v_uhid_no,"'
					and `Recon Code_` <> '",v_dr_recon_code,"'
					and `Recon Code_` <> ''
					and cast(`Exception Value_` as decimal(15,2)) = ",cast(v_dr_amount as nchar),"
					and `UHID Multi Location Flag` = 'Y'
          and `Line Category_` like '%COLLECTION%'
          and `Event_` <> 'CREDIT NOTE REFUND'
          and `IP/OP No_` like '%IP%'
          and `IUT IP/OP` is null limit 0,1
					");

				call pr_run_sql2(v_sql,@msg,@result);

        set v_tran_cr_gid = ifnull(@v_tran_cr_gid,0);
        set v_cr_recon_code = ifnull(@v_cr_recon_code,'');
        set v_cr_loc_code = ifnull(@v_cr_loc_code,'');

        if v_tran_cr_gid > 0 then
          set v_ref_no = fn_get_autocode('IUT');

          -- dr side
					set v_sql = concat("update ",v_tran_table," set
						col41 = 'Y',
						col45 = '", v_cr_recon_code ,"',
						col46 = '-",cast(v_dr_amount as nchar),"',
						col47 = 'IUT - IP Only',
            col50 = '",v_cr_loc_code,"',
            col51 = '",v_ref_no,"'
					where tran_gid = ",cast(v_tran_dr_gid as nchar),"
					");

					call pr_run_sql2(v_sql,@msg,@result);

					-- cr side
					set v_sql = concat("update ",v_tran_table," set
						col41 = 'Y',
						col45 = '", v_dr_recon_code ,"',
						col46 = '",cast(v_dr_amount as nchar),"',
						col47 = 'IUT - IP Only',
            col50 = '",v_dr_loc_code,"',
            col51 = '",v_ref_no,"'
					where tran_gid = ",cast(v_tran_cr_gid as nchar),"
					");

					call pr_run_sql2(v_sql,@msg,@result);

          -- col4-Tran Date_
          -- col8-Tran Value_
          -- col9-Exception Value_
          -- col11-Dr/Cr_
					-- col12-Dr/Cr Mult_
					-- col16-Particulars_
					-- col17-Debit_
					-- col18-Credit_
					-- col19-Bill No_
					-- col20-Registration No_
					-- col21-IP/OP No_
					-- col22-Event_
					-- col23-Pay Mode_
					-- col30-Receipt Number_
					-- col37-Net Exception_
          -- col38-Recon Code
					-- col43-Location Code
					-- col47-IUT IP/OP

          -- tranbrkp_dataset_code - recon_trn_tiutentry
          -- dataset_code - DS274

          -- dr location
          set v_sql=concat("insert into ",v_tranbrkp_table,"
            (
              scheduler_gid,
              recon_code,
              dataset_code,
              tranbrkp_dataset_code,
              col4,
              col7,
              col8,
              col9,
              col11,
              col12,
              col13,col54,
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
              col7,
              col8,
              col9,
              col11,
              col12,
              'IUT','IUT',
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
              col7,
              col8,
              col9,
              col11,
              col12,
              col13,col54,
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
              col7,
              col8,
              col9,
              col11,
              col12,
              'IUT','IUT',
              'Entry',
              col17,
              col18,
              col19,
              col20,
              col21,
              'Entry',
              'Entry',
              col37,
              '",v_cr_recon_code,"',
              '",v_cr_loc_code,"',
              '",v_dr_recon_code,"',
              col37,
              col47,
              '",v_dr_loc_code,"',
              '",v_ref_no,"'
            from ",v_tran_table,"
            where recon_code = '",in_recon_code,"'
            and tran_gid = ",cast(v_tran_dr_gid as nchar),"
            and delete_flag = 'N'
          ");

					call pr_run_sql2(v_sql,@msg,@result);
        end if;
      end if;
		end loop dr1_loop;

		close dr1_cursor;
	end dr1_block;

  truncate recon_tmp_tuhid;
  truncate recon_tmp_tuhiddr;
  truncate recon_tmp_tuhidcr;
  truncate recon_tmp_tuhidcr1;
  truncate recon_tmp_tuhiddr1;

  -- create recon_tmp_treconview table
  truncate recon_tmp_treconview;
  set v_sql = concat("insert into recon_tmp_treconview  select * from ",v_recon_view1);
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
  -- col44 - UHID Multi Location Flag
  -- col43 - Location Code
  -- col45 - IUT recon_code
  -- col46 - IUT amount
  -- col47 - IUT IP/OP

  -- col22 - Event
  -- col23 - PayMode

  -- find agg negative values
  set v_sql = concat("insert into recon_tmp_tuhiddr1 (uhid_no,recon_code,loc_code,dataset_name,dr_amount)
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
    and `IP/OP No_` like '%IP%'
    and `IUT IP/OP` is null
    group by `Registration No_`,`Recon Code_`,`Location Code`,`Dataset_`
    having sum(cast(`Exception Value_` as decimal(15,2))*cast(`Dr/Cr Mult_` as signed)) < 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- calculate uhid cr unit wise
  set v_sql = concat("insert into recon_tmp_tuhidcr1 (uhid_no,recon_code,loc_code,dataset_name,cr_amount,min_tran_gid)
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
    and `IP/OP No_` like '%IP%'
    and `IUT IP/OP` is null
    group by `Registration No_`,`Recon Code_`,`Location Code`,`Dataset_`
    having sum(cast(`Exception Value_` as decimal(15,2))*cast(`Dr/Cr Mult_` as signed)) > 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

	-- dr block
	dr3_block:begin
		declare dr3_done int default 0;
		declare dr3_cursor cursor for
		  select uhid_no,dr_amount,recon_code,loc_code,dataset_name from recon_tmp_tuhiddr1;
		declare continue handler for not found set dr3_done=1;

		open dr3_cursor;

		dr3_loop: loop
			fetch dr3_cursor into v_uhid_no,v_dr_amount,v_dr_recon_code,v_dr_loc_code,v_dr_dataset;

			if dr3_done = 1 then leave dr3_loop; end if;

      set v_dr_amount = abs(v_dr_amount);
      set v_succ_flag = false;

      if exists(select * from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount = v_dr_amount) then

        select
          recon_code,loc_code,dataset_name into v_cr_recon_code,v_cr_loc_code,v_cr_dataset
        from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount = v_dr_amount
        limit 0,1;

        set @rec_count = 0;

				-- cr side
				set v_sql = concat("select count(*) into @rec_count from ",v_recon_view,"
				where true
        and (`Theme_` = ''
        or `Theme_` like '%IUT%'
        or `Theme_` = 'UHID - Deposit CB'
        or `Theme_` = 'IP Deposit'
        or `Theme_` = 'IP Refund')
				and `Registration No_` = '",cast(v_uhid_no as nchar),"'
				and `Recon Code_` = '",v_cr_recon_code,"'
        and cast(`Exception Value_` as decimal(15,2)) = ",cast(v_dr_amount as nchar),"
        and `Dr/Cr Mult_` = '1'
        /*and `Line Category_` like '%COLLECTION%'
        and (`Event_` <> 'CREDIT NOTE REFUND' or `Event_` is null)*/
        and `UHID Multi Location Flag` = 'Y'
        and `IP/OP No_` like '%IP%'
				and `IUT IP/OP` is null
				");

        call pr_run_sql2(v_sql,@msg,@result);

        set @rec_count = ifnull(@rec_count,0);

        if @rec_count > 0 then
          truncate recon_tmp_tuhidcr;

				  set v_sql = concat(" insert into recon_tmp_tuhidcr (tran_gid,cr_amount)
          select `Tran Id`,cast(`Exception Value_` as decimal(15,2)) from ",v_recon_view,"
				  where true
          and (`Theme_` = ''
          or `Theme_` like '%IUT%'
          or `Theme_` = 'UHID - Deposit CB'
          or `Theme_` = 'IP Deposit'
          or `Theme_` = 'IP Refund')
				  and `Registration No_` = '",cast(v_uhid_no as nchar),"'
				  and `Recon Code_` = '",v_cr_recon_code,"'
          and cast(`Exception Value_` as decimal(15,2)) = ",cast(v_dr_amount as nchar),"
          and `Dr/Cr Mult_` = '1'
          /*and `Line Category_` like '%COLLECTION%'
          and (`Event_` <> 'CREDIT NOTE REFUND' or `Event_` is null)*/
          and `UHID Multi Location Flag` = 'Y'
          and `IP/OP No_` like '%IP%'
				  and `IUT IP/OP` is null
          order by cast(`Tran Date_` as date) asc
          limit 0,1
				  ");

          -- and col12 = '1'

          call pr_run_sql2(v_sql,@msg,@result);

          if exists(select * from recon_tmp_tuhidcr) then
            set v_ref_no = fn_get_autocode('IUT');

            select tran_gid,cr_amount into v_tran_cr_gid,v_cr_amount from recon_tmp_tuhidcr;

            set v_sql = concat("update ",v_tran_table," set
						    col41 = 'Y',
						    col45 = '", v_dr_recon_code ,"',
						    col46 = '",cast(v_cr_amount as nchar),"',
						    col47 = 'IUT - IP Only',
						    col50 = '", v_dr_loc_code ,"',
                col51 = '",v_ref_no,"'
              where tran_gid = ",cast(v_tran_cr_gid as nchar),"
              and delete_flag = 'N'
            ");

            call pr_run_sql2(v_sql,@msg,@result);

						-- dr side
						set v_sql = concat("update ",v_tran_table," set
              col41 = 'Y',
              col45 = '",v_cr_recon_code,"',
              col46 = col37,
							col47 = 'IUT - IP Only',
              col50 = '", v_cr_loc_code ,"',
              col51 = '",v_ref_no,"'
						where recon_code = '",in_recon_code,"'
            and (col13 = ''
            or col13 like '%IUT%'
            or col13 = 'UHID - Deposit CB'
            or col13 = 'IP Deposit'
            or col13 = 'IP Refund')
						and col20 = '",cast(v_uhid_no as nchar),"'
						and col38 = '",v_dr_recon_code,"'
            /*
            and col29 like '%COLLECTION%'
						and col2 <> '0'
						and col2 <> ''
            and (col22 <> 'CREDIT NOTE REFUND' or col22 is null)
            */
						and col47 is null
            and col44 = 'Y'
            and col21 like '%IP%'
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
                col7,
								col8,
								col9,
								col11,
								col12,
                col13,col54,
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
                '",v_dr_dataset,"',
								col8,
								col9,
								col11,
								col12,
                'IUT','IUT',
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
                col7,
								col8,
								col9,
								col11,
								col12,
                col13,col54,
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
                col7,
								col8,
								col9,
								'D',
								'-1',
                'IUT','IUT',
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
        end if;
      end if;

      -- dr breakup
      if exists(select * from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount >= v_dr_amount) and v_succ_flag = false then

        set v_ref_no = fn_get_autocode('IUT');

        select
          recon_code,loc_code,dataset_name,cr_amount,min_tran_gid
        into
          v_cr_recon_code,v_cr_loc_code,v_cr_dataset,v_cr_amount,v_tran_cr_min_gid
        from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount >= v_dr_amount
        order by cr_amount
        limit 0,1;

        set v_sql = concat("select col51 into @entry_ref_no from ", v_tran_table,"
          where tran_gid = ",cast(v_tran_cr_min_gid as nchar),"
          and delete_flag = 'N'
          ");

        call pr_run_sql2(v_sql,@msg,@result);

        set v_entry_ref_no = ifnull(@entry_ref_no,'');

				-- cr side
        if v_entry_ref_no = '' then
					if v_cr_amount = v_dr_amount then
						set v_sql = concat("update ",v_tran_table," set
              col41 = 'Y',
							col45 = '",v_dr_recon_code,"',
							col46 = col37,
							col47 = 'IUT - IP Only',
							col50 = '",v_dr_loc_code,"',
							col51 = '",v_ref_no,"'
						where recon_code = '",in_recon_code,"'
            and (col13 = ''
            or col13 like '%IUT%'
            or col13 = 'UHID - Deposit CB'
            or col13 = 'IP Deposit'
            or col13 = 'IP Refund')
						and col20 = '",cast(v_uhid_no as nchar),"'
						and col38 = '",v_cr_recon_code,"'
						/*and ((col2 <> '0'
						and col2 <> ''
            and col29 like '%COLLECTION%'
            col22 <> 'CREDIT NOTE REFUND') or col22 is null)*/
						and col47 is null
						and col44 = 'Y'
            and col21 like '%IP%'
						and delete_flag = 'N'
						");

					  call pr_run_sql2(v_sql,@msg,@result);
					else
						set v_sql = concat("update ",v_tran_table," set
                col41 = 'Y',
								col45 = '",v_dr_recon_code,"',
								col50 = '",v_dr_loc_code,"',
								col51 = '",v_ref_no,"',
								col46 = '",cast(v_dr_amount as nchar),"'
							where tran_gid = ",cast(v_tran_cr_min_gid as nchar),"
							and delete_flag = 'N'
						");

						call pr_run_sql2(v_sql,@msg,@result);

						set v_sql = concat("update ",v_tran_table," set
              col45 = '",v_dr_recon_code,"',
							col47 = 'IUT - IP Only',
              col50 = '",v_dr_loc_code,"'
						where recon_code = '",in_recon_code,"'
            and (col13 = ''
            or col13 like '%IUT%'
            or col13 = 'UHID - Deposit CB'
            or col13 = 'IP Deposit'
            or col13 = 'IP Refund')
						and col20 = '",cast(v_uhid_no as nchar),"'
						and col38 = '",v_cr_recon_code,"'
						/*and ((col2 <> '0'
						and col2 <> ''
            and col29 like '%COLLECTION%'
						col22 <> 'CREDIT NOTE REFUND') or col22 is null)*/
						and col47 is null
						and col44 = 'Y'
            and col21 like '%IP%'
						and delete_flag = 'N'
						");

					  call pr_run_sql2(v_sql,@msg,@result);
					end if;
        else
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
              'IUT','IUT',
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
							'IUT - IP Only',
							col48,
							'",v_dr_loc_code,"',
							'",v_ref_no,"',
							'",cast(v_dr_amount*-1 as nchar),"'
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
					col47 = 'IUT - IP Only',
          col50 = '", v_cr_loc_code ,"',
          col51 = '",v_ref_no,"'
				where recon_code = '",in_recon_code,"'
        and (col13 = ''
        or col13 like '%IUT%'
        or col13 = 'UHID - Deposit CB'
        or col13 = 'IP Deposit'
        or col13 = 'IP Refund')
				and col20 = '",cast(v_uhid_no as nchar),"'
				and col38 = '",v_dr_recon_code,"'
        /*and ((col2 <> '0'
        and col2 <> ''
        and col29 like '%COLLECTION%'
        col22 <> 'CREDIT NOTE REFUND') or col22 is null)*/
				and col47 is null
        and col44 = 'Y'
        and col21 like '%IP%'
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
              col7,
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
              '",v_cr_dataset,"',
              '",cast(v_dr_amount as nchar),"',
              '",cast(v_dr_amount as nchar),"',
							'D',
							'-1',
              'IUT','IUT',
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
							'IUT - IP Only',
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
              col7,
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
              '",v_dr_dataset,"',
              '",cast(v_dr_amount as nchar),"',
              '",cast(v_dr_amount as nchar),"',
							'C',
							'1',
              'IUT','IUT',
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
							'IUT - IP Only',
							'",v_cr_loc_code,"',
              '",v_ref_no,"'
					");

					call pr_run_sql2(v_sql,@msg,@result);
      end if;
		end loop dr3_loop;

		close dr3_cursor;
	end dr3_block;

  drop temporary table if exists recon_tmp_tgid1;
  drop temporary table if exists recon_tmp_tuhiddr;
  drop temporary table if exists recon_tmp_tuhiddr1;
  drop temporary table if exists recon_tmp_tuhidcr;
  drop temporary table if exists recon_tmp_tuhidcr1;
  drop temporary table if exists recon_tmp_tuhidrecon;
  drop temporary table if exists recon_tmp_tuhid;
  drop temporary table if exists recon_tmp_treconuhid;
  drop temporary table if exists recon_tmp_treconview;
end $$

DELIMITER ;