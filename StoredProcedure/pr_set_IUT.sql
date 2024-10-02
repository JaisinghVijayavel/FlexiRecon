DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_IUT $$
CREATE PROCEDURE pr_set_IUT(in_recon_code varchar(32))
me:begin
  declare v_tran_gid int default 0;
  declare v_uhid_no text default '';
  declare v_dr_amount decimal(15,2) default 0;
  declare v_cr_amount decimal(15,2) default 0;
  declare v_recon_code text default '';

  declare v_dr_recon_code text default '';
  declare v_cr_recon_code text default '';

  declare v_tran_dr_gid int default 0;
  declare v_tran_cr_gid int default 0;

  declare v_tran_dr_min_gid int default 0;
  declare v_tran_cr_min_gid int default 0;

  declare v_tran_table text default '';
  declare v_transfer_table text default '';

  declare v_dataset_db_name text default '';
  declare v_sql text default '';
  declare v_succ_flag boolean default false;

  set v_tran_table = 'recon_trn_ttran';

  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  if v_dataset_db_name = '' then
    set v_transfer_table = 'recon_trn_tiutentry';
  else
    set v_transfer_table = concat(v_dataset_db_name,'.recon_trn_tiutentry');
  end if;

  -- trucate entry table
  set v_sql = concat("truncate ",v_transfer_table);
  call pr_run_sql2(v_sql,@msg,@result);


  -- iUT - IP Entry Generation
  -- col38 - Source Recon Code
  -- col41 - IUT Entry Flag
  -- col45 - IUT Recon Code
  -- col47 - IUT IP/OP
  -- col9 - Exception Value
  -- col12 - Dr/Cr Mult

  set v_sql = concat("insert into ",v_transfer_table,"
    (entry_date,uhid_no,transfer_amount,from_recon_code,to_recon_code,ipop_type)
    select
      sysdate(),
      col20,
     cast(col9 as decimal(15,2)),
      col38,
      col45,'IUT - IP'
    from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and col12 = '1'
    and col41 = 'Y'
    and col47 = 'IUT - IP'
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  set v_sql = concat("insert into ",v_transfer_table,"
    (entry_date,uhid_no,transfer_amount,from_recon_code,to_recon_code,ipop_type)
    select
      sysdate(),
      col20,
     cast(col9 as decimal(15,2)),
      col45,
      col38,'IUT - IP'
    from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and col12 = '-1'
    and col41 = 'Y'
    and col47 = 'IUT - IP'
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

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
    tran_date date,
    cr_amount double(15,2) not null default 0,
    min_tran_gid int not null default 0,
    PRIMARY KEY (uhid_no,recon_code)
  ) ENGINE = MyISAM;

  -- unid dr table
  CREATE temporary TABLE recon_tmp_tuhiddr1(
    uhid_no varchar(255),
    recon_code varchar(32),
    dr_amount double(15,2) not null default 0,
    min_tran_gid int not null default 0,
    PRIMARY KEY (uhid_no,recon_code)
  ) ENGINE = MyISAM;

  -- unid table
  CREATE temporary TABLE recon_tmp_tgid1(
    gid integer,
    PRIMARY KEY (gid)
  ) ENGINE = MyISAM;

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
    and col42 is null
    and col20 <> ''
    and col38 <> ''
    and col2 <> '0'
    and col2 <> ''
    and col21 not like '%IP%'
    and col19 not like '%-ICR-%'
    and col19 not like '%-ICS-%'
    and delete_flag = 'N'
    group by col20
    having count(distinct col38) > 1
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  set v_sql = concat("insert into recon_tmp_tuhidrecon (uhid_no,recon_code)
    select a.col20,a.col38 from ",v_tran_table," as a
    inner join recon_tmp_tuhid as b on  a.col20 = b.uhid_no
    where a.recon_code = '",in_recon_code,"'
    and a.col42 is null
    and a.col20 <> ''
    and a.col38 <> ''
    and a.col21 not like '%IP%'
    and a.col19 not like '%-ICR-%'
    and a.col19 not like '%-ICS-%'
    and a.delete_flag = 'N'
    group by a.col20,a.col38
    having sum(cast(a.col9 as decimal(15,2))*cast(a.col12 as signed)) <= 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  truncate recon_tmp_tuhid;
  insert into recon_tmp_tuhid (uhid_no) select distinct uhid_no from recon_tmp_tuhidrecon;

  -- update in tran table
  set v_sql = concat("update ",v_tran_table," as a
    inner join recon_tmp_tuhid as b on a.col20 = b.uhid_no
      set a.col44 = 'Y'
    where a.recon_code = '",in_recon_code,"'
    and a.col42 is null
    and a.col20 <> ''
    and a.col38 <> ''
    and a.col21 not like '%IP%'
    and a.col19 not like '%-ICR-%'
    and a.col19 not like '%-ICS-%'
    and a.delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- col4 - Tran Date
  -- col9 - Exception Value
  -- col12 - Mult
  -- col20 - uhid
  -- col38 - Recon Code
  -- col44 - UHID Multi Location Flag

  -- col23 - PayMode
  -- col22 - Event

  -- Case1 - Deposit Adjustment Transfer-Refund
  -- calculate uhid dr unit wise
  set v_sql = concat("insert into recon_tmp_tuhiddr (uhid_no,recon_code,tran_gid,tran_date,dr_amount)
    select
      col20,
      col38,
      tran_gid,
      cast(col4 as date),
      cast(col9 as decimal(15,2))
    from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and col12 = '-1'
    and col20 <> ''
    and col22 = 'REFUND'
    and col23 = 'Deposit Adjustment/Transfer'
    and col38 <> ''
    and col44 = 'Y'
    and col2 <> '0'
    and col2 <> ''
    and col47 is null
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

	-- dr block
	dr1_block:begin
		declare dr1_done int default 0;
		declare dr1_cursor cursor for
		  select tran_gid,uhid_no,dr_amount,recon_code from recon_tmp_tuhiddr;
		declare continue handler for not found set dr1_done=1;

		open dr1_cursor;

		dr1_loop: loop
			fetch dr1_cursor into v_tran_dr_gid,v_uhid_no,v_dr_amount,v_dr_recon_code;

			if dr1_done = 1 then leave dr1_loop; end if;

			-- check uhid cr
			set v_sql = concat("select count(*) into @rec_count
				from ",v_tran_table,"
				where recon_code = '",in_recon_code,"'
				and col12 = '1'
				and col20 = '",v_uhid_no,"'
				and col38 <> '",v_dr_recon_code,"'
        and cast(col9 as decimal(15,2)) = ",cast(v_dr_amount as nchar),"
				and col38 <> ''
				and col44 = 'Y'
				and col2 <> '0'
				and col2 <> ''
        and col47 is null
				and delete_flag = 'N'
				");

			call pr_run_sql2(v_sql,@msg,@result);

      set @rec_count = ifnull(@rec_count,0);

      if @rec_count > 0 then
        set v_tran_cr_gid = 0;
        set v_cr_recon_code = '';

        set @v_tran_cr_gid = 0;
        set @v_cr_recon_code = '';

				-- check uhid cr
				set v_sql = concat("select tran_gid,col38 into @v_tran_cr_gid,@v_cr_recon_code
					from ",v_tran_table,"
					where recon_code = '",in_recon_code,"'
					and col12 = '1'
					and col20 = '",v_uhid_no,"'
					and col38 <> '",v_dr_recon_code,"'
					and cast(col9 as decimal(15,2)) = ",cast(v_dr_amount as nchar),"
					and col38 <> ''
					and col44 = 'Y'
					and col2 <> '0'
					and col2 <> ''
          and col47 is null
					and delete_flag = 'N' limit 0,1
					");

				call pr_run_sql2(v_sql,@msg,@result);

        set v_tran_cr_gid = ifnull(@v_tran_cr_gid,0);
        set v_cr_recon_code = ifnull(@v_cr_recon_code,'');

        if v_tran_cr_gid > 0 then
          -- dr side
					set v_sql = concat("update ",v_tran_table," set
						col41 = 'Y',
						col45 = '", v_cr_recon_code ,"',
						col46 = '",cast(v_dr_amount as nchar),"',
						col47 = 'IUT - OP'
					where tran_gid = ",cast(v_tran_dr_gid as nchar),"
					");

					call pr_run_sql2(v_sql,@msg,@result);

					-- cr side
					set v_sql = concat("update ",v_tran_table," set
						col47 = 'IUT - OP'
					where tran_gid = ",cast(v_tran_cr_gid as nchar),"
					");

					call pr_run_sql2(v_sql,@msg,@result);

          set v_sql = concat("insert into ",v_transfer_table,"
            (entry_date,uhid_no,transfer_amount,from_recon_code,to_recon_code,ipop_type)
            select
              sysdate(),
              '",v_uhid_no,"',
              '",cast(v_dr_amount as nchar),"',
              '",v_cr_recon_code,"',
              '",v_dr_recon_code,"','IUT - OP'
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
  -- col45 - IUT recon_code
  -- col46 - IUT amount

  -- col23 - PayMode
  -- col22 - Event

  -- find agg negative values
  set v_sql = concat("insert into recon_tmp_tuhiddr1 (uhid_no,recon_code,dr_amount)
    select
      col20,
      col38,
      sum(cast(col9 as decimal(15,2))*cast(col12 as signed))
    from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and col20 <> ''
    and col38 <> ''
    and col44 = 'Y'
    and col47 is null
    and delete_flag = 'N'
    group by col20,col38
    having sum(cast(col9 as decimal(15,2))*cast(col12 as signed)) < 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- calculate uhid cr unit wise
  set v_sql = concat("insert into recon_tmp_tuhidcr1 (uhid_no,recon_code,cr_amount,min_tran_gid)
    select col20,col38,sum(cast(col9 as decimal(15,2))*cast(col12 as signed)),min(tran_gid) from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and col20 <> ''
    and col38 <> ''
    and col44 = 'Y'
    and col47 is null
    and delete_flag = 'N'
    group by col20,col38
    having sum(cast(col9 as decimal(15,2))*cast(col12 as signed)) > 0
    ");

  call pr_run_sql2(v_sql,@msg,@result);

	-- dr block
	dr3_block:begin
		declare dr3_done int default 0;
		declare dr3_cursor cursor for
		  select uhid_no,dr_amount,recon_code from recon_tmp_tuhiddr1;
		declare continue handler for not found set dr3_done=1;

		open dr3_cursor;

		dr3_loop: loop
			fetch dr3_cursor into v_uhid_no,v_dr_amount,v_dr_recon_code;

			if dr3_done = 1 then leave dr3_loop; end if;

      set v_dr_amount = abs(v_dr_amount);
      set v_succ_flag = false;

      if exists(select * from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount = v_dr_amount) then

        select
          recon_code into v_cr_recon_code
        from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount = v_dr_amount
        limit 0,1;

        set @rec_count = 0;

				-- cr side
				set v_sql = concat("select count(*) into @rec_count from ",v_tran_table,"
				where recon_code = '",in_recon_code,"'
				and col20 = '",cast(v_uhid_no as nchar),"'
				and col38 = '",v_cr_recon_code,"'
        and col12 = '1'
        and cast(col9 as decimal(15,2)) = ",cast(v_dr_amount as nchar),"
				and col2 <> '0'
				and col2 <> ''
        and col44 = 'Y'
				and col47 is null
				and delete_flag = 'N'
				");

        call pr_run_sql2(v_sql,@msg,@result);

        set @rec_count = ifnull(@rec_count,0);

        if @rec_count > 0 then
          truncate recon_tmp_tuhidcr;

				  set v_sql = concat(" insert into recon_tmp_tuhidcr (tran_gid,cr_amount)
          select tran_gid,cast(col9 as decimal(15,2)) from ",v_tran_table,"
				  where recon_code = '",in_recon_code,"'
				  and col20 = '",cast(v_uhid_no as nchar),"'
				  and col38 = '",v_cr_recon_code,"'
          and col12 = '1'
          and cast(col9 as decimal(15,2)) = ",cast(v_dr_amount as nchar),"
				  and col2 <> '0'
				  and col2 <> ''
          and col44 = 'Y'
				  and col47 is null
				  and delete_flag = 'N'
          order by cast(col4 as date) asc
          limit 0,1
				  ");

          call pr_run_sql2(v_sql,@msg,@result);

          if exists(select * from recon_tmp_tuhidcr) then
            select tran_gid,cr_amount into v_tran_cr_gid,v_cr_amount from recon_tmp_tuhidcr;

            set v_sql = concat("update ",v_tran_table," set
						    col41 = 'Y',
						    col45 = '", v_dr_recon_code ,"',
						    col46 = '",cast(v_cr_amount as nchar),"',
						    col47 = 'IUT - OP'
              where tran_gid = ",cast(v_tran_cr_gid as nchar),"
              and delete_flag = 'N'
            ");

            call pr_run_sql2(v_sql,@msg,@result);

						-- dr side
						set v_sql = concat("update ",v_tran_table," set
							col47 = 'IUT - OP'
						where recon_code = '",in_recon_code,"'
						and col20 = '",cast(v_uhid_no as nchar),"'
						and col38 = '",v_dr_recon_code,"'
						and col2 <> '0'
						and col2 <> ''
						and col47 is null
            and col44 = 'Y'
						and delete_flag = 'N'
						");

				    call pr_run_sql2(v_sql,@msg,@result);

            -- advice entry
            set v_sql = concat("insert into ",v_transfer_table,"
              (entry_date,uhid_no,transfer_amount,from_recon_code,to_recon_code,ipop_type)
              select
                sysdate(),
                '",v_uhid_no,"',
                '",cast(v_dr_amount as nchar),"',
                '",v_cr_recon_code,"',
                '",v_dr_recon_code,"','IUT - OP'
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

        select
          recon_code,min_tran_gid into v_cr_recon_code,v_tran_cr_min_gid
        from recon_tmp_tuhidcr1
        where uhid_no = v_uhid_no
        and recon_code <> v_dr_recon_code
        and cr_amount >= v_dr_amount
        limit 0,1;

        -- dr side
        set v_sql = concat("update ",v_tran_table," set
          col41 = 'Y',
          col45 = '", v_dr_recon_code ,"',
          col46 = '",cast(v_dr_amount as nchar),"'
        where tran_gid = ",cast(v_tran_cr_min_gid as nchar),"
        ");

        call pr_run_sql2(v_sql,@msg,@result);

				-- cr side
				set v_sql = concat("update ",v_tran_table," set
					col47 = 'IUT - OP'
				where recon_code = '",in_recon_code,"'
				and col20 = '",cast(v_uhid_no as nchar),"'
				and col38 = '",v_cr_recon_code,"'
				and col2 <> '0'
				and col2 <> ''
				and col47 is null
        and col44 = 'Y'
				and delete_flag = 'N'
				");

				call pr_run_sql2(v_sql,@msg,@result);

				-- dr side
				set v_sql = concat("update ",v_tran_table," set
					col47 = 'IUT - OP'
				where recon_code = '",in_recon_code,"'
				and col20 = '",cast(v_uhid_no as nchar),"'
				and col38 = '",v_dr_recon_code,"'
				and col2 <> '0'
				and col2 <> ''
				and col47 is null
        and col44 = 'Y'
				and delete_flag = 'N'
				");

				call pr_run_sql2(v_sql,@msg,@result);

        -- advice entry
        set v_sql = concat("insert into ",v_transfer_table,"
          (entry_date,uhid_no,transfer_amount,from_recon_code,to_recon_code,ipop_type)
          select
            sysdate(),
            '",v_uhid_no,"',
            '",cast(v_dr_amount as nchar),"',
            '",v_cr_recon_code,"',
            '",v_dr_recon_code,"','IUT - OP'
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
end $$

DELIMITER ;