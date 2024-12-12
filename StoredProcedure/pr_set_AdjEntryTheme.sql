DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_AdjEntryTheme $$
CREATE PROCEDURE pr_set_AdjEntryTheme(in_recon_code varchar(32))
me:begin
  declare v_uhid_no text default '';
  declare v_uhid_closing_bal decimal(15,2) default 0;
  declare v_tran_gid int default 0;
  declare v_tranbrkp_gid int default 0;

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_ds_code text default '';
  declare v_tranbrkp_ds_code text default '';

  declare v_sql text default '';

  set v_tran_table = 'recon_trn_ttran';
  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  set v_ds_code = 'DS274';
  set v_tranbrkp_ds_code = 'DS277';

  drop temporary table if exists recon_tmp_tuhid;

  CREATE temporary TABLE recon_tmp_tuhid(
    uhid_no varchar(255),
    tranbrkp_gid int,
    PRIMARY KEY (tranbrkp_gid)
  ) ENGINE = MyISAM;

  -- Column info
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
  set v_sql = concat("insert into recon_tmp_tuhid (uhid_no,tranbrkp_gid)
    select col20,tranbrkp_gid from ",v_tranbrkp_table,"
    where recon_code = '",in_recon_code,"'
    and tranbrkp_dataset_code = '",v_tranbrkp_ds_code,"'
    and col22 = 'Adj Entry'
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- uhid
  uhid_block:begin
    declare uhid_done int default 0;
    declare uhid_cursor cursor for
      select
        uhid_no,tranbrkp_gid
      from recon_tmp_tuhid;
    declare continue handler for not found set uhid_done=1;

    open uhid_cursor;

    uhid_loop: loop
      fetch uhid_cursor into v_uhid_no,v_tranbrkp_gid;

      if uhid_done = 1 then leave uhid_loop; end if;

      set v_uhid_no = ifnull(v_uhid_no,'');
      set v_tranbrkp_gid = ifnull(v_tranbrkp_gid,0);

      -- get uhid closing balance from tran table
			set v_sql = concat("select sum(cast(col53 as decimal(15,2))) into @uhid_closing_bal from ",v_tran_table,"
				where recon_code = '",in_recon_code,"'
				and col20 = '",v_uhid_no,"'
				and delete_flag = 'N'
				");

      call pr_run_sql2(v_sql,@msg,@result);

      set v_uhid_closing_bal = ifnull(@uhid_closing_bal,0);

      -- get uhid closing balance from tranbrkp table
			set v_sql = concat("select sum(cast(col53 as decimal(15,2))) into @uhid_closing_bal from ",v_tranbrkp_table,"
				where recon_code = '",in_recon_code,"'
				and col20 = '",v_uhid_no,"'
				and delete_flag = 'N'
				");

      call pr_run_sql2(v_sql,@msg,@result);

      set v_uhid_closing_bal = v_uhid_closing_bal + ifnull(@uhid_closing_bal,0);

      if v_uhid_closing_bal = 0 then
        -- blank adj entry theme
				set v_sql = concat("update ",v_tranbrkp_table," set
            col13 = ''
					where tranbrkp_gid = ",cast(v_tranbrkp_gid as nchar),"
					and delete_flag = 'N'
					");

        call pr_run_sql2(v_sql,@msg,@result);
      end if;
    end loop uhid_loop;

    close uhid_cursor;
  end uhid_block;

  drop temporary table if exists recon_tmp_tuhid;
end $$

DELIMITER ;