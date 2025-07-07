DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_PDclusterIUTIP_CB` $$
CREATE PROCEDURE `pr_set_PDclusterIUTIP_CB`
(
  in in_recon_code text,
  in in_pdrecon_code text,
  in in_cycle_date date,
  in in_cluster_name text,
  in in_unit_name text,
  in in_cbds_code text,
  in in_ip_type text,
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By - Vijayavel
    Created Date - 12-03-2025

    Updated By - Vijayavel
    Updated Date - 14-03-2025

	  Version - 003
	*/

  declare v_sql text default '';
  declare v_count int default 0;

  declare v_dataset_gid int default 0;
  declare v_unit_name text default '';
  declare v_uhid_no text default '';
  declare v_ip_no text default '';
  declare v_cb_type text default '';
  declare v_cb_amount decimal(15,2) default 0;
  declare v_iut_cb_amount decimal(15,2) default 0;
  declare v_iut_status text default '';
  declare v_adjentry_flag text default '';

  drop temporary table if exists recon_tmp_tcb;
  drop temporary table if exists recon_tmp_tuhidadjentry;

  create temporary table recon_tmp_tcb(
    cb_gid int unsigned NOT NULL AUTO_INCREMENT,
    dataset_gid int unsigned NOT NULL default 0,
    unit_name varchar(128) not null,
    uhid_no varchar(64) not null,
    ip_no varchar(64) not null,
    cb_type varchar(64) not null,
    cb_amount decimal(15,2) not null default 0,
    PRIMARY KEY (cb_gid),
    key idx_uhid_no(uhid_no,ip_no)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tuhidadjentry(
    uhid_no varchar(64) not null,
    PRIMARY KEY (uhid_no)
  ) ENGINE = MyISAM;

  -- Closing Balance Columns
  -- col1 - UNIT NAME
  -- col2 - TYPE
  -- col3 - UHID
  -- col5 - IP NO
  -- col6 - AMOUNT

  -- col40 - IUT Amount
  -- col41 - IUT Status

  -- set closing balance
  set v_sql = concat("insert into recon_tmp_tcb
    (
      dataset_gid,
      unit_name,
      uhid_no,
      ip_no,
      cb_type,
      cb_amount
    )
    select
      dataset_gid,
      col1,
      col3,
      col5,
      col2,
      cast(col6 as decimal(15,2))
    from ",in_cbds_code,"
    where col1 = '",in_unit_name,"'
    and col2 = '",in_ip_type,"'
    and col3 <> ''
    and col5 <> ''
    and cast(col6 as decimal(15,2)) <> 0 ",
    if(in_cycle_date is null,"",concat("and col12 = '",cast(in_cycle_date as nchar),"' ")),
    "and delete_flag = 'N'");

    -- and col9 <> 'TALLIED' ",

  call pr_run_sql2(v_sql,@msg,@result);

  -- adj entry uhid
  insert into recon_tmp_tuhidadjentry(uhid_no)
  select
    distinct col20
  from recon_trn_ttranbrkp
  where recon_code = in_recon_code
  and col16 = 'Adj Entry'
  and delete_flag = 'N'
  LOCK IN SHARE MODE;

  -- Cluster Recon Field Columns
  -- col6 - Dataset Code
  -- col19 - Bill No
  -- col20 - UHID
  -- col21 - IP/OP No
  -- col22 - Event
  -- col38 - Recon Code
  -- col53 - Closing Balance
  -- col74 - IUT CB Flag
  -- col75 - IUT DB Type

  -- IP Refund CB get
	cb_block:begin
		declare cb_done int default 0;
		declare cb_cursor cursor for
		select
      dataset_gid,
      unit_name,
      uhid_no,
      ip_no,
      cb_type,
      cb_amount
    from recon_tmp_tcb;

		declare continue handler for not found set cb_done=1;

		open cb_cursor;

		cb_loop: loop
			fetch cb_cursor into v_dataset_gid,
                           v_unit_name,
                           v_uhid_no,
                           v_ip_no,
                           v_cb_type,
                           v_cb_amount;

			if cb_done = 1 then leave cb_loop; end if;

      set v_cb_amount = ifnull(v_cb_amount,0);

      -- case 1
      -- get closing balance sum
      select
        sum(cast(col53 as decimal(15,2))),count(*)
      into
        @closing_balance,@cb_rec_count
      from recon_trn_ttran
      where recon_code = in_recon_code
      and col38 = in_pdrecon_code
      and col20 = v_uhid_no
      and col19 not like '%-OC%'
      and (col21 = v_ip_no or col21 = col20 or col21 = '' or col21 is null)
      and cast(col53 as decimal(15,2)) <> 0
      and col74 is null
      and delete_flag = 'N'
      LOCK IN SHARE MODE;

      set v_iut_cb_amount = ifnull(@closing_balance,0);
      set v_count = ifnull(@cb_rec_count,0);

      set v_adjentry_flag = 'N';

      if exists(select uhid_no from recon_tmp_tuhidadjentry
        where uhid_no = v_uhid_no) then
        -- Adj Entry Sum
        select
          sum(cast(col53 as decimal(15,2))),count(*)
        into
          @closing_balance,@cb_rec_count
        from recon_trn_ttranbrkp
        where recon_code = in_recon_code
        and col38 = in_pdrecon_code
        and col20 = v_uhid_no
        and col19 not like '%-OC%'
        and (col21 = v_ip_no or col21 = col20 or col21 = '' or col21 is null)
        and cast(col53 as decimal(15,2)) <> 0
        and col16 = 'Adj Entry'
        and col74 is null
        and delete_flag = 'N'
        LOCK IN SHARE MODE;

        set v_iut_cb_amount = v_iut_cb_amount + ifnull(@closing_balance,0);
        set v_count = v_count + ifnull(@cb_rec_count,0);

        -- adjentry flag
        if @cb_rec_count > 0 then
          set v_adjentry_flag = 'Y';
        else
          set v_adjentry_flag = 'N';
        end if;
      end if;

      set v_iut_status = '';

      if v_count = 0 then
        set v_iut_status = 'NOT AVAILABLE';
      end if;

      if v_iut_cb_amount = v_cb_amount and v_count > 0 then
        set v_iut_status = 'TALLIED';

        -- update the IUT Status
        set v_sql = concat("update ",in_cbds_code," set
            col40 = ",cast(v_iut_cb_amount as nchar),",
            col41 = '",v_iut_status,"'
          where dataset_gid = ",cast(v_dataset_gid as nchar),"
          and delete_flag = 'N'
          ");

        call pr_run_sql2(v_sql,@msg,@result);

        update recon_trn_ttran set
          col74 = 'Y',
          col75 = in_ip_type
        where recon_code = in_recon_code
        and col38 = in_pdrecon_code
        and col20 = v_uhid_no
        and col19 not like '%-OC%'
        and (col21 = v_ip_no or col21 = col20 or col21 = '' or col21 is null)
        and cast(col53 as decimal(15,2)) <> 0
        and col74 is null
        and delete_flag = 'N';

        if v_adjentry_flag = 'Y' then
          update recon_trn_ttranbrkp set
            col74 = 'Y',
            col75 = in_ip_type
          where recon_code = in_recon_code
          and col38 = in_pdrecon_code
          and col16 = 'Adj Entry'
          and col20 = v_uhid_no
          and col19 not like '%-OC%'
          and (col21 = v_ip_no or col21 = col20 or col21 = '' or col21 is null)
          and cast(col53 as decimal(15,2)) <> 0
          and col74 is null
          and delete_flag = 'N';
        end if;
      end if;

      -- case 2
      if v_iut_status <> 'TALLIED' then
				-- get closing balance sum
				select
					sum(cast(col53 as decimal(15,2))),count(*)
				into
					@closing_balance,@cb_rec_count
				from recon_trn_ttran
				where recon_code = in_recon_code
				and col38 = in_pdrecon_code
				and col20 = v_uhid_no
        and col21 = v_ip_no
        and cast(col53 as decimal(15,2)) <> 0
				and col74 is null
				and delete_flag = 'N'
				LOCK IN SHARE MODE;

				set v_iut_cb_amount = ifnull(@closing_balance,0);
        set v_count = ifnull(@cb_rec_count,0);

        set v_adjentry_flag = 'N';
				set v_iut_status = '';

        if exists(select uhid_no from recon_tmp_tuhidadjentry
          where uhid_no = v_uhid_no) then
          -- Adj Entry Sum
          select
            sum(cast(col53 as decimal(15,2))),count(*)
          into
            @closing_balance,@cb_rec_count
          from recon_trn_ttranbrkp
          where recon_code = in_recon_code
          and col38 = in_pdrecon_code
          and col20 = v_uhid_no
          and col21 = v_ip_no
          and cast(col53 as decimal(15,2)) <> 0
          and col16 = 'Adj Entry'
          and col74 is null
          and delete_flag = 'N'
          LOCK IN SHARE MODE;

          set v_iut_cb_amount = v_iut_cb_amount + ifnull(@closing_balance,0);
          set v_count = v_count + ifnull(@cb_rec_count,0);

          -- adjentry flag
          if @cb_rec_count > 0 then
            set v_adjentry_flag = 'Y';
          else
            set v_adjentry_flag = 'N';
          end if;
        end if;

				if v_count = 0 then
					set v_iut_status = 'NOT AVAILABLE';
				end if;

				if v_iut_cb_amount = v_cb_amount and v_count > 0 then
					set v_iut_status = 'TALLIED';
        end if;

        -- update the IUT Status
        set v_sql = concat("update ",in_cbds_code," set
            col40 = ",cast(v_iut_cb_amount as nchar),",
            col41 = '",v_iut_status,"'
          where dataset_gid = ",cast(v_dataset_gid as nchar),"
          and delete_flag = 'N'
          ");

        call pr_run_sql2(v_sql,@msg,@result);

        -- update in tran table
        update recon_trn_ttran set
          col74 = 'Y',
          col75 = in_ip_type
        where recon_code = in_recon_code
        and col38 = in_pdrecon_code
        and col20 = v_uhid_no
        and col21 = v_ip_no
        and cast(col53 as decimal(15,2)) <> 0
        and col74 is null
        and delete_flag = 'N';

        if v_adjentry_flag = 'Y' then
					update recon_trn_ttranbrkp set
						col74 = 'Y',
            col75 = in_ip_type
					where recon_code = in_recon_code
					and col38 = in_pdrecon_code
          and col16 = 'Adj Entry'
					and col20 = v_uhid_no
				  and col21 = v_ip_no
          and cast(col53 as decimal(15,2)) <> 0
					and col74 is null
					and delete_flag = 'N';
				end if;
      end if;
	  end loop cb_loop;

		close cb_cursor;
	end cb_block;

  drop temporary table if exists recon_tmp_tcb;
  drop temporary table if exists recon_tmp_tuhidadjentry;
end $$

DELIMITER ;