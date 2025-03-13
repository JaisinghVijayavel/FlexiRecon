DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_PDclusterIUTRefundCB` $$
CREATE PROCEDURE `pr_set_PDclusterIUTRefundCB`
(
  in in_recon_code text,
  in in_pdrecon_code text,
  in in_cycle_date date,
  in in_cluster_name text,
  in in_unit_name text,
  in in_unitds_code text,
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By - Vijayavel
    Created Date - 12-03-2025

    Updated By -
    Updated Date -

	  Version - 001
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

  drop temporary table if exists recon_tmp_tcb;

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
    from ",in_unitds_code,"
    where col1 = '",in_unit_name,"'
    and col2 = 'IP Refund'
    and col3 <> ''
    and col5 <> ''
    and col9 <> 'TALLIED' ",
    if(in_cycle_date is null,"",concat("and col12 = '",cast(in_cycle_date as nchar),"' ")),
    "and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);

  -- Cluster Recon Field Columns
  -- col6 - Dataset Code
  -- col20 - UHID
  -- col21 - IP/OP No
  -- col22 - Event
  -- col38 - Recon Code
  -- col53 - Closing Balance

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
      and col74 is null
      and delete_flag = 'N'
      LOCK IN SHARE MODE;

      set v_iut_cb_amount = ifnull(@closing_balance,0);
      set v_count = ifnull(@cb_rec_count,0);

      set v_iut_status = '';

      if v_count = 0 then
        set v_iut_status = 'NOT AVAILABLE';
      end if;

      if v_iut_cb_amount = v_cb_amount and v_count > 0 then
        set v_iut_status = 'TALLIED';
      end if;

      -- update the IUT Status
      set v_sql = concat("update ",in_unitds_code," set
          col40 = ",cast(v_iut_cb_amount as nchar),",
          col41 = '",v_iut_status,"'
        where dataset_gid = ",cast(v_dataset_gid as nchar),"
        and delete_flag = 'N'
        ");

      call pr_run_sql2(v_sql,@msg,@result);

      if v_iut_status <> 'NOT AVAILABLE' then
        update recon_trn_ttran set
          col74 = 'Y'
        where recon_code = in_recon_code
        and col38 = in_pdrecon_code
        and col20 = v_uhid_no
        and col21 = v_ip_no
        and col74 is null
        and delete_flag = 'N'
      end if;
	  end loop cb_loop;

		close cb_cursor;
	end cb_block;

  drop temporary table if exists recon_tmp_tcb;
end $$

DELIMITER ;