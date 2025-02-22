DELIMITER $$

DROP procedure IF EXISTS `pr_get_PD_minibill` $$
CREATE procedure `pr_get_PD_minibill`
(
  in_recon_code varchar(32),
  in_user_code varchar(32)
)
me:begin
  declare v_sql text default '';
  declare v_pdunit_code text default '';
  declare v_unit_code text default '';
  declare v_bill_type text default '';
  declare v_bill_no text default '';
  declare v_mini_field_name text default '';
  declare v_ds_billsummary text default '';
  declare v_ds_db_name text default '';
  declare v_recon_closure_date text default '';
  declare v_recon_cycle_date text default '';
  declare v_cycle_date text default '';

  /*
  drop temporary table if exists recon_tmp_tminibill;

  create temporary table if not exists recon_tmp_tminibill
  (
    unit_code varchar(32),
    bill_type varchar(32),
    user_code varchar(32),
    cycle_date text default null,
    mini_billno text default null,
    PRIMARY KEY (unit_code,bill_type,user_code)
  ) ENGINE = MyISAM;
  */

  -- recon closure date
  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and active_status = 'Y'
    and delete_flag = 'N') then
    leave me;
  end if;

  -- delete existing records in temp table
  delete from recon_tmp_tminibill
  where user_code = in_user_code;

  -- get recon closure date
  select
    ifnull(date_format(recon_closure_date,'%Y-%m-%d'),'2000-01-01'),
    ifnull(date_format(recon_cycle_date,'%Y-%m-%d'),'')
  into
    v_recon_closure_date,
    v_recon_cycle_date
  from recon_mst_trecon
  where recon_code = in_recon_code
  and period_from <= curdate()
  and (period_to >= curdate()
  or until_active_flag = 'Y')
  and active_status = 'Y'
  and delete_flag = 'N';

  if v_recon_closure_date <> '' and v_recon_cycle_date <> '' then
    set v_cycle_date = concat(date_format(adddate(cast(v_recon_closure_date as date),1),'%d-%m-%Y'),
                              ' To ',date_format(cast(v_recon_cycle_date as date),'%d-%m-%Y'));
  end if;

  -- get mini field name
  select
    recon_field_name into v_mini_field_name
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_desc = 'Mini Bill No'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_mini_field_name = ifnull(v_mini_field_name,'');

  -- get dataset bill summary
  if exists(select a.dataset_code from recon_mst_trecondataset as a
    inner join recon_mst_tdataset as b on a.dataset_code = b.dataset_code
      and b.dataset_name like 'bill summary%'
      and b.active_status = 'Y'
      and b.delete_flag = 'N'
    where a.recon_code = in_recon_code
    and a.active_status = 'Y'
    and a.delete_flag = 'N') then
    select
      a.dataset_code into v_ds_billsummary
    from recon_mst_trecondataset as a
    inner join recon_mst_tdataset as b on a.dataset_code = b.dataset_code
      and b.dataset_name like 'bill summary%'
      and b.active_status = 'Y'
      and b.delete_flag = 'N'
    where a.recon_code = in_recon_code
    and a.active_status = 'Y'
    and a.delete_flag = 'N';

    set v_ds_billsummary = ifnull(v_ds_billsummary,'');

    if v_ds_billsummary <> '' then
      set v_ds_db_name = fn_get_configvalue('dataset_db_name');

      if v_ds_db_name <> '' then
        set v_ds_billsummary = concat(v_ds_db_name,'.',v_ds_billsummary);
      end if;
    end if;
  else
    leave me;
  end if;

  -- pdunit
  pdunit_block:begin
    declare pdunit_done int default 0;
    declare pdunit_cursor cursor for
      select
        distinct split(col2,'-',1),split(col2,'-',2)
      from recon_trn_ttranbrkp
      where recon_code = in_recon_code
      and split(col2,'-',2) in ('OCS','OCR','ICS','ICR')
      and delete_flag = 'N'
      union
      select
        distinct split(col2,'-',1),split(col2,'-',2)
      from recon_trn_ttran
      where recon_code = in_recon_code
      and split(col2,'-',2) in ('OCS','OCR','ICS','ICR')
      and col14 <> ''
      and delete_flag = 'N';

    declare continue handler for not found set pdunit_done=1;

    open pdunit_cursor;

    pdunit_loop: loop
      fetch pdunit_cursor into v_pdunit_code,v_bill_type;

      if pdunit_done = 1 then leave pdunit_loop; end if;

      set v_pdunit_code = ifnull(v_pdunit_code,'');
      set v_bill_type = ifnull(v_bill_type,'');

			set v_sql = concat("
				select
					min(cast(split(col2,'-',3) as unsigned)) into @min_bill
				from ",v_ds_billsummary,"
				where col2 like '",v_pdunit_code,"%'
				and col2 like '%-",v_bill_type,"-%'
				and cast(col8 as date) > '",v_recon_closure_date,"'
				and delete_flag = 'N'
				");

			call pr_run_sql2(v_sql,@msg,@result);

			set @min_bill = ifnull(@min_bill,0);

      insert into recon_tmp_tminibill(user_code,unit_code,bill_type,cycle_date,mini_billno)
        select in_user_code,v_pdunit_code,v_bill_type,v_cycle_date,@min_bill;
    end loop pdunit_loop;

    close pdunit_cursor;
  end pdunit_block;
end $$

DELIMITER ;