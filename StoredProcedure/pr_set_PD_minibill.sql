DELIMITER $$

DROP procedure IF EXISTS `pr_set_PD_minibill` $$
CREATE procedure `pr_set_PD_minibill`
(
  in_recon_code varchar(32)
)
me:begin
  declare v_sql text default '';
  declare v_unit_code text default '';
  declare v_bill_type text default '';
  declare v_bill_no text default '';
  declare v_mini_field_name text default '';
  declare v_ds_billsummary text default '';
  declare v_ds_db_name text default '';
  declare v_recon_closure_date text default '';

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

  select
    ifnull(date_format(recon_closure_date,'%Y-%m-%d'),'2000-01-01') into v_recon_closure_date
  from recon_mst_trecon
  where recon_code = in_recon_code
  and period_from <= curdate()
  and (period_to >= curdate()
  or until_active_flag = 'Y')
  and active_status = 'Y'
  and delete_flag = 'N'

  -- OCR mini bill no
  select col2 into v_bill_no from recon_trn_ttranbrkp
  where recon_code = in_recon_code
  and split(col2,'-',2) = 'OCR'
  and delete_flag = 'N'
  limit 0,1;

  set v_bill_no = ifnull(v_bill_no,'');

  set v_unit_code = SPLIT(v_bill_no,'-',1);
  set v_bill_type = SPLIT(v_bill_no,'-',2);

  -- get mini field name
  select
    recon_field_name into v_mini_field_name
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_desc = 'Mini Bill No'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_mini_field_name = ifnull(v_mini_field_name,'');

  if v_bill_no <> '' and v_mini_field_name <> '' then
    /*
    select
      min(cast(split(col2,'-',3) as unsigned)) into @min_bill
    from recon_flexi_dataset_poc.DS151
    where split(col2,'-',1) = v_unit_code
    and split(col2,'-',2) = v_bill_type
    and delete_flag = 'N';
    */

    set v_sql = concat("
      select
        min(cast(split(col2,'-',3) as unsigned)) into @min_bill
      from ",v_ds_billsummary,"
      where split(col2,'-',1) = '",v_unit_code,"'
      and split(col2,'-',2) = '",v_bill_type,"'
      and cast(col8 as date) > '",v_recon_closure_date,"'
      and delete_flag = 'N'
      ");

    call pr_run_sql2(v_sql,@msg,@result);

    set @min_bill = ifnull(@min_bill,0);

    set v_sql = concat("update recon_trn_ttranbrkp set
      ",v_mini_field_name,"='",cast(@min_bill as nchar),"'
      where recon_code = '",in_recon_code,"'
      and split(col2,'-',2) = '",v_bill_type,"'
      and (col5 = 'DEPOSIT'
      or col5 = 'BILL REALIZATION')
      and delete_flag = 'N'");

    call pr_run_sql(v_sql,@msg,@result);
  end if;

  -- OCS mini bill no
  select col2 into v_bill_no from recon_trn_ttranbrkp
  where recon_code = in_recon_code
  and split(col2,'-',2) = 'OCS'
  and delete_flag = 'N'
  limit 0,1;

  set v_bill_no = ifnull(v_bill_no,'');

  set v_unit_code = SPLIT(v_bill_no,'-',1);
  set v_bill_type = SPLIT(v_bill_no,'-',2);

  -- get mini field name
  select
    recon_field_name into v_mini_field_name
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_desc = 'Mini Bill No'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_mini_field_name = ifnull(v_mini_field_name,'');

  if v_bill_no <> '' and v_mini_field_name <> '' then
    /*
    select
      min(cast(split(col2,'-',3) as unsigned)) into @min_bill
    from recon_flexi_dataset_poc.DS151
    where split(col2,'-',1) = v_unit_code
    and split(col2,'-',2) = v_bill_type
    and delete_flag = 'N';
    */

    set v_sql = concat("
      select
        min(cast(split(col2,'-',3) as unsigned)) into @min_bill
      from ",v_ds_billsummary,"
      where split(col2,'-',1) = '",v_unit_code,"'
      and split(col2,'-',2) = '",v_bill_type,"'
      and cast(col8 as date) > '",v_recon_closure_date,"'
      and delete_flag = 'N'
      ");

    call pr_run_sql2(v_sql,@msg,@result);

    set @min_bill = ifnull(@min_bill,0);

    set v_sql = concat("update recon_trn_ttranbrkp set
      ",v_mini_field_name,"='",cast(@min_bill as nchar),"'
      where recon_code = '",in_recon_code,"'
      and split(col2,'-',2) = '",v_bill_type,"'
      and (col5 = 'DEPOSIT'
      or col5 = 'BILL REALIZATION')
      and delete_flag = 'N'");

    call pr_run_sql(v_sql,@msg,@result);
  end if;

  -- ICR mini bill no
  select col2 into v_bill_no from recon_trn_ttranbrkp
  where recon_code = in_recon_code
  and split(col2,'-',2) = 'ICR'
  and delete_flag = 'N'
  limit 0,1;

  set v_bill_no = ifnull(v_bill_no,'');

  set v_unit_code = SPLIT(v_bill_no,'-',1);
  set v_bill_type = SPLIT(v_bill_no,'-',2);

  if v_bill_no <> '' and v_mini_field_name <> '' then
    /*
    select
      min(cast(split(col2,'-',3) as unsigned)) into @min_bill
    from recon_flexi_dataset_poc.DS151
    where split(col2,'-',1) = v_unit_code
    and split(col2,'-',2) = v_bill_type
    and delete_flag = 'N';
    */

    set v_sql = concat("
      select
        min(cast(split(col2,'-',3) as unsigned)) into @min_bill
      from ",v_ds_billsummary,"
      where split(col2,'-',1) = '",v_unit_code,"'
      and split(col2,'-',2) = '",v_bill_type,"'
      and cast(col8 as date) > '",v_recon_closure_date,"'
      and delete_flag = 'N'
      ");

    call pr_run_sql2(v_sql,@msg,@result);

    set @min_bill = ifnull(@min_bill,0);

    set v_sql = concat("update recon_trn_ttranbrkp set
      ",v_mini_field_name,"='",cast(@min_bill as nchar),"'
      where recon_code = '",in_recon_code,"'
      and split(col2,'-',2) = '",v_bill_type,"'
      and (col5 = 'DEPOSIT'
      or col5 = 'BILL REALIZATION')
      and delete_flag = 'N'");

    call pr_run_sql(v_sql,@msg,@result);
  end if;

  -- ICS mini bill no
  select col2 into v_bill_no from recon_trn_ttranbrkp
  where recon_code = in_recon_code
  and split(col2,'-',2) = 'ICS'
  and delete_flag = 'N'
  limit 0,1;

  set v_bill_no = ifnull(v_bill_no,'');

  set v_unit_code = SPLIT(v_bill_no,'-',1);
  set v_bill_type = SPLIT(v_bill_no,'-',2);

  -- get mini field name
  select
    recon_field_name into v_mini_field_name
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_desc = 'Mini Bill No'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_mini_field_name = ifnull(v_mini_field_name,'');

  if v_bill_no <> '' and v_mini_field_name <> '' then
    /*
    select
      min(cast(split(col2,'-',3) as unsigned)) into @min_bill
    from recon_flexi_dataset_poc.DS151
    where split(col2,'-',1) = v_unit_code
    and split(col2,'-',2) = v_bill_type
    and delete_flag = 'N';
    */

    set v_sql = concat("
      select
        min(cast(split(col2,'-',3) as unsigned)) into @min_bill
      from ",v_ds_billsummary,"
      where split(col2,'-',1) = '",v_unit_code,"'
      and split(col2,'-',2) = '",v_bill_type,"'
      and cast(col8 as date) > '",v_recon_closure_date,"'
      and delete_flag = 'N'
      ");

    call pr_run_sql2(v_sql,@msg,@result);

    set @min_bill = ifnull(@min_bill,0);

    set v_sql = concat("update recon_trn_ttranbrkp set
      ",v_mini_field_name,"='",cast(@min_bill as nchar),"'
      where recon_code = '",in_recon_code,"'
      and split(col2,'-',2) = '",v_bill_type,"'
      and (col5 = 'DEPOSIT'
      or col5 = 'BILL REALIZATION')
      and delete_flag = 'N'");

    call pr_run_sql(v_sql,@msg,@result);
  end if;
end $$

DELIMITER ;