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
  where recon_code = 'RE116'
  and recon_field_desc = 'Mini Bill No'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_mini_field_name = ifnull(v_mini_field_name,'');

  if v_bill_no <> '' and v_mini_field_name <> '' then
    select
      min(cast(split(col2,'-',3) as unsigned)) into @min_bill
    from recon_flexi_dataset_poc.DS151
    where split(col2,'-',1) = v_unit_code
    and split(col2,'-',2) = v_bill_type
    and delete_flag = 'N';

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
  where recon_code = 'RE116'
  and recon_field_desc = 'Mini Bill No'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_mini_field_name = ifnull(v_mini_field_name,'');

  if v_bill_no <> '' and v_mini_field_name <> '' then
    select
      min(cast(split(col2,'-',3) as unsigned)) into @min_bill
    from recon_flexi_dataset_poc.DS151
    where split(col2,'-',1) = v_unit_code
    and split(col2,'-',2) = v_bill_type
    and delete_flag = 'N';

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
    select
      min(cast(split(col2,'-',3) as unsigned)) into @min_bill
    from recon_flexi_dataset_poc.DS151
    where split(col2,'-',1) = v_unit_code
    and split(col2,'-',2) = v_bill_type
    and delete_flag = 'N';

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
  where recon_code = 'RE116'
  and recon_field_desc = 'Mini Bill No'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_mini_field_name = ifnull(v_mini_field_name,'');

  if v_bill_no <> '' and v_mini_field_name <> '' then
    select
      min(cast(split(col2,'-',3) as unsigned)) into @min_bill
    from recon_flexi_dataset_poc.DS151
    where split(col2,'-',1) = v_unit_code
    and split(col2,'-',2) = v_bill_type
    and delete_flag = 'N';

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