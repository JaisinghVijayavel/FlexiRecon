DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_brsmonthend` $$
CREATE PROCEDURE `pr_get_brsmonthend`
(
  in in_recon_code varchar(32),
  in in_tran_date date,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_particulars text default '';
  declare v_value double(15,2) default 0;
  declare v_count int default 0;
  declare v_diff_value double(15,2) default 0;
  declare v_dataset_code varchar(32) default '';
  declare v_tran_acc_mode varchar(8) default '';
  declare v_base_bal_value double(15,2) default 0;
  declare v_base_bal_date date;
  declare v_target_bal_value double(15,2) default 0;
  declare v_target_bal_date date;
  declare v_cr_total double(15,2) default 0;
  declare v_dr_total double(15,2) default 0;
  declare v_base_dataset varchar(32) default '';
  declare v_target_dataset varchar(32) default '';
  declare v_base_dataset_name varchar(255) default '';
  declare v_target_dataset_name varchar(255) default '';
  declare v_txt text default '';
  declare v_web_date_format text default '';

  declare v_recon_name varchar(255) default '';

  declare v_base_dr_total double(15,2) default 0;
  declare v_base_cr_total double(15,2) default 0;

  declare v_target_dr_total double(15,2) default 0;
  declare v_target_cr_total double(15,2) default 0;

  declare v_base_dr_count int default 0;
  declare v_base_cr_count int default 0;

  declare v_target_dr_count int default 0;
  declare v_target_cr_count int default 0;

  declare v_sql text default '';
  declare v_recon_field text default '';
  declare v_recon_field_desc text default '';

  declare v_tran_date text default '';
  declare v_condition text default '';

  set v_web_date_format = fn_get_configvalue('web_date_format');

  set v_web_date_format = ifnull(v_web_date_format,'%d-%m-%Y');

  drop temporary table if exists tb_brs;
  drop temporary table if exists tb_dataset;

  create temporary table if not exists tb_brs
  (
    brs_gid int unsigned not null auto_increment,
    particulars text default null,
    tran_value text default null,
    tran_acc_mode varchar(32) default null,
    bal_value text default null,
    PRIMARY KEY (brs_gid)
  ) ENGINE = MyISAM;

  create temporary table if not exists tb_dataset
  (
    recon_code varchar(32),
    dataset_code varchar(32),
    dataset_name varchar(255),
    dataset_type varchar(32),
    recon_name text,
    recontype_code varchar(32),
    PRIMARY KEY (recon_code,dataset_code),
    key idx_recontype_code (recon_code,recontype_code)
  ) ENGINE = MyISAM;

  insert into tb_dataset select
                            a.recon_code,a.dataset_code,c.dataset_name,a.dataset_type,b.recon_name,b.recontype_code
                         from recon_mst_trecondataset as a
                         inner join recon_mst_trecon as b on a.recon_code = b.recon_code and b.active_status = 'Y'
                           and b.delete_flag = 'N'
                         inner join recon_mst_tdataset as c on a.dataset_code = c.dataset_code and c.delete_flag = 'N'
                         where a.recon_code = in_recon_code
                         and a.dataset_type in ('B','T')
                         and a.active_status = 'Y'
                         and a.delete_flag = 'N';

  drop temporary table if exists tb_balance;

  create temporary table if not exists tb_balance
  (
    bal_gid int unsigned not null auto_increment,
    dataset_code varchar(32) default null,
    dataset_type varchar(32) default null,
    tran_date date default null,
    bal_value text default null,
    PRIMARY KEY (bal_gid)
  );

  dataset_block:begin
    declare dataset_done int default 0;
    declare dataset_cursor cursor for
      select
        a.dataset_code
      from recon_mst_trecondataset as a
      where a.recon_code = in_recon_code
      and a.dataset_type <> 'S'
      and a.active_status = 'Y'
      and a.delete_flag = 'N';
    declare continue handler for not found set dataset_done=1;

    open dataset_cursor;

    dataset_loop: loop
      fetch dataset_cursor into v_dataset_code;

      if dataset_done = 1 then leave dataset_loop; end if;

      insert into tb_balance(dataset_code,dataset_type,tran_date,bal_value)
        select
          a.dataset_code,b.dataset_type,a.tran_date,
          a.bal_value
        from recon_trn_taccbal as a
        inner join recon_mst_trecondataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
        where b.recon_code = in_recon_code
        and a.dataset_code = v_dataset_code
        and a.tran_date <= in_tran_date
        and a.delete_flag = 'N' order by tran_date desc limit 0,1;
    end loop dataset_loop;

    close dataset_cursor;
  end dataset_block;

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and recontype_code = 'B'
    and active_status = 'Y'
    and delete_flag = 'N') then

    set out_msg = 'Selected Recon is not BRS';
    set out_result = 0;

    select * from tb_brs;
    leave me;
  else
    select
      group_concat(dataset_code),
      group_concat(dataset_name)
    into
      v_base_dataset,
      v_base_dataset_name
    from tb_dataset
    where recon_code = in_recon_code
    and dataset_type = 'B';

    select
      group_concat(dataset_code),
      group_concat(dataset_name)
    into
      v_target_dataset,
      v_target_dataset_name
    from tb_dataset
    where recon_code = in_recon_code
    and dataset_type = 'T';
  end if;

  -- Base dataset balance
  select
    sum(a.bal_value),max(a.tran_date)
  into
    v_base_bal_value,v_base_bal_date
  from tb_balance as a
  where a.dataset_type = 'B';

  set v_base_bal_value = ifnull(v_base_bal_value,0);
  set v_base_bal_date = ifnull(v_base_bal_date,in_tran_date);

  if v_base_bal_value >= 0 then
    set v_tran_acc_mode = 'CR';
  else
    set v_tran_acc_mode = 'DR';
  end if;

  -- base credit exception
  select
    sum(a.excp_value),count(*)
  into
    v_base_cr_total,v_base_cr_count
  from recon_trn_ttran as a
  inner join tb_dataset as b
    on a.dataset_code = b.dataset_code
    and b.dataset_type = 'B'
  where a.recon_code = in_recon_code
  and a.excp_value > 0
  and a.tran_acc_mode = 'C'
  and a.delete_flag = 'N';

  set v_base_cr_total = ifnull(v_base_cr_total,0);
  set v_base_cr_count = ifnull(v_base_cr_count,0);
  set v_cr_total = v_cr_total + v_base_cr_total;

  -- target credit exception
  select
    sum(a.excp_value),count(*)
  into
    v_target_cr_total,v_target_cr_count
  from recon_trn_ttran as a
  inner join tb_dataset as b
    on a.dataset_code = b.dataset_code
    and b.dataset_type = 'T'
  where a.recon_code = in_recon_code
  and a.excp_value > 0
  and a.tran_acc_mode = 'C'
  and a.delete_flag = 'N';

  set v_target_cr_total = ifnull(v_target_cr_total,0);
  set v_target_cr_count = ifnull(v_target_cr_count,0);
  set v_cr_total = v_cr_total + v_target_cr_total;

  -- base debit total
  select
    sum(a.excp_value),count(*)
  into
    v_base_dr_total,v_base_dr_count
  from recon_trn_ttran as a
  inner join tb_dataset as b
    on a.dataset_code = b.dataset_code
    and b.dataset_type = 'B'
  where a.recon_code = in_recon_code
  and a.excp_value > 0
  and a.tran_acc_mode = 'D'
  and a.delete_flag = 'N';

  set v_base_dr_total = ifnull(v_base_dr_total,0);
  set v_base_dr_count = ifnull(v_base_dr_count,0);
  set v_dr_total = v_dr_total + v_base_dr_total;

  -- target debit total
  select
    sum(a.excp_value),count(*)
  into
    v_target_dr_total,v_target_dr_count
  from recon_trn_ttran as a
  inner join tb_dataset as b
    on a.dataset_code = b.dataset_code
    and b.dataset_type = 'T'
  where a.recon_code = in_recon_code
  and a.excp_value > 0
  and a.tran_acc_mode = 'D'
  and a.delete_flag = 'N';

  set v_target_dr_total = ifnull(v_target_dr_total,0);
  set v_target_dr_count = ifnull(v_target_dr_count,0);
  set v_dr_total = v_dr_total + v_target_dr_total;

  -- target dataset balance
  select
    sum(a.bal_value) as bal_value,max(a.tran_date) as tran_date
  into
    v_target_bal_value,v_target_bal_date
  from tb_balance as a
  where a.dataset_type = 'T';

  set v_target_bal_value = ifnull(v_target_bal_value,0);
  set v_target_bal_date = ifnull(v_target_bal_date,in_tran_date);


  set v_value = v_cr_total - v_dr_total + (v_base_bal_value * -1);
  set v_value = round(v_value,2);
  set v_target_bal_value = round(v_target_bal_value,2);

  set v_diff_value = round(v_value-v_target_bal_value,2);

  select  fn_get_configvalue('entity_name') as entity,
          v_recon_name as recon_name,
          v_base_dataset_name as source_dataset_name,
          v_base_bal_date as source_bal_date,
          v_base_bal_value as source_bal_value,
          v_base_dr_total as source_dr_total,
          v_base_cr_total as source_cr_total,
          v_base_dr_count as source_dr_count,
          v_base_cr_count as source_cr_count,
          v_target_dataset_name as target_dataset_name,
          v_target_bal_date as target_bal_date,
          v_target_bal_value as target_bal_value,
          v_target_dr_total as target_dr_total,
          v_target_cr_total as target_cr_total,
          v_target_dr_count as target_dr_count,
          v_target_cr_count as target_cr_count;

  -- get recon field
  select
    group_concat('a.',recon_field_name,' as ',char(39),ifnull(recon_field_desc,recon_field_name),char(39)),
    group_concat("null as '",ifnull(recon_field_desc,recon_field_name),"'")
  into
    v_recon_field,
    v_recon_field_desc
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_name like 'col%'
  and display_flag = 'Y'
  and active_status = 'Y'
  and delete_flag = 'N'
  order by recon_field_sno;

  set v_recon_field = ifnull(v_recon_field,'');
  set v_recon_field_desc = ifnull(v_recon_field_desc,'');

  if v_recon_field <> '' then
    set v_recon_field = concat(v_recon_field,',');
    set v_recon_field_desc = concat(v_recon_field_desc,',');
  end if;

  set v_tran_date = date_format(in_tran_date,'%Y-%m-%d');

  -- target debit
  set v_sql = concat("
  select
    a.tran_gid as 'Tran ID',
    date_format(a.tran_date,'%d-%m-%Y') as 'Transaction Date',",
    "a.tran_value as 'Value',
    a.excp_value as 'Exception Value',
    '' as ' ',",
    v_recon_field,"
    datediff(curdate(),a.tran_date) as 'Pending Days',
    if (datediff(curdate(),a.tran_date)<=3,1,null) as '0>=3',
    if (datediff(curdate(),a.tran_date)>3 and datediff(curdate(),tran_date)<=7,2,null) as '4>=7',
    if (datediff(curdate(),a.tran_date)>7 and datediff(curdate(),tran_date)<=15,3,null) as '8><=15',
    if (datediff(curdate(),a.tran_date)>15 and datediff(curdate(),tran_date)<=30,4,null) as '16><=30',
    if (datediff(curdate(),a.tran_date)>30 and datediff(curdate(),tran_date)<=60,5,null) as '31><=60',
    if (datediff(curdate(),a.tran_date)>60 and datediff(curdate(),tran_date)<=90,6,null) as '61><=90',
    if (datediff(curdate(),a.tran_date)>90,7,null) as '>91'
  from recon_trn_ttran as a
  inner join recon_mst_trecondataset as b on a.recon_code = b.recon_code
    and b.dataset_code = a.dataset_code
    and b.dataset_type = 'T'
    and b.delete_flag = 'N'
  where a.recon_code = '",in_recon_code,"'
  and a.excp_value > 0
  and a.tran_date <= '",v_tran_date,"'
  and a.tran_acc_mode = 'D'
  and a.delete_flag = 'N'
  union all
  select
    null as 'Tran ID',
    null as 'Transaction Date',
    null as 'Tran Value',
    null as 'Exception Value',",
    v_recon_field_desc,"
    null as ' ',
    null as 'Pending Days',
    null as '0>=3',
    null as '4>=7',
    null as '8><=15',
    null as '16><=30',
    null as '31><=60',
    null as '61><=90',
    null as '>91';
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- target credit
  set v_sql = concat("
   select
    a.tran_gid as 'Tran ID',
    date_format(a.tran_date,'%d-%m-%Y') as 'Transaction Date',",
    "a.tran_value as 'Value',
    a.excp_value as 'Exception Value',
    '' as ' ',",
    v_recon_field,"
    datediff(curdate(),a.tran_date) as 'Pending Days',
    if (datediff(curdate(),a.tran_date)<=3,1,null) as '0>=3',
    if (datediff(curdate(),a.tran_date)>3 and datediff(curdate(),tran_date)<=7,2,null) as '4>=7',
    if (datediff(curdate(),a.tran_date)>7 and datediff(curdate(),tran_date)<=15,3,null) as '8><=15',
    if (datediff(curdate(),a.tran_date)>15 and datediff(curdate(),tran_date)<=30,4,null) as '16><=30',
    if (datediff(curdate(),a.tran_date)>30 and datediff(curdate(),tran_date)<=60,5,null) as '31><=60',
    if (datediff(curdate(),a.tran_date)>60 and datediff(curdate(),tran_date)<=90,6,null) as '61><=90',
    if (datediff(curdate(),a.tran_date)>90,7,null) as '>91'
  from recon_trn_ttran as a
  inner join recon_mst_trecondataset as b on a.recon_code = b.recon_code
    and b.dataset_code = a.dataset_code
    and b.dataset_type = 'T'
    and b.delete_flag = 'N'
  where a.recon_code = '",in_recon_code,"'
  and a.excp_value > 0
  and a.tran_date <= '",v_tran_date,"'
  and a.tran_acc_mode = 'C'
  and a.delete_flag = 'N'
  union all
  select
    null as 'Tran ID',
    null as 'Transaction Date',
    null as 'Tran Value',
    null as 'Exception Value',",
    v_recon_field_desc,"
    null as ' ',
    null as 'Pending Days',
    null as '0>=3',
    null as '4>=7',
    null as '8><=15',
    null as '16><=30',
    null as '31><=60',
    null as '61><=90',
    null as '>91';
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- Base Debit
  set v_sql = concat("
   select
    a.tran_gid as 'Tran ID',
    date_format(a.tran_date,'%d-%m-%Y') as 'Transaction Date',",
    "a.tran_value as 'Value',
    a.excp_value as 'Exception Value',
    '' as ' ',",
    v_recon_field,"
    datediff(curdate(),a.tran_date) as 'Pending Days',
    if (datediff(curdate(),a.tran_date)<=3,1,null) as '0>=3',
    if (datediff(curdate(),a.tran_date)>3 and datediff(curdate(),tran_date)<=7,2,null) as '4>=7',
    if (datediff(curdate(),a.tran_date)>7 and datediff(curdate(),tran_date)<=15,3,null) as '8><=15',
    if (datediff(curdate(),a.tran_date)>15 and datediff(curdate(),tran_date)<=30,4,null) as '16><=30',
    if (datediff(curdate(),a.tran_date)>30 and datediff(curdate(),tran_date)<=60,5,null) as '31><=60',
    if (datediff(curdate(),a.tran_date)>60 and datediff(curdate(),tran_date)<=90,6,null) as '61><=90',
    if (datediff(curdate(),a.tran_date)>90,7,null) as '>91'
  from recon_trn_ttran as a
  inner join recon_mst_trecondataset as b on a.recon_code = b.recon_code
    and b.dataset_code = a.dataset_code
    and b.dataset_type = 'B'
    and b.delete_flag = 'N'
  where a.recon_code = '",in_recon_code,"'
  and a.excp_value > 0
  and a.tran_date <= '",v_tran_date,"'
  and a.tran_acc_mode = 'D'
  and a.delete_flag = 'N'
  union all
  select
    null as 'Tran ID',
    null as 'Transaction Date',
    null as 'Tran Value',
    null as 'Exception Value',",
    v_recon_field_desc,"
    null as ' ',
    null as 'Pending Days',
    null as '0>=3',
    null as '4>=7',
    null as '8><=15',
    null as '16><=30',
    null as '31><=60',
    null as '61><=90',
    null as '>91';
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- base credit
  set v_sql = concat("
   select
    a.tran_gid as 'Tran ID',
    date_format(a.tran_date,'%d-%m-%Y') as 'Transaction Date',",
    "a.tran_value as 'Value',
    a.excp_value as 'Exception Value',
    '' as ' ',",
    v_recon_field,"
    datediff(curdate(),a.tran_date) as 'Pending Days',
    if (datediff(curdate(),a.tran_date)<=3,1,null) as '0>=3',
    if (datediff(curdate(),a.tran_date)>3 and datediff(curdate(),tran_date)<=7,2,null) as '4>=7',
    if (datediff(curdate(),a.tran_date)>7 and datediff(curdate(),tran_date)<=15,3,null) as '8><=15',
    if (datediff(curdate(),a.tran_date)>15 and datediff(curdate(),tran_date)<=30,4,null) as '16><=30',
    if (datediff(curdate(),a.tran_date)>30 and datediff(curdate(),tran_date)<=60,5,null) as '31><=60',
    if (datediff(curdate(),a.tran_date)>60 and datediff(curdate(),tran_date)<=90,6,null) as '61><=90',
    if (datediff(curdate(),a.tran_date)>90,7,null) as '>91'
  from recon_trn_ttran as a
  inner join recon_mst_trecondataset as b on a.recon_code = b.recon_code
    and b.dataset_code = a.dataset_code
    and b.dataset_type = 'B'
    and b.delete_flag = 'N'
  where a.recon_code = '",in_recon_code,"'
  and a.excp_value > 0
  and a.tran_date <= '",v_tran_date,"'
  and a.tran_acc_mode = 'C'
  and a.delete_flag = 'N'
  union all
  select
    null as 'Tran ID',
    null as 'Transaction Date',
    null as 'Tran Value',
    null as 'Exception Value',",
    v_recon_field_desc,"
    null as ' ',
    null as 'Pending Days',
    null as '0>=3',
    null as '4>=7',
    null as '8><=15',
    null as '16><=30',
    null as '31><=60',
    null as '61><=90',
    null as '>91';
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- ageing
  select
    b.aging_desc,
    sum(if(c.dataset_type = 'B' and a.tran_acc_mode = 'D',a.excp_value,0)) as base_dr_value,
    sum(if(c.dataset_type = 'B' and a.tran_acc_mode = 'C',a.excp_value,0)) as base_cr_value,
    sum(if(c.dataset_type = 'T' and a.tran_acc_mode = 'D',a.excp_value,0)) as target_dr_value,
    sum(if(c.dataset_type = 'T' and a.tran_acc_mode = 'C',a.excp_value,0)) as target_cr_value,
    sum(if(c.dataset_type = 'B' and a.tran_acc_mode = 'D',1,0)) as base_dr_count,
    sum(if(c.dataset_type = 'B' and a.tran_acc_mode = 'C',1,0)) as base_cr_count,
    sum(if(c.dataset_type = 'T' and a.tran_acc_mode = 'D',1,0)) as target_dr_count,
    sum(if(c.dataset_type = 'T' and a.tran_acc_mode = 'C',1,0)) as target_cr_count
  from recon_trn_ttran as a
  inner join recon_mst_taging as b on datediff(curdate(),a.tran_date) between b.aging_from and b.aging_to and b.delete_flag = 'N'
  inner join recon_mst_trecondataset as c on a.recon_code = c.recon_code and c.delete_flag = 'N'
  where a.recon_code = in_recon_code
  and a.excp_value > 0
  and a.tran_date <= in_tran_date
  and a.delete_flag = 'N';

  if exists(select tran_gid from recon_trn_ttran
    where recon_code = in_recon_code
    and tran_date <= in_tran_date
    and excp_value > 0
    and delete_flag = 'N') then

    set v_condition = concat(" and recon_code = '",in_recon_code,"' ",
                             " and tran_date <= '",v_tran_date,"' ",
                             " and excp_value > 0 ",
                             " and delete_flag = 'N' ");
  else
    set v_condition = concat(" and 1 = 2 ");
  end if;



   call pr_get_tablequery(in_recon_code,'recon_trn_ttran',v_condition,0,in_user_code,@msg,@result);


  if exists(select tranbrkp_gid from recon_trn_ttranbrkp
    where tran_gid in
    (
      select tran_gid from recon_trn_ttran
      where recon_code = in_recon_code
      and tran_date <= in_tran_date
      and excp_value > 0
      and delete_flag = 'N'
    )
    and excp_value > 0
    and delete_flag = 'N') then
    set v_condition = concat(" and recon_code = '",in_recon_code,"' ",
                           " and excp_value > 0 ",
                           " and tran_gid in (select tran_gid from recon_trn_ttran where true ",
                           v_condition,") ",
                           " and delete_flag = 'N' ");

  else
    set v_condition = concat(" and 1 = 2 ");
  end if;

  call pr_get_tablequery(in_recon_code,'recon_trn_ttranbrkp',v_condition,0,in_user_code,@msg,@result);
end $$

DELIMITER ;