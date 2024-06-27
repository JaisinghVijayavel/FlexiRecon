DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_brssummary` $$
CREATE PROCEDURE `pr_get_brssummary`
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

  declare v_threshold_value double(15,2) default 0;
  declare v_threshold_count int default 0;
  declare v_threshold_total double(15,2) default 0;

  set v_web_date_format = fn_get_configvalue('web_date_format');

  set v_web_date_format = ifnull(v_web_date_format,'%d-%m-%Y');

  call pr_get_rollbacktran(in_recon_code,in_tran_date,@msg,@result);

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

  -- get recon threshold value
	select
		(threshold_plus_value+abs(threshold_minus_value))
	into
		v_threshold_value
	from recon_mst_trecon
	where recon_code = in_recon_code
	and delete_flag = 'N';

	set v_threshold_value = ifnull(v_threshold_value,0);

  -- rounding off
  if v_threshold_value > 0 then
		select
      sum(a.excp_value*a.tran_mult),count(*)
    into
      v_value,v_count
    from recon_tmp_ttran as a
		where a.recon_code = in_recon_code
		and a.excp_value <> 0
    and a.roundoff_value <> 0
		and a.tran_value <> a.excp_value
    and (a.excp_value - a.roundoff_value * a.tran_mult) = 0
		and a.delete_flag = 'N';

    set v_value = ifnull(v_value,0);
    set v_count = ifnull(v_count,0);

    set v_threshold_count = v_count;
    set v_threshold_total = v_value;
  end if;

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

  -- roundoff diff
  set v_sql = concat("
   select
    a.tran_gid as 'Tran ID',
    date_format(a.tran_date,'%d-%m-%Y') as 'Transaction Date',",
    "a.tran_value as 'Value',
    a.roundoff_value as 'Exception Value',",
    v_recon_field,"
    datediff(curdate(),a.tran_date) as 'Pending Days',
    if (datediff(curdate(),a.tran_date)<=3,1,null) as '0>=3',
    if (datediff(curdate(),a.tran_date)>3 and datediff(curdate(),tran_date)<=7,2,null) as '4>=7',
    if (datediff(curdate(),a.tran_date)>7 and datediff(curdate(),tran_date)<=15,3,null) as '8><=15',
    if (datediff(curdate(),a.tran_date)>15 and datediff(curdate(),tran_date)<=30,4,null) as '16><=30',
    if (datediff(curdate(),a.tran_date)>30 and datediff(curdate(),tran_date)<=60,5,null) as '31><=60',
    if (datediff(curdate(),a.tran_date)>60 and datediff(curdate(),tran_date)<=90,6,null) as '61><=90',
    if (datediff(curdate(),a.tran_date)>90,7,null) as '>91'
  from recon_tmp_ttran as a
  inner join recon_mst_trecondataset as b on a.recon_code = b.recon_code
    and b.dataset_code = a.dataset_code
    and b.delete_flag = 'N'
  where a.recon_code = '",in_recon_code,"'
  and a.excp_value <> 0
  and a.tran_date <= '",v_tran_date,"'
  and a.delete_flag = 'N'
  union all
  select
    null as 'Tran ID',
    null as 'Transaction Date',
    null as 'Tran Value',
    null as 'Exception Value',",
    v_recon_field_desc,"
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

  select
    a.tran_date as 'Tran Date',
    a.dataset_code as 'Dataset Code',
    b.dataset_name as 'Dataset Name',
    a.dataset_type as 'Dataset Type',
    a.bal_value as 'Balance'
  from tb_balance as a
  inner join recon_mst_tdataset as b on a.dataset_code = b.dataset_code
  and b.delete_flag = 'N';
end $$

DELIMITER ;