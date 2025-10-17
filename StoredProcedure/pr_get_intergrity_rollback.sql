DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_intergrity_rollback` $$
CREATE PROCEDURE `pr_get_intergrity_rollback`
(
  in in_recon_code varchar(32),
  in in_tran_date date,
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
  declare v_base_tran_date date;
  declare v_target_bal_value double(15,2) default 0;
  declare v_target_tran_date date;
  declare v_base_total double(15,2) default 0;
  declare v_target_total double(15,2) default 0;
  declare v_source_dataset varchar(32) default '';
  declare v_target_dataset varchar(32) default '';
  declare v_source_dataset_name varchar(255) default '';
  declare v_target_dataset_name varchar(255) default '';
  declare v_txt text default '';
  declare v_web_date_format text default '';

  declare v_base_roundoff_count int default 0;
  declare v_base_roundoff_value double(15,2) default 0;

  declare v_target_roundoff_count int default 0;
  declare v_target_roundoff_value double(15,2) default 0;

  declare v_threshold_value double(15,2) default 0;

  declare v_roundoff_count double(15,2) default 0;
  declare v_roundoff_total double(15,2) default 0;

  declare v_sql text default '';

  declare v_tran_table text default '';
  declare v_concurrent_ko_flag text default '';

  -- rollback action
  call pr_get_rollbacktran(in_recon_code,in_tran_date,@msg,@result);
  set v_tran_table = 'recon_tmp_ttran';


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

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and recontype_code = 'I'
    and active_status = 'Y'
    and delete_flag = 'N') then

    set out_msg = 'Selected Recon is not Integrity';
    set out_result = 0;

    select * from tb_brs;
    leave me;
  else
    select
      (threshold_plus_value+abs(threshold_minus_value))
    into
      v_threshold_value
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_threshold_value = ifnull(v_threshold_value,0);

    -- get dataset
    select
      group_concat(dataset_code),
      group_concat(dataset_name)
    into
      v_source_dataset,
      v_source_dataset_name
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
      select z.* from
      (
        select
          a.dataset_code,b.dataset_type,a.tran_date,
          a.bal_value
        from recon_trn_taccbal as a
        inner join recon_mst_trecondataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
        where b.recon_code = in_recon_code
        and a.dataset_code = v_dataset_code
        and a.tran_date <= in_tran_date
        and a.delete_flag = 'N'
        order by tran_date desc limit 0,1
        LOCK IN SHARE MODE
      ) as z;
    end loop dataset_loop;

    close dataset_cursor;
  end dataset_block;

  -- Base dqtaset balance
  select
    sum(a.bal_value),max(a.tran_date)
  into
    v_base_bal_value,v_base_tran_date
  from tb_balance as a
  where a.dataset_type = 'B';

  set v_base_bal_value = ifnull(v_base_bal_value,0);
  set v_base_tran_date = ifnull(v_base_tran_date,in_tran_date);

  if v_base_bal_value >= 0 then
    set v_tran_acc_mode = 'CR';
  else
    set v_tran_acc_mode = 'DR';
  end if;

  insert into tb_brs
  (
      particulars,
      tran_value,
      tran_acc_mode,
      bal_value
  )
  values
  (
      concat('Balance as per ',v_source_dataset_name,' (',date_format(v_base_tran_date,v_web_date_format),')'),
      '',
      v_tran_acc_mode,
      format(v_base_bal_value,2,'en_IN')
  );

  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');

  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values (v_source_dataset_name,'','','');


  set v_sql = concat("
  select
    sum(a.excp_value),count(*)
  into
    @v_value,@v_count
  from ",v_tran_table," as a
  inner join tb_dataset as b
    on a.dataset_code = b.dataset_code
    and b.dataset_type = 'B'
  where a.recon_code = '",in_recon_code,"'
  and a.tran_date <= '",cast(in_tran_date as nchar),"'
  and a.excp_value <> 0
  and (a.excp_value - a.roundoff_value) <> 0
  /*
  and (a.tran_value = a.excp_value
  or (a.tran_value <> a.excp_value
  and a.excp_value > ",cast(v_threshold_value as nchar),"))
  */
  and a.tran_acc_mode = 'C'
  and a.delete_flag = 'N'
  LOCK IN SHARE MODE");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_value = ifnull(@v_value,0);
  set v_count = ifnull(@v_count,0);
  set v_base_total = v_base_total + v_value;

  set v_txt = concat('Credit exceptions in ',v_source_dataset_name);

  if v_count > 0 then
    set v_txt = concat(v_txt,' (',cast(v_count as char),')');
  end if;

  insert into tb_brs
  (
    particulars,
    tran_value,
    tran_acc_mode,
    bal_value
  )
  values
  (
    v_txt,
    format(v_value,2,'en_IN'),
    '',
    ''
  );

  set v_sql = concat("
  select
    sum(a.excp_value),count(*)
  into
    @v_value,@v_count
  from ",v_tran_table," as a
  inner join tb_dataset as b
    on a.dataset_code = b.dataset_code
    and b.dataset_type = 'B'
  where a.recon_code = '",in_recon_code,"'
  and a.tran_date <= '",cast(in_tran_date as nchar),"'
  and a.excp_value <> 0
  and (a.excp_value - a.roundoff_value) <> 0
  /*
  and (a.tran_value = a.excp_value
  or (a.tran_value <> a.excp_value
  and a.excp_value > ",cast(v_threshold_value as nchar),"))
  */
  and a.tran_acc_mode = 'D'
  and a.delete_flag = 'N'
  LOCK IN SHARE MODE");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_value = ifnull(@v_value,0);
  set v_count = ifnull(@v_count,0);
  set v_base_total = v_base_total - v_value;

  set v_txt = concat('Debit exceptions in ',v_source_dataset_name);

  if v_count > 0 then
    set v_txt = concat(v_txt,' (',cast(v_count as char),')');
  end if;

  insert into tb_brs
  (
    particulars,
    tran_value,
    tran_acc_mode,
    bal_value
  )
  values
  (
    v_txt,
    format(v_value,2,'en_IN'),
    '',
    ''
  );

    -- base
    set v_sql = concat("
		select
      sum(a.excp_value*a.tran_mult),count(*)
    into
      @v_value,@v_count
    from ",v_tran_table," as a
    inner join tb_dataset as b
      on a.dataset_code = b.dataset_code
      and b.dataset_type = 'B'
		where a.recon_code = '",in_recon_code,"'
    and a.tran_date <= '",cast(in_tran_date as nchar),"'
		and a.excp_value <> 0
    and a.roundoff_value <> 0
		and a.tran_value <> a.excp_value
    and (a.excp_value - a.roundoff_value) = 0
		and a.delete_flag = 'N'
    LOCK IN SHARE MODE");

    call pr_run_sql2(v_sql,@msg2,@result2);

    set v_base_roundoff_count = ifnull(@v_count,0);
    set v_base_roundoff_value = ifnull(@v_value,0);

    -- base rounding off
    if v_base_roundoff_count > 0 then
			set v_txt = concat(v_txt,' (',cast(v_base_roundoff_count as char),')');

			insert into tb_brs
			(
				particulars,
				tran_value,
				tran_acc_mode,
				bal_value
			)
			values
			(
				concat('Roundoff (',cast(v_base_roundoff_count as nchar),')'),
				format(v_base_roundoff_value,2,'en_IN'),
				'',
				''
			);

      set v_base_total = v_base_total + v_base_roundoff_value;
    end if;


  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');


  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('Subtotal','','',format(v_base_total,2,'en_IN'));


  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');


  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values (v_target_dataset_name,'','','');


  set v_sql = concat("
  select
    sum(a.excp_value),count(*)
  into
    @v_value,@v_count
  from ",v_tran_table," as a
  inner join tb_dataset as b
    on a.dataset_code = b.dataset_code
    and b.dataset_type = 'T'
  where a.recon_code = '",in_recon_code,"'
  and a.tran_date <= '",cast(in_tran_date as nchar),"'
  and a.excp_value <> 0
  and (a.excp_value - a.roundoff_value) <> 0
  /*
  and (a.tran_value = a.excp_value
  or (a.tran_value <> a.excp_value
  and a.excp_value > ",cast(v_threshold_value as nchar),"))
  */
  and a.tran_acc_mode = 'C'
  and a.delete_flag = 'N'
  LOCK IN SHARE MODE");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_value = ifnull(@v_value,0);
  set v_count = ifnull(@v_count,0);
  set v_target_total = v_target_total + v_value;

  set v_txt = concat('Credit exceptions in ',v_target_dataset_name);

  if v_count > 0 then
    set v_txt = concat(v_txt,' (',cast(v_count as char),')');
  end if;

  insert into tb_brs
  (
    particulars,
    tran_value,
    tran_acc_mode,
    bal_value
  )
  values
  (
    v_txt,
    format(v_value,2,'en_IN'),
    '',
    ''
  );

  set v_sql = concat("
  select
    sum(a.excp_value),count(*)
  into
    @v_value,@v_count
  from ",v_tran_table," as a
  inner join tb_dataset as b
    on a.dataset_code = b.dataset_code
    and b.dataset_type = 'T'
  where a.recon_code = '",in_recon_code,"'
  and a.tran_date <= '",cast(in_tran_date as nchar),"'
  and a.excp_value <> 0
  and (a.excp_value - a.roundoff_value) <> 0
  /*
  and (a.tran_value = a.excp_value
  or (a.tran_value <> a.excp_value
  and a.excp_value > ",cast(v_threshold_value as nchar),"))
  */
  and a.tran_acc_mode = 'D'
  and a.delete_flag = 'N'
  LOCK IN SHARE MODE");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_value = ifnull(@v_value,0);
  set v_count = ifnull(@v_count,0);
  set v_target_total = v_target_total - v_value;

  set v_txt = concat('Debit exceptions in ',v_target_dataset_name);

  if v_count > 0 then
    set v_txt = concat(v_txt,' (',cast(v_count as char),')');
  end if;


  insert into tb_brs
  (
    particulars,
    tran_value,
    tran_acc_mode,
    bal_value
  )
  values
  (
    v_txt,
    format(v_value,2,'en_IN'),
    '',
    ''
  );

    -- target
    set v_sql = concat("
		select
      sum(a.excp_value*a.tran_mult),count(*)
    into
      @v_value,@v_count
    from ",v_tran_table," as a
    inner join tb_dataset as b
      on a.dataset_code = b.dataset_code
      and b.dataset_type = 'T'
		where a.recon_code = '",in_recon_code,"'
    and a.tran_date <= '",cast(in_tran_date as nchar),"'
		and a.excp_value <> 0
    and a.roundoff_value <> 0
		and a.tran_value <> a.excp_value
    and (a.excp_value - a.roundoff_value) = 0
		and a.delete_flag = 'N'
    LOCK IN SHARE MODE");

    call pr_run_sql2(v_sql,@msg2,@result2);

    set v_target_roundoff_count = ifnull(@v_count,0);
    set v_target_roundoff_value = ifnull(@v_value,0);

    -- target rounding off
    if v_target_roundoff_count > 0 then
			insert into tb_brs
			(
				particulars,
				tran_value,
				tran_acc_mode,
				bal_value
			)
			values
			(
				concat('Roundoff (',cast(v_target_roundoff_count as nchar),')'),
				format(v_target_roundoff_value,2,'en_IN'),
				'',
				''
			);

      set v_target_total = v_target_total + v_target_roundoff_value;
    end if;


  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');


  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('Subtotal','','',format(v_target_total,2,'en_IN'));


  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');

  -- target dataset balance
  select
    sum(a.bal_value) as bal_value,max(a.tran_date) as tran_date
  into
    v_target_bal_value,v_target_tran_date
  from tb_balance as a
  where a.dataset_type = 'T';

  set v_target_bal_value = ifnull(v_target_bal_value,0);
  set v_target_tran_date = ifnull(v_target_tran_date,in_tran_date);

  if v_target_bal_value >= 0 then
    set v_tran_acc_mode = 'CR';
  else
    set v_tran_acc_mode = 'DR';
  end if;

  insert into tb_brs
  (
    particulars,
    tran_value,
    tran_acc_mode,
    bal_value
  )
  values
  (
    concat('Balance as per ',v_target_dataset_name,' (',date_format(v_target_tran_date,v_web_date_format),')'),
    '',
    v_tran_acc_mode,
    format(v_target_bal_value,2,'en_IN')
  );

  set v_value = v_base_bal_value - v_base_total + v_target_total;
  set v_value = round(v_value,2);
  set v_target_bal_value = round(v_target_bal_value,2);

  set v_diff_value = round(v_value-v_target_bal_value,2);

  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('Arrived balance','','',format(v_value,2,'en_IN'));


  if v_diff_value = 0 then
    insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');
  else
    insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('Difference in recon','','',format(v_diff_value,2,'en_IN'));
  end if;

  set out_msg = 'Success';
  set out_result = 1;

  select * from tb_brs;

  drop temporary table if exists tb_dataset;
  drop temporary table if exists tb_brs;
end $$

DELIMITER ;