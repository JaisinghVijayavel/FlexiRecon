﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_proofbal` $$
CREATE PROCEDURE `pr_get_proofbal`
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
  declare v_bal_value1 double(15,2) default 0;
  declare v_bal_tran_date1 date;
  declare v_bal_value2 double(15,2) default 0;
  declare v_bal_tran_date2 date;
  declare v_cr_total double(15,2) default 0;
  declare v_dr_total double(15,2) default 0;
  declare v_source_dataset varchar(32) default '';
  declare v_comparison_dataset varchar(32) default '';
  declare v_source_dataset_name varchar(255) default '';
  declare v_comparison_dataset_name varchar(255) default '';
  declare v_txt text default '';
  declare v_web_date_format text default '';
  declare v_threshold_value double(15,2) default 0;
  declare v_threshold_total double(15,2) default 0;
  declare v_roundoff_value double(15,2) default 0;

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
    dataset_name text default null,
    dataset_type varchar(32) default null,
    tran_date date default null,
    bal_value text default null,
    PRIMARY KEY (bal_gid)
  );

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and recontype_code = 'W'
    and active_status = 'Y'
    and delete_flag = 'N') then

    set out_msg = 'Selected Recon is not Proof';
    set out_result = 0;

    select * from tb_brs;
    leave me;
  end if;

  dataset_block:begin
    declare dataset_done int default 0;
    declare dataset_cursor cursor for
      select
        a.dataset_code
      from recon_mst_trecondataset as a
      where a.recon_code = in_recon_code
      and a.dataset_type = 'B'
      and a.active_status = 'Y'
      and a.delete_flag = 'N';
    declare continue handler for not found set dataset_done=1;

    open dataset_cursor;

    dataset_loop: loop
      fetch dataset_cursor into v_dataset_code;

      if dataset_done = 1 then leave dataset_loop; end if;

      insert into tb_balance(dataset_code,dataset_name,dataset_type,tran_date,bal_value)
        select
          a.dataset_code,c.dataset_name,b.dataset_type,a.tran_date,
          a.bal_value
        from recon_trn_taccbal as a
        inner join recon_mst_trecondataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
        left join recon_mst_tdataset as c on b.dataset_code = c.dataset_code and c.delete_flag = 'N'
        where b.recon_code = in_recon_code
        and a.dataset_code = v_dataset_code
        and a.tran_date <= in_tran_date
        and a.delete_flag = 'N' order by tran_date desc limit 0,1;
    end loop dataset_loop;

    close dataset_cursor;
  end dataset_block;

  -- Base dqtaset balance
  select
    sum(a.bal_value),max(a.tran_date)
  into
    v_bal_value1,v_bal_tran_date1
  from tb_balance as a
  where a.dataset_type = 'B';

  set v_bal_value1 = ifnull(v_bal_value1,0);
  set v_bal_tran_date1 = ifnull(v_bal_tran_date1,in_tran_date);

  if v_bal_value1 >= 0 then
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
      concat('Balance as per ',v_source_dataset_name,' (',date_format(v_bal_tran_date1,v_web_date_format),')'),
      '',
      v_tran_acc_mode,
      format(v_bal_value1,2,'en_IN')
  );

  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');

  insert into tb_brs (particulars,tran_value,tran_acc_mode,bal_value) values ('Add','','','');

  select
    sum(a.excp_value*a.tran_mult),count(*)
  into
    v_value,v_count
  from recon_trn_ttran as a
  inner join tb_dataset as b
    on a.dataset_code = b.dataset_code
    and b.dataset_type = 'B'
  where a.recon_code = in_recon_code
  and a.tran_date <= in_tran_date
  and a.excp_value <> 0
  and a.delete_flag = 'N';

  set v_value = ifnull(v_value,0);
  set v_count = ifnull(v_count,0);

  -- delete user records from temp table
  delete from recon_tmp_tproofbal
  where user_code = in_user_code;

  -- calculated balance
  insert into recon_tmp_tproofbal
  select in_recon_code,in_user_code,
         v_value as arrived_closingbal,
         v_count as arrived_closingbal_count,
         v_bal_value1 as acc_closingbal,
         v_bal_tran_date1 as acc_closingbal_date,
         v_value - v_bal_value1 as diff_bal;

  drop temporary table if exists tb_dataset;
  drop temporary table if exists tb_brs;
end $$

DELIMITER ;