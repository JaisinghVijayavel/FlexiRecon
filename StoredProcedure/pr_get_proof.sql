DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_proof` $$
CREATE PROCEDURE `pr_get_proof`
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
  declare v_bal_value double(15,2) default 0;
  declare v_bal_tran_date date;
  declare v_cr_total double(15,2) default 0;
  declare v_dr_total double(15,2) default 0;
  declare v_dataset_code varchar(32) default '';
  declare v_dataset_name varchar(255) default '';
  declare v_dataset_type varchar(32) default '';
  declare v_tran_acc_mode varchar(32) default '';
  declare v_txt text default '';
  declare v_web_date_format text default '';

  set v_web_date_format = fn_get_configvalue('web_date_format');

  set v_web_date_format = ifnull(v_web_date_format,'%d-%m-%Y');

  drop temporary table if exists tb_proof;
  create temporary table if not exists tb_proof
  (
    proof_gid int unsigned not null auto_increment,
    particulars text default null,
    tran_value text default null,
    tran_acc_mode varchar(8) default null,
    bal_value text default null,
    PRIMARY KEY (proof_gid)
  ) ENGINE = MyISAM;

  drop temporary table if exists tb_dataset;
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
  ) ENGINE = MyISAM;

  dataset_block:begin
    declare dataset_done int default 0;
    declare dataset_cursor cursor for
      select
        a.dataset_code,
        a.dataset_type
      from recon_mst_trecondataset as a
      where a.recon_code = in_recon_code
      and a.dataset_type <> 'S'
      and a.active_status = 'Y'
      and a.delete_flag = 'N';
    declare continue handler for not found set dataset_done=1;

    open dataset_cursor;

    dataset_loop: loop
      fetch dataset_cursor into v_dataset_code,v_dataset_type;

      if dataset_done = 1 then leave dataset_loop; end if;

      insert into tb_balance(dataset_code,dataset_type,tran_date,bal_value)
        select a.dataset_code,v_dataset_type,a.tran_date,a.bal_value from recon_trn_taccbal as a
        where a.dataset_code = v_dataset_code
        and a.tran_date <= in_tran_date
        and a.delete_flag = 'N'
        order by a.tran_date desc limit 0,1;
    end loop dataset_loop;

    close dataset_cursor;
  end dataset_block;

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and active_status = 'Y'
    and recontype_code = 'W'
    and delete_flag = 'N') then

    set out_msg = 'Selected Recon is not Proof';
    set out_result = 0;

    select * from tb_proof;
    leave me;
  else
    select
      group_concat(a.dataset_code),
      group_concat(b.dataset_name)
    into
      v_dataset_code,
      v_dataset_name
    from recon_mst_trecondataset as a
    inner join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
    where a.recon_code = in_recon_code
    and a.delete_flag = 'N';
  end if;

  if exists(select * from tb_balance) then
    select
      sum(bal_value),max(tran_date)
    into
      v_bal_value,v_bal_tran_date
    from tb_balance;

    set v_bal_value = ifnull(v_bal_value,0);
    set v_bal_value = round(v_bal_value,2);

    if v_bal_value >= 0 then
      set v_tran_acc_mode = 'CR';
    else
      set v_tran_acc_mode = 'DR';
    end if;

    insert into tb_proof
    (
      particulars,
      tran_value,
      tran_acc_mode,
      bal_value
    )
    values
    (
      concat('Balance on ',date_format(v_bal_tran_date,v_web_date_format)),
      '',
      v_tran_acc_mode,
      replace(format(v_bal_value,2),',','')
    );
  else
    insert into tb_proof
    (
      particulars,
      tran_value,
      tran_acc_mode,
      bal_value
    )
    values
    (
      concat('Balance as per ',v_dataset_name),
      '',
      'CR',
      '0.00'
    );
  end if;

  insert into tb_proof (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');

  insert into tb_proof (particulars,tran_value,tran_acc_mode,bal_value) values ('Exception','','','');

  select sum(excp_value),count(*) into v_value,v_count from recon_trn_ttran
  where recon_code = in_recon_code
  and excp_value > 0
  and tran_acc_mode = 'D'
  and tran_date <= in_tran_date
  and delete_flag = 'N';

  set v_value = ifnull(v_value,0);
  set v_count = ifnull(v_count,0);
  set v_dr_total = v_dr_total + v_value;

  set v_txt = concat('Debit Total');

  if v_count > 0 then
    set v_txt = concat(v_txt,' ',cast(v_count as nchar));
  end if;

  insert into tb_proof
  (
    particulars,
    tran_value,
    tran_acc_mode,
    bal_value
  )
  values
  (
    v_txt,
    replace(format(v_value,2),',',''),
    '',
    ''
  );

  select sum(excp_value),count(*) into v_value,v_count from recon_trn_ttran
  where recon_code = in_recon_code
  and excp_value > 0
  and tran_acc_mode = 'C'
  and tran_date <= in_tran_date
  and delete_flag = 'N';

  set v_value = ifnull(v_value,0);
  set v_count = ifnull(v_count,0);
  set v_cr_total = v_cr_total + v_value;

  set v_txt = concat('Credit Total');

  if v_count > 0 then
    set v_txt = concat(v_txt,' ',cast(v_count as nchar));
  end if;

  insert into tb_proof
  (
    particulars,
    tran_value,
    tran_acc_mode,
    bal_value
  )
  values
  (
    v_txt,
    replace(format(v_value,2),',',''),
    '',
    ''
  );

  insert into tb_proof (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');

  set v_value = v_cr_total - v_dr_total;
  set v_value = round(v_value,2);

  if v_value >= 0 then
    set v_tran_acc_mode = 'CR';
  else
    set v_tran_acc_mode = 'DR';
  end if;

  insert into tb_proof (particulars,tran_value,tran_acc_mode,bal_value) values ('Net Exception','',v_tran_acc_mode,replace(format(v_value,2),',',''));

  insert into tb_proof (particulars,tran_value,tran_acc_mode,bal_value) values ('','','','');

  set v_diff_value = round(v_value-v_bal_value,2);

  if v_diff_value = 0 then
    insert into tb_proof (particulars,tran_value,tran_acc_mode,bal_value) values ('Difference','','','0.00');
  else
    insert into tb_proof (particulars,tran_value,tran_acc_mode,bal_value) values ('Difference','','',replace(format(v_diff_value,2),',',''));
  end if;

  set out_msg = 'Success';
  set out_result = 1;

  select * from tb_proof;

  drop temporary table if exists tb_proof;
end $$

DELIMITER ;