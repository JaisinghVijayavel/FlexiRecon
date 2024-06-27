DELIMITER $$

DROP function IF EXISTS `fn_get_chkbalance` $$
CREATE function `fn_get_chkbalance`
(
  in_recon_code varchar(32),
  in_tran_date date
) returns boolean
begin
  declare v_dataset_code text default '';
  declare v_bal_value double(15,2) default 0;
  declare v_value double(15,2) default 0;

  -- for testing purpose return true - 21-06-2024/Vijayavel
  return true;

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
    and recontype_code in ('B','W')
    and active_status = 'Y'
    and delete_flag = 'N') then

    return false;
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

  -- balance table value
  select
    sum(bal_value)
  into
    v_bal_value
  from tb_balance;

  -- total exception value
  select
    sum(excp_value*tran_mult)
  into
    v_value
  from recon_trn_ttran
  where recon_code = in_recon_code
  and tran_date <= in_tran_date
  and excp_value <> 0
  and delete_flag = 'N';

  set v_bal_value = ifnull(v_bal_value,0);
  set v_value = ifnull(v_value,0);

  drop temporary table if exists tb_balance;

  if v_bal_value = v_value then
    return true;
  else
    return false;
  end if;
end $$

DELIMITER ;