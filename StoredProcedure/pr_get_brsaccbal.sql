DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_brsaccbal` $$
CREATE PROCEDURE `pr_get_brsaccbal`
(
  in in_recon_code varchar(32),
  in in_tran_date date,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    Updated Date : 15-07-2026

    Version : 2
  */

  declare v_dataset_code text default '';
  declare v_dataset_category text default '';

  set in_tran_date = ifnull(in_tran_date,curdate());

  drop temporary table if exists tb_balance;

  create temporary table if not exists tb_balance
  (
    bal_gid int unsigned not null auto_increment,
    dataset_code varchar(32) default null,
    dataset_type varchar(32) default null,
    dataset_category varchar(32) default null,
    tran_date date default null,
    bal_value text default null,
    PRIMARY KEY (bal_gid)
  );

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and recontype_code = 'B'
    and active_status = 'Y'
    and delete_flag = 'N') then

    set out_msg = 'Selected Recon is not BRS';
    set out_result = 0;

    select * from tb_balance;
    leave me;
  end if;

  -- Dataset Block
  dataset_block:begin
    declare dataset_done int default 0;
    declare dataset_cursor cursor for
			select
				a.dataset_code,b.dataset_category
			from recon_mst_trecondataset as a
			inner join recon_mst_tdataset as b on a.dataset_code = b.dataset_code
				and b.active_status = 'Y'
				and b.dataset_category in ('BankStatement','Ledger')
				and b.delete_flag = 'N'
			where a.recon_code = in_recon_code
			and a.dataset_type <> 'S'
			and a.active_status = 'Y'
			and a.delete_flag = 'N'
			order by b.dataset_category;
    declare continue handler for not found set dataset_done=1;

    open dataset_cursor;

    dataset_loop: loop
      fetch dataset_cursor into v_dataset_code,v_dataset_category;

      if dataset_done = 1 then leave dataset_loop; end if;

			insert into tb_balance(dataset_code,dataset_type,dataset_category,tran_date,bal_value)
			select
				a.dataset_code,
				b.dataset_type,
				v_dataset_category as dataset_category,
				a.tran_date,
				a.bal_value
			from recon_trn_taccbal as a
			inner join recon_mst_trecondataset as b on a.dataset_code = b.dataset_code
				and b.delete_flag = 'N'
			where b.recon_code = in_recon_code
			and a.dataset_code = v_dataset_code
			and a.tran_date <= in_tran_date
			and a.delete_flag = 'N'
			order by tran_date desc limit 0,1;
    end loop dataset_loop;

    close dataset_cursor;
  end dataset_block;


  -- Bank Statement
	if not exists(select * from tb_balance where dataset_category = 'Ledger') then
		insert into tb_balance(dataset_code,dataset_type,dataset_category,tran_date,bal_value)
		select '' as dataset_code,
					 '' as dataset_type,
					 'BankStatement' as dataset_category,
					 curdate() as tran_date,
					 0 as bal_value;
	end if;

	-- Ledger
	if not exists(select * from tb_balance where dataset_category = 'Ledger') then
		insert into tb_balance(dataset_code,dataset_type,dataset_category,tran_date,bal_value)
		select '' as dataset_code,
					 '' as dataset_type,
					 'Ledger' as dataset_category,
					 curdate() as tran_date,
					 0 as bal_value;
	end if;

  -- return Balance
  select
    a.tran_date as 'Tran Date',
    a.dataset_code as 'Dataset Code',
    b.dataset_name as 'Dataset Name',
    a.dataset_type as 'Dataset Type',
    a.dataset_category as 'Dataset Category',
    a.bal_value as 'Balance'
  from tb_balance as a
  left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code
    and b.delete_flag = 'N';
end $$

DELIMITER ;