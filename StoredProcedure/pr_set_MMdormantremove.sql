DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_MMdormantremove $$
CREATE PROCEDURE pr_set_MMdormantremove()
begin
	declare v_dataset_db_name text default '';
	declare v_MM_tb_name text default '';
	declare v_MMraw_tb_name text default '';
  declare v_sql text default '';

	-- get datasetdb name
	set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  if v_dataset_db_name = '' then
    select database() into v_dataset_db_name;
  end if;

	-- OF table name
	if v_dataset_db_name = '' then
		set v_MM_tb_name = 'DS182';
	else
		set v_MM_tb_name = concat(v_dataset_db_name,'.DS182');
	end if;

  -- Nil Transactions
  -- update transaction count
  set v_sql = concat("update ",v_MM_tb_name," as a
    inner join ",v_dataset_db_name,".DS270 as b on b.col1 = a.col1
    set a.col31 = b.col4
    where a.delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;

  set v_sql = concat("update ",v_MM_tb_name," set delete_flag = 'G'
    where col31 = '0'
    and delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;
end $$

DELIMITER ;