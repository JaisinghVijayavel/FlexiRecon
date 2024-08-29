DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_OFemployeeremove $$
CREATE PROCEDURE pr_set_OFemployeeremove()
begin
	declare v_dataset_db_name text default '';
	declare v_OF_tb_name text default '';
	declare v_OFraw_tb_name text default '';
	declare v_employee_tb_name text default '';
  declare v_sql text default '';

	drop temporary table if exists recon_tmp_tOFpan;

	-- temporary oracle fusion table
  CREATE temporary TABLE recon_tmp_tOFpan
	(
    vendor_code varchar(128) not null,
		pan_no varchar(255),
    PRIMARY KEY (vendor_code)
  ) ENGINE = MyISAM;
	
	-- get datasetdb name
	set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  if v_dataset_db_name = '' then
    select database() into v_dataset_db_name;
  end if;

	-- OF table name
	if v_dataset_db_name = '' then
		set v_OF_tb_name = 'DS180';
	else
		set v_OF_tb_name = concat(v_dataset_db_name,'.DS180');
	end if;

	-- OF raw data table name
	if v_dataset_db_name = '' then
		set v_OFraw_tb_name = 'DS189';
	else
		set v_OFraw_tb_name = concat(v_dataset_db_name,'.DS189');
	end if;
	
	-- employee table name
	if v_dataset_db_name = '' then
		set v_employee_tb_name = 'DS194';
	else
		set v_employee_tb_name = concat(v_dataset_db_name,'.DS194');
	end if;
	
	-- get PAN from raw data oracle fusion DS189
	-- vendor_code = col1, pan_no = col22
	set v_sql = concat("insert into recon_tmp_tOFpan (vendor_code,pan_no) 
		select col1,group_concat(distinct col22) as pan_no from ",v_OFraw_tb_name, "
		where col22 <> ''
		and delete_flag = 'N'
		group by col1");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;

	-- update pan no in oracle fusion dataset DS180
	-- pan = col8, vendor_code = col1
	set v_sql = concat("update ",v_OF_tb_name," as a
		inner join recon_tmp_tOFpan as b on a.col1 = b.vendor_code
		set a.col8 = b.pan_no
	where a.delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;

	-- join with employee dataset and update the delete_flag = 'E'
	-- Vendor Code & PAN Match
	-- employee_id = col1, pan_no = col10
	set v_sql = concat("update ",v_OF_tb_name," as a
		inner join ",v_employee_tb_name," as b on substr(a.col1,2) = b.col1
      and substr(a.col1,1,1) = 'H'
			and a.col8 = b.col10
			and b.delete_flag = 'N'
		set a.delete_flag = 'E'
	where a.delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;

	-- join with employee dataset and update the delete_flag = 'E'
	-- Vendor Code (col1) & Revised Vendor Name (col3) Match
	-- employee_id = col1, revised_employee_name = col11
	set v_sql = concat("update ",v_OF_tb_name," as a
		inner join ",v_employee_tb_name," as b on substr(a.col1,2) = b.col1
      and substr(a.col1,1,1) = 'H'
			and a.col3 = b.col11
			and b.delete_flag = 'N'
		set a.delete_flag = 'F'
	where a.delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;

	drop temporary table if exists recon_tmp_tOFpan;

  -- Nil Transactions
  -- update transaction count
  set v_sql = concat("update ",v_OF_tb_name," as a
    inner join ",v_dataset_db_name,".DS256 as b on b.col1 = a.col1 and b.col2 = a.col2
    set a.col31 = b.col11
    where a.delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;

  set v_sql = concat("update ",v_OF_tb_name," set delete_flag = 'G'
    where (col31 = '0' or col31 = '' or col31 is null)
    and delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;
end $$

DELIMITER ;