DELIMITER $$

DROP PROCEDURE IF EXISTS pr_set_NGemployeeremove $$
CREATE PROCEDURE pr_set_NGemployeeremove()
begin
	declare v_dataset_db_name text default '';
	declare v_NG_tb_name text default '';
	declare v_NGraw_tb_name text default '';
	declare v_employee_tb_name text default '';
  declare v_sql text default '';

	drop temporary table if exists recon_tmp_tNGpan;

	-- temporary new gen table
  CREATE temporary TABLE recon_tmp_tNGpan
	(
    vendor_code varchar(128) not null,
		pan_no varchar(255),
    PRIMARY KEY (vendor_code)
  ) ENGINE = MyISAM;

	-- get datasetdb name
	set v_dataset_db_name = fn_get_configvalue('dataset_db_name');
	
	-- NG table name
	if v_dataset_db_name = '' then
		set v_NG_tb_name = 'DS183';
	else
		set v_NG_tb_name = concat(v_dataset_db_name,'.DS183');
	end if;

	-- NG raw data table name
	if v_dataset_db_name = '' then
		set v_NGraw_tb_name = 'DS192';
	else
		set v_NGraw_tb_name = concat(v_dataset_db_name,'.DS192');
	end if;
	
	-- employee table name
	if v_dataset_db_name = '' then
		set v_employee_tb_name = 'DS194';
	else
		set v_employee_tb_name = concat(v_dataset_db_name,'.DS194');
	end if;
	
	-- get PAN from raw data new gen DS192
	-- vendor_code = col2, pan_no = col15
	set v_sql = concat("insert into recon_tmp_tNGpan (vendor_code,pan_no) 
		select col2,group_concat(distinct col15) as pan_no from ",v_NGraw_tb_name, "
		where col15 <> '' 
		and delete_flag = 'N' 
		group by col2");
		
  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;
	
	-- update pan no in new gen dataset DS183
	-- pan = col5, vendor_code = col1
	set v_sql = concat("update ",v_NG_tb_name," as a 
		inner join recon_tmp_tNGpan as b on a.col1 = b.vendor_code 
		set a.col5 = b.pan_no  
	where a.delete_flag = 'N'");
		
  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;

	-- join with employee dataset and update the delete_flag = 'E'
	-- Vendor Code (col1) & PAN Match (col5)
	-- employee_id = col1, pan_no = col10
	set v_sql = concat("update ",v_NG_tb_name," as a
		inner join ",v_employee_tb_name," as b on substr(a.col1,2) = b.col1
      and substr(a.col1,1,1) = 'H'
			and a.col5 = b.col10
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
	set v_sql = concat("update ",v_NG_tb_name," as a
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

	drop temporary table if exists recon_tmp_tNGpan;
end $$

DELIMITER ;