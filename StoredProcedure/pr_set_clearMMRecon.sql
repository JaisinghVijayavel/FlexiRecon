DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_clearMMRecon` $$
CREATE PROCEDURE `pr_set_clearMMRecon`()
begin
	declare v_dataset_db_name text default '';
  declare v_sql text default '';

  update recon_trn_ttran set
    col4 = null,
    col5 = null,
    col6 = null,
    col7 = null,
    col8 = null,
    col9 = null,
    col10 = null,
    col11 = null,
    col12 = null,
    col13 = null,
    col14 = null,
    col15 = null,
    col16 = null,
    col17 = null,
    col18 = null,
    col19 = null,
    col20 = null
  where recon_code = 'RE147'
  and delete_flag = 'N';

	-- get datasetdb name
	set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  if v_dataset_db_name = '' then
    select database() into v_dataset_db_name;
  end if;

  set v_sql = concat("update ",v_dataset_db_name,".DS180 set delete_flag = 'N'
    where delete_flag <> 'Y'");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;
end $$

DELIMITER ;