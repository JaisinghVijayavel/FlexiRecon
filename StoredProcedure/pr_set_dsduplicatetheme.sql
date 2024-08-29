DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_dsduplicatetheme` $$
CREATE PROCEDURE `pr_set_dsduplicatetheme`(
  in in_dataset_code varchar(64),
  in in_duplicate_col text
)
begin
  declare v_sql text default '';

	drop temporary table if exists recon_tmp_ttran;

  -- get duplicate record
  set v_sql = concat("create temporary table recon_tmp_ttran
	  select ",in_duplicate_col," from recon_flexi_dataset_poc.",in_dataset_code,
	  " where delete_flag = 'N'
	  group by ",in_duplicate_col,"
	  having count(*) > 1");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;

  # Updating delete_flag = 'D' in dataset table
  set v_sql = concat("update recon_flexi_dataset_poc.",in_dataset_code," set delete_flag = 'D'
	  where (",in_duplicate_col,") in (select ",in_duplicate_col,"
	  from recon_tmp_ttran)
	  and delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt2 from @sql;
  execute sql_stmt2;
  deallocate prepare sql_stmt2;

	drop temporary table recon_tmp_ttran;
end $$

DELIMITER ;