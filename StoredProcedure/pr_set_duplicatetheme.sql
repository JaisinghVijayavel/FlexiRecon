DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_duplicatetheme` $$
CREATE PROCEDURE `pr_set_duplicatetheme`
(
  in in_recon_code varchar(32),
  in in_duplicate_col text
)
begin
  declare v_sql text default '';

	drop temporary table if exists recon_tmp_ttran;

  -- get duplicate record
  set v_sql = concat("create temporary table recon_tmp_ttran
	  select ",in_duplicate_col," from recon_trn_ttran
	  where recon_code = '",in_recon_code,"'
	  and delete_flag = 'N'
	  group by ",in_duplicate_col,"
	  having count(*) > 1");

  set @sql = v_sql;
  prepare sql_stmt from @sql;
  execute sql_stmt;
  deallocate prepare sql_stmt;

  -- update theme
  set v_sql = concat("update recon_trn_ttran set theme_code = 'Duplicate vendor code'
	  where recon_code = '",in_recon_code,"'
	  and (",in_duplicate_col,") in (select ",in_duplicate_col,"
	from recon_tmp_ttran)
	and delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt from @sql;
  execute sql_stmt;
  deallocate prepare sql_stmt;

	drop temporary table recon_tmp_ttran;
end $$

DELIMITER ;