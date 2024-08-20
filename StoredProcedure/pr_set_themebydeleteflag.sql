DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_themebydeleteflag` $$
CREATE PROCEDURE `pr_set_themebydeleteflag`
(
  in in_recon_code varchar(32),
  in in_theme_desc text,
  in in_delete_flag text
)
begin
  declare v_sql text default '';

  set v_sql = concat("update recon_trn_ttran set
      theme_code = '",in_theme_desc,"',
      delete_flag = 'N'
	  where recon_code = '",in_recon_code,"'
	  and delete_flag = '",in_delete_flag,"'");

  set @sql = v_sql;
  prepare sql_stmt from @sql;
  execute sql_stmt;
  deallocate prepare sql_stmt;
end $$

DELIMITER ;