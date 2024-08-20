DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_employeeremove` $$
CREATE PROCEDURE `pr_set_employeeremove`
(
  in in_recon_code varchar(32),
  in in_remove_condition text,
  in in_delete_flag text
)
begin
  declare v_sql text default '';

	drop temporary table if exists recon_tmp_ttran;

  CREATE temporary TABLE recon_tmp_ttran(
    tran_gid int unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  set in_remove_condition = ifnull(in_remove_condition,'');

  if in_remove_condition = '' then
    set in_remove_condition = " 1 = 2 ";
  end if;

  -- get employee record tran_gid
  set v_sql = concat("insert into recon_tmp_ttran (tran_gid)
	  select tran_gid from recon_trn_ttran
	  where recon_code = '",in_recon_code,"'
    and ", in_remove_condition ,"
	  and delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt1 from @sql;
  execute sql_stmt1;
  deallocate prepare sql_stmt1;

  -- update employee record delete_flag = 'E'
  set v_sql = concat("update recon_trn_ttran set delete_flag = '",in_delete_flag,"'
	  where recon_code = '",in_recon_code,"'
	  and tran_gid in (select tran_gid
	  from recon_tmp_ttran)
	  and delete_flag = 'N'");

  set @sql = v_sql;
  prepare sql_stmt2 from @sql;
  execute sql_stmt2;
  deallocate prepare sql_stmt2;

	drop temporary table recon_tmp_ttran;
end $$

DELIMITER ;