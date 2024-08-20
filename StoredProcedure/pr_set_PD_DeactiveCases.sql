DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_PD_DeactiveCases` $$
CREATE PROCEDURE `pr_set_PD_DeactiveCases`
(
  in in_recon_code varchar(32),
  in in_include_exclude text
)
begin
  declare v_sql text default '';

	drop temporary table if exists recon_tmp_ttran;

  set in_include_exclude = lower(in_include_exclude);

  -- update theme filter

  if in_include_exclude = 'include' then
    set v_sql = concat("update recon_trn_ttranbrkp
      set delete_flag = 'N'
      where recon_code = '",in_recon_code,"'
      and tran_gid = 0
      and col11 = 'DEACTIVE'
      and delete_flag = 'V'");

    set @sql = v_sql;
    prepare sql_stmt1 from @sql;
    execute sql_stmt1;
    deallocate prepare sql_stmt1;
  end if;

  if in_include_exclude = 'exclude' then
    set v_sql = concat("update recon_trn_ttranbrkp
      set delete_flag = 'V'
      where recon_code = '",in_recon_code,"'
      and tran_gid = 0
      and col11 = 'DEACTIVE'
      and delete_flag = 'N'");

    set @sql = v_sql;
    prepare sql_stmt1 from @sql;
    execute sql_stmt1;
    deallocate prepare sql_stmt1;
  end if;
end $$

DELIMITER ;