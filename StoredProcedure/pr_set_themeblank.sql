DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_themeblank` $$
CREATE PROCEDURE `pr_set_themeblank`
(
  in in_recon_code varchar(32),
  in in_blank_status text
)
begin
  declare v_sql text default '';

	drop temporary table if exists recon_tmp_ttran;

  set in_blank_status = lower(in_blank_status);

  -- update theme filter

  if in_blank_status = 'invoke' then
    set v_sql = concat("update recon_mst_tthemefilter
      set delete_flag = 'N'
      where theme_code in
      (
        select theme_code from recon_mst_ttheme
        where recon_code = '",in_recon_code ,"'
        and active_status = 'Y'
        and delete_flag = 'N'
      )
      and filter_field = 'theme_code'
      and (filter_criteria = '='
      or filter_criteria = 'EXACT')
      and filter_value = ''
      and active_status = 'Y'
      and delete_flag = 'I'");

    set @sql = v_sql;
    prepare sql_stmt from @sql;
    execute sql_stmt;
    deallocate prepare sql_stmt;
  end if;

  if in_blank_status = 'revoke' then
    set v_sql = concat("update recon_mst_tthemefilter
      set delete_flag = 'I'
      where theme_code in
      (
        select theme_code from recon_mst_ttheme
        where recon_code = '",in_recon_code ,"'
        and active_status = 'Y'
        and delete_flag = 'N'
      )
      and filter_field = 'theme_code'
      and (filter_criteria = '='
      or filter_criteria = 'EXACT')
      and filter_value = ''
      and active_status = 'Y'
      and delete_flag = 'N'");

    set @sql = v_sql;
    prepare sql_stmt from @sql;
    execute sql_stmt;
    deallocate prepare sql_stmt;
  end if;
end $$

DELIMITER ;