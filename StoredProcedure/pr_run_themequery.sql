DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_themequery` $$
CREATE PROCEDURE `pr_run_themequery`
(
  in in_recon_code varchar(32),
  in in_theme_code varchar(32),
  in in_job_gid int,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_theme_code text default '';
  declare v_theme_desc text default '';
  declare v_theme_query text default '';

  declare i int default 0;

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  -- recon validation
  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and active_status = 'Y'
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and delete_flag = 'N') then

    set out_msg = 'Invalid recon !';
    set out_result = 0;

    leave me;
  end if;

  -- process
  theme_block:begin
    declare theme_done int default 0;
    declare theme_cursor cursor for
      select
        theme_code,theme_desc,theme_query
      from recon_mst_ttheme
      where recon_code = in_recon_code
      and theme_code = in_theme_code
      and theme_type_code = 'QCD_THEME_QUERY'
      and hold_flag = 'N'
      and active_status = 'Y'
      and delete_flag = 'N'
      order by theme_order;
    declare continue handler for not found set theme_done=1;

    open theme_cursor;

    theme_loop: loop
      fetch theme_cursor into v_theme_code,v_theme_desc;

      if theme_done = 1 then leave theme_loop; end if;

      set v_theme_code = ifnull(v_theme_code,'');
      set v_theme_desc = ifnull(v_theme_desc,'');
      set v_theme_query = ifnull(v_theme_query,'');

      call pr_upd_job(in_job_gid,'P',concat('Applying Theme - ',v_theme_desc),@msg,@result);

      call pr_run_sql1(v_theme_query,@msg,@result);
    end loop theme_loop;

    close theme_cursor;
  end theme_block;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;