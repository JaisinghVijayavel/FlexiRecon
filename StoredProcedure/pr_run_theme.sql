﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_theme` $$
CREATE PROCEDURE `pr_run_theme`(
  in in_recon_code varchar(32),
  in in_job_gid int,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_job_gid int default 0;
  declare v_job_input_param text default '';
  declare v_date_format text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_txt text default '';
  declare v_sql text default '';

  declare v_recon_date_condition text default '';
  declare v_recon_date_field text default '';
  declare v_recon_date_flag text default '';

  declare v_theme_code text default '';
  declare v_theme_type_code text default '';

  -- get recon details
  select
    recon_date_field,
    recon_date_flag
  into
    v_recon_date_field,
    v_recon_date_flag
  from recon_mst_trecon
  where recon_code = in_recon_code
  and period_from <= curdate()
  and (until_active_flag = 'Y'
  or period_to >= curdate())
  and delete_flag = 'N';

  set v_recon_date_field = ifnull(v_recon_date_field,'N');
  set v_recon_date_flag = ifnull(v_recon_date_flag,'N');

  if v_recon_date_flag = 'Y' then
    set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' >= ');
    set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
    set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' <= ');
    set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
  end if;

  if in_automatch_flag = 'Y' then
    set v_tran_table = 'recon_trn_ttran';
    set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  else
    set v_tran_table = 'recon_tmp_ttran';
    set v_tranbrkp_table = 'recon_tmp_ttranbrkp';
  end if;

  -- set date format
  set v_date_format = fn_get_configvalue('web_date_format');

  set v_job_input_param = concat(v_job_input_param,'Period From : ',date_format(in_period_from,v_date_format),char(13),char(10));
  set v_job_input_param = concat(v_job_input_param,'Period To : ',date_format(in_period_to,v_date_format),char(13),char(10));

  set in_job_gid = ifnull(in_job_gid,0);

  if in_job_gid = 0 then
    call pr_ins_job(in_recon_code,'T',0,'Theming','',in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

    set v_job_gid = @out_job_gid;

    -- blank the theme code
	  set v_sql = 'update $TABLENAME$ set ';
	  set v_sql = concat(v_sql,'theme_code = '''' ');
	  set v_sql = concat(v_sql,'where recon_code = ',char(39),in_recon_code,char(39),' ');
	  set v_sql = concat(v_sql,v_recon_date_condition);
	  set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' ');

	  call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
	  call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
  else
    set v_job_gid = in_job_gid;
  end if;

  if in_automatch_flag = 'Y' then
    -- postprocess
    call pr_run_preprocess(in_recon_code,v_job_gid,'Y',in_period_from,in_period_to,in_automatch_flag,@msg,@result);
  end if;

  -- theme
  theme_block:begin
    declare theme_done int default 0;
    declare theme_cursor cursor for
      select
        theme_code,theme_type_code
      from recon_mst_ttheme
      where recon_code = in_recon_code
      and hold_flag = 'N'
      and active_status = 'Y'
      and delete_flag = 'N'
      order by theme_order;
    declare continue handler for not found set theme_done=1;

    open theme_cursor;

    theme_loop: loop
      fetch theme_cursor into v_theme_code,v_theme_type_code;

      if theme_done = 1 then leave theme_loop; end if;

      set v_theme_code = ifnull(v_theme_code,'');
      set v_theme_type_code = ifnull(v_theme_type_code,'');

      -- set theme
      if v_theme_type_code = 'QCD_THEME_DIRECT' then
        call pr_run_themedirect(in_recon_code,v_theme_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,@msg,@result);
      elseif v_theme_type_code = 'QCD_THEME_COMPARE' then
        call pr_run_theme_comparison(in_recon_code,v_theme_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,@msg,@result);
      elseif v_theme_type_code = 'QCD_THEME_COMPARE_AGG' then
        call pr_run_theme_comparisonagg(in_recon_code,v_theme_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,@msg,@result);
      elseif v_theme_type_code = 'QCD_THEME_QUERY' then
        call pr_run_themequery(in_recon_code,v_theme_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,@msg,@result);
      end if;
    end loop theme_loop;

    close theme_cursor;
  end theme_block;

  call pr_upd_jobwithparam(v_job_gid,v_job_input_param,'C','Completed',@msg,@result);

  set out_result = @result;
  set out_msg = @msg;
end $$

DELIMITER ;