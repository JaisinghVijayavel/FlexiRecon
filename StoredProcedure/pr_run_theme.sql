DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_theme` $$
CREATE PROCEDURE `pr_run_theme`(
  in in_recon_code varchar(32),
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_theme_code text default '';

  declare v_recon_date_flag text default '';
  declare v_recon_date_field text default '';
  declare v_recon_date_condition text default '';

  declare v_recon_field text default '';

  declare v_theme_filter text default '';

  declare v_filter_criteria text default '';
  declare v_filter_value text default '';

  declare v_open_parentheses_flag text default '';
  declare v_close_parentheses_flag text default '';
  declare v_join_condition text default '';

  declare v_build_condition text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_sql text default '';
  declare v_tran_sql text default '';
  declare v_tranbrkp_sql text default '';

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
  else
    select
      recon_date_flag,
      recon_date_field
    into
      v_recon_date_flag,
      v_recon_date_field
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_recon_date_flag = ifnull(v_recon_date_flag,'');
    set v_recon_date_field = ifnull(v_recon_date_field,'');

    if v_recon_date_flag = 'Y' then
      set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' >= ');
      set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
      set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' <= ');
      set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
    end if;
  end if;

  if in_automatch_flag = 'Y' then
    set v_tran_table = 'recon_trn_ttran';
    set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  else
    set v_tran_table = 'recon_tmp_ttran';
    set v_tranbrkp_table = 'recon_tmp_ttranbrkp';

    leave me;
  end if;

  -- blank the theme code
	set v_sql = 'update $TABLENAME$ set ';
	set v_sql = concat(v_sql,'theme_code = null ');
	set v_sql = concat(v_sql,'where recon_code = ',char(39),in_recon_code,char(39),' ');
	set v_sql = concat(v_sql,v_recon_date_condition);
	set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' ');

	call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
	call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);

  -- process
  theme_block:begin
    declare theme_done int default 0;
    declare theme_cursor cursor for
      select
        theme_code
      from recon_mst_ttheme
      where recon_code = in_recon_code
      and active_status = 'Y'
      and delete_flag = 'N'
      order by theme_order;
    declare continue handler for not found set theme_done=1;

    open theme_cursor;

    theme_loop: loop
      fetch theme_cursor into v_theme_code;

      if theme_done = 1 then leave theme_loop; end if;

      set v_theme_code = ifnull(v_theme_code,'');

      -- filter condition
      if v_theme_code <> '' then
        set v_theme_filter = ' and ';

				-- filter block
				filter_block:begin
					declare filter_done int default 0;

					declare filter_cursor cursor for
					  select
              recon_field,
              filter_criteria,
              filter_value,
              open_parentheses_flag,
              close_parentheses_flag,
              join_condition
            from recon_mst_tthemefilter
            where theme_code = v_theme_code
            and active_status = 'Y'
            and delete_flag = 'N'
            order by theme_seqno;

					declare continue handler for not found set filter_done=1;

					open filter_cursor;

					filter_loop: loop
						fetch filter_cursor into
              v_recon_field,
              v_filter_criteria,
              v_filter_value,
              v_open_parentheses_flag,
              v_close_parentheses_flag,
              v_join_condition;

						if filter_done = 1 then leave filter_loop; end if;

            set v_recon_field = ifnull(v_recon_field,'');
            set v_filter_criteria = ifnull(v_filter_criteria,'');
            set v_filter_value = ifnull(v_filter_value,'');

            set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
            set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');

            set v_join_condition = ifnull(v_join_condition,'');

            if v_join_condition = '' then
              set v_join_condition = 'and';
            end if;

            set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
            set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

            set v_theme_filter = concat(v_theme_filter,
                                             v_open_parentheses_flag,
                                             fn_get_basefilterformat(v_recon_field,'EXACT',0,v_filter_criteria,v_filter_value),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition,' ');

					end loop filter_loop;

					close filter_cursor;

				end filter_block;

        if v_theme_filter = ' and ' then
          set v_theme_filter = '';
        else
          set v_theme_filter = concat(v_theme_filter,' 1 = 1 ');
        end if;

        set v_sql = 'update $TABLENAME$ set ';
        set v_sql = concat(v_sql,'theme_code = concat(ifnull(theme_code,',char(39),'N',char(39),'),');
        set v_sql = concat(v_sql,char(39),',',char(39),',');
        set v_sql = concat(v_sql,char(39),v_theme_code,char(39),') ');
        set v_sql = concat(v_sql,'where recon_code = ',char(39),in_recon_code,char(39),' ');
        set v_sql = concat(v_sql,v_recon_date_condition);
        set v_sql = concat(v_sql, v_theme_filter);
        set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' ');

        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
      end if;
    end loop theme_loop;

    close theme_cursor;
  end theme_block;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;