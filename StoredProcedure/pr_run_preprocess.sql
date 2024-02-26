DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_preprocess` $$
CREATE PROCEDURE `pr_run_preprocess`(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_update_recon_field text default '';
  declare v_preprocess_code text default '';
  declare v_process_method text default '';
  declare v_process_query text default '';

  declare v_dataset_db_name text default '';

  declare v_process_function text default '';
  declare v_lookup_dataset_code text default '';
  declare v_lookup_return_field text default '';

  declare v_recon_date_flag text default '';
  declare v_recon_date_field text default '';
  declare v_recon_date_condition text default '';

  declare v_recon_field text default '';
  declare v_lookup_field text default '';

  declare v_filter_field text default '';
  declare v_filter_criteria text default '';
  declare v_filter_value text default '';

  declare v_extraction_criteria text default '';
  declare v_extraction_filter int default '';
  declare v_extraction_value text default '';

  declare v_comparison_criteria text default '';
  declare v_comparison_filter int default '';
  declare v_comparison_value text default '';

  declare v_open_parentheses_flag text default '';
  declare v_close_parentheses_flag text default '';
  declare v_join_condition text default '';

  declare v_recon_field_format text default '';
  declare v_build_condition text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_preprocess_filter text default '';
  declare v_lookup_condition text default '';

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

  -- get dataset db name
  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  if in_automatch_flag = 'Y' then
    set v_tran_table = 'recon_trn_ttran';
    set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  else
    set v_tran_table = 'recon_tmp_ttran';
    set v_tranbrkp_table = 'recon_tmp_ttranbrkp';
  end if;

  -- process
  process_block:begin
    declare process_done int default 0;
    declare process_cursor cursor for
      select
        preprocess_code,
        update_recon_field,
        process_method,
        process_query,
        process_function,
        lookup_dataset_code,
        lookup_return_field
      from recon_mst_tpreprocess
      where recon_code = in_recon_code
      and active_status = 'Y'
      and delete_flag = 'N'
      order by preprocess_order;
    declare continue handler for not found set process_done=1;

    open process_cursor;

    process_loop: loop
      fetch process_cursor into
        v_preprocess_code,
        v_update_recon_field,
        v_process_method,
        v_process_query,
        v_process_function,
        v_lookup_dataset_code,
        v_lookup_return_field;

      if process_done = 1 then leave process_loop; end if;

      set v_preprocess_code = ifnull(v_preprocess_code,'');
      set v_update_recon_field = ifnull(v_update_recon_field,'');
      set v_process_method = ifnull(v_process_method,'');
      set v_process_query = ifnull(v_process_query,'');
      set v_process_function = ifnull(v_process_function,'');
      set v_lookup_dataset_code = ifnull(v_lookup_dataset_code,'');
      set v_lookup_return_field = ifnull(v_lookup_return_field,'');

      if v_dataset_db_name <> '' then
        set v_lookup_dataset_code = concat(v_dataset_db_name,'.',v_lookup_dataset_code);
      end if;

      -- filter condition
      if v_process_method <> 'Q' then
        set v_preprocess_filter = ' and ';

				-- filter block
				filter_block:begin
					declare filter_done int default 0;

					declare filter_cursor cursor for
					  select
              filter_field,
              filter_criteria,
              filter_value,
              open_parentheses_flag,
              close_parentheses_flag,
              join_condition
            from recon_mst_tpreprocessfilter
            where preprocess_code = v_preprocess_code
            and active_status = 'Y'
            and delete_flag = 'N'
            order by filter_seqno;

					declare continue handler for not found set filter_done=1;

					open filter_cursor;

					filter_loop: loop
						fetch filter_cursor into
              v_filter_field,
              v_filter_criteria,
              v_filter_value,
              v_open_parentheses_flag,
              v_close_parentheses_flag,
              v_join_condition;

						if filter_done = 1 then leave filter_loop; end if;

            set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
            set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
            set v_join_condition = ifnull(v_join_condition,'');

            if v_join_condition = '' then
              set v_join_condition = 'and';
            end if;

            set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
            set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

            if v_process_method = 'L' then
              set v_filter_field = concat('a.',v_filter_field);
            end if;

            set v_preprocess_filter = concat(v_preprocess_filter,
                                             v_open_parentheses_flag,
                                             fn_get_basefilterformat(v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition,' ');

					end loop filter_loop;

					close filter_cursor;

				end filter_block;

        if v_preprocess_filter = ' and ' then
          set v_preprocess_filter = '';
        else
          set v_preprocess_filter = concat(v_preprocess_filter,' 1 = 1 ');
        end if;
      end if;

      -- lookup condition
      if v_process_method = 'L' then
        set v_lookup_condition = ' and ';

				-- condition block
				condition_block:begin
					declare condition_done int default 0;

					declare condition_cursor cursor for
					  select
              recon_field,
              extraction_criteria,
              extraction_filter,
              lookup_field,
              comparison_criteria,
              comparison_filter,
              open_parentheses_flag,
              close_parentheses_flag,
              join_condition
            from recon_mst_tpreprocesscondition
            where preprocess_code = v_preprocess_code
            and active_status = 'Y'
            and delete_flag = 'N'
            order by condition_seqno;

					declare continue handler for not found set condition_done=1;

					open condition_cursor;

					condition_loop: loop
						fetch condition_cursor into
              v_recon_field,
              v_extraction_criteria,
              v_extraction_filter,
              v_lookup_field,
              v_comparison_criteria,
              v_comparison_filter,
              v_open_parentheses_flag,
              v_close_parentheses_flag,
              v_join_condition;

						if condition_done = 1 then leave condition_loop; end if;

            set v_recon_field = concat('a.',ifnull(v_recon_field,''));
            set v_extraction_criteria = ifnull(v_extraction_criteria,'');
            set v_extraction_filter = ifnull(v_extraction_filter,0);
            set v_lookup_field = concat('b.',ifnull(v_lookup_field,''));
            set v_comparison_criteria = ifnull(v_comparison_criteria,'');
            set v_comparison_filter = ifnull(v_comparison_filter,0);
            set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
            set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
            set v_join_condition = ifnull(v_join_condition,'');

            if v_join_condition = '' then
              set v_join_condition = 'and';
            end if;

            set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
            set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

            set v_recon_field_format = fn_get_fieldfilterformat(v_recon_field,v_extraction_criteria,v_extraction_filter);

            set v_build_condition = concat(v_open_parentheses_flag,
                                           fn_get_comparisoncondition(in_recon_code,v_recon_field_format,v_lookup_field,v_comparison_criteria,v_comparison_filter),
                                           v_close_parentheses_flag,' ',
                                           v_join_condition);

            set v_lookup_condition = concat(v_lookup_condition,v_build_condition,' ');
					end loop condition_loop;

					close condition_cursor;
				end condition_block;

        if v_lookup_condition = ' and ' then
          set v_lookup_condition = 'and 1 = 2 ';
        else
          set v_lookup_condition = concat(v_lookup_condition,' 1 = 1 ');
        end if;
      end if;

      if v_process_method = 'F' then
        set v_sql = 'update $TABLENAME$ set ';
        set v_sql = concat(v_sql,v_update_recon_field,' = ifnull(');
        set v_sql = concat(v_sql,replace(v_process_function,'$FIELD$',v_lookup_return_field),',',v_update_recon_field,') ');
        set v_sql = concat(v_sql,'where recon_code = ',char(39),in_recon_code,char(39),' ');
        set v_sql = concat(v_sql,v_recon_date_condition);
        set v_sql = concat(v_sql,v_preprocess_filter);
        set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' ');

        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
      elseif v_process_method = 'Q' then
        set v_sql = v_process_query;
        set v_sql = concat(v_sql,v_recon_date_condition);

        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
      elseif v_process_method = 'L' then
        if v_recon_date_flag = 'Y' then
          set v_recon_date_condition = '';
          set v_recon_date_condition = concat(v_recon_date_condition,' and a.',v_recon_date_field,' >= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
          set v_recon_date_condition = concat(v_recon_date_condition,' and a.',v_recon_date_field,' <= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
        end if;

        set v_sql = 'update $TABLENAME$ as a ';
        set v_sql = concat(v_sql,'inner join ',v_lookup_dataset_code,' as b ');
        set v_sql = concat(v_sql,'on 1 = 1 ');
        set v_sql = concat(v_sql,v_lookup_condition);
        set v_sql = concat(v_sql,'set a.',v_update_recon_field,'=b.',v_lookup_return_field,' ');
        set v_sql = concat(v_sql,'where a.recon_code = ',char(39),in_recon_code,char(39),' ');
        set v_sql = concat(v_sql,v_recon_date_condition);
        set v_sql = concat(v_sql,v_preprocess_filter);
        set v_sql = concat(v_sql,'and a.delete_flag = ',char(39),'N',char(39),' ');

        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
      end if;
    end loop process_loop;

    close process_cursor;
  end process_block;

  set out_result = 1;
  set out_msg = 'Success';

end $$

DELIMITER ;