DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_preprocesscode` $$
CREATE PROCEDURE `pr_run_preprocesscode`(
  in in_recon_code text,
  in in_preprocess_code text,
  in in_job_gid int,
  in in_postprocess_flag varchar(32),
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_get_recon_field text default '';
  declare v_set_recon_field text default '';
  declare v_cumulative_flag text default '';
  declare v_opening_flag text default '';
  declare v_preprocess_code text default '';
  declare v_preprocess_desc text default '';
  declare v_process_method text default '';
  declare v_process_query text default '';
  declare v_process_function text default '';
  declare v_process_expression text default '';
  declare v_recorderby_type text default '';

  declare v_dataset_db_name text default '';
  declare v_table_name text default '';
  declare v_index_name text default '';
  declare v_index_field text default '';

  declare v_lookup_dataset_code text default '';
  declare v_lookup_return_field text default '';
  declare v_lookup_group_flag text default '';
  declare v_lookup_table text default '';

  declare v_recon_date_flag text default '';
  declare v_recon_date_field text default '';
  declare v_recon_date_condition text default '';
  declare v_recon_condition text default '';

  declare v_field_expression text default '';
  declare v_field text default '';
  declare v_orderby_field text default '';
  declare v_grp_field text default '';
  declare v_idx_grp_field text default '';
  declare v_grpby_condition text default '';

  declare v_recon_field text default '';
  declare v_source_field_type text default '';
  declare v_lookup_field text default '';
  declare v_lookup_grp_field text default '';
  declare v_lookup_multi_return_flag text default '';
  declare v_lookup_agg_return_function text default '';
  declare v_lookup_update_fields text default '';
  declare v_lookup_filter text default '';
  declare v_reverse_update_flag text default '';

  declare v_filter_applied_on text default '';
  declare v_filter_field text default '';
  declare v_filter_criteria text default '';
  declare v_filter_value_flag text default '';
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
  declare v_lookup_field_format text default '';
  declare v_build_condition text default '';

  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_tran_field text default '';
  declare v_tranbrkp_field text default '';

  declare v_preprocess_filter text default '';
  declare v_lookup_condition text default '';

  declare v_aggjoin_condition text default '';
  declare v_cumulative_expression text default '';

  declare v_sql text default '';
  declare v_tran_sql text default '';
  declare v_tranbrkp_sql text default '';

  declare v_count_sql text default '';

  declare i int default 0;

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  drop temporary table if exists recon_tmp_tlookup;

  set in_job_gid = ifnull(in_job_gid,0);

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
  end if;

  -- get dataset db name
  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  if in_automatch_flag = 'Y' then
    /*
    set v_tran_table = concat(in_recon_code,'_tran');
    set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
    */

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
        preprocess_desc,
        get_recon_field,
        set_recon_field,
        process_method,
        process_query,
        process_function,
        process_expression,
        cumulative_flag,
        opening_flag,
        lookup_dataset_code,
        lookup_return_field,
        lookup_group_flag,
        lookup_multi_return_flag,
        lookup_agg_return_function,
        recorderby_type
      from recon_mst_tpreprocess
      where recon_code = in_recon_code
      and preprocess_code = in_preprocess_code
      and postprocess_flag = in_postprocess_flag
      and hold_flag = 'N'
      and active_status = 'Y'
      and delete_flag = 'N'
      order by preprocess_order;
    declare continue handler for not found set process_done=1;

    open process_cursor;

    process_loop: loop
      fetch process_cursor into
        v_preprocess_code,
        v_preprocess_desc,
        v_get_recon_field,
        v_set_recon_field,
        v_process_method,
        v_process_query,
        v_process_function,
        v_process_expression,
        v_cumulative_flag,
        v_opening_flag,
        v_lookup_dataset_code,
        v_lookup_return_field,
        v_lookup_group_flag,
        v_lookup_multi_return_flag,
        v_lookup_agg_return_function,
        v_recorderby_type;

      if process_done = 1 then leave process_loop; end if;

      set v_preprocess_code = ifnull(v_preprocess_code,'');
      set v_preprocess_desc = ifnull(v_preprocess_desc,'');
      set v_get_recon_field = ifnull(v_get_recon_field,'');
      set v_set_recon_field = ifnull(v_set_recon_field,'');
      set v_process_method = ifnull(v_process_method,'');
      set v_process_query = ifnull(v_process_query,'');
      set v_process_function = ifnull(v_process_function,'');
      set v_process_expression = ifnull(v_process_expression,'');
      set v_cumulative_flag = ifnull(v_cumulative_flag,'N');
      set v_opening_flag = ifnull(v_opening_flag,'N');
      set v_lookup_dataset_code = ifnull(v_lookup_dataset_code,'');
      set v_lookup_return_field = ifnull(v_lookup_return_field,'');
      set v_lookup_group_flag = ifnull(v_lookup_group_flag,'N');
      set v_lookup_multi_return_flag = ifnull(v_lookup_multi_return_flag,'N');
      set v_lookup_agg_return_function = ifnull(v_lookup_agg_return_function,'');
      set v_recorderby_type = ifnull(v_recorderby_type,'asc');

      if v_dataset_db_name <> '' then
        set v_lookup_dataset_code = concat(v_dataset_db_name,'.',v_lookup_dataset_code);
      end if;

      set v_lookup_table = v_lookup_dataset_code;

      if v_process_method = 'QCD_QUERY' then
        set v_process_method = 'Q';
      elseif v_process_method = 'QCD_LOOKUP' then
        set v_process_method = 'L';
      elseif v_process_method = 'QCD_FUNCTION' then
        set v_process_method = 'F';
      elseif v_process_method = 'QCD_EXPRESSION' then
        set v_process_method = 'E';
      elseif v_process_method = 'QCD_CUMULATIVEXP' then
        set v_process_method = 'C';
      elseif v_process_method = 'QCD_AGGEXP' then
        set v_process_method = 'A';
      end if;

      if in_postprocess_flag = 'Y' then
        call pr_upd_job(in_job_gid,'P',concat('Applying Postprocess - ',v_preprocess_desc),@msg,@result);
      else
        call pr_upd_job(in_job_gid,'P',concat('Applying Preprocess - ',v_preprocess_desc),@msg,@result);
      end if;

      select v_process_method;

      -- filter condition
      if v_process_method <> 'Q' then
        set v_preprocess_filter = ' and (';
        set v_lookup_filter = ' and (';

				-- filter block
				filter_block:begin
					declare filter_done int default 0;

					declare filter_cursor cursor for
					  select
              filter_applied_on,
              filter_field,
              filter_criteria,
              filter_value_flag,
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
              v_filter_applied_on,
              v_filter_field,
              v_filter_criteria,
              v_filter_value_flag,
              v_filter_value,
              v_open_parentheses_flag,
              v_close_parentheses_flag,
              v_join_condition;

						if filter_done = 1 then leave filter_loop; end if;

            set v_filter_applied_on = ifnull(v_filter_applied_on,'');

            if v_filter_applied_on = '' then
              set v_filter_applied_on = 'RECON';
            end if;

            set v_filter_field = ifnull(v_filter_field,'');
            set v_filter_criteria = ifnull(v_filter_criteria,'');

            set v_filter_value_flag = ifnull(v_filter_value_flag,'Y');
            set v_filter_value = ifnull(v_filter_value,'');

            set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
            set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
            set v_join_condition = ifnull(v_join_condition,'');

            if v_join_condition = '' then
              set v_join_condition = 'and';
            end if;

            set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
            set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

            if v_process_method = 'L' then
              if v_filter_applied_on = 'LOOKUP' then
                set v_filter_field = concat('b.',v_filter_field);
                set v_filter_field = fn_get_dsfieldnamecast(v_lookup_dataset_code,v_filter_field);
              else
                set v_filter_field = concat('a.',v_filter_field);
                set v_filter_field = fn_get_reconfieldnamecast(in_recon_code,v_filter_field);
              end if;

              if v_filter_value_flag <> 'Y' then
                if v_filter_applied_on = 'LOOKUP' then
                  set v_filter_value = concat('b.',v_filter_value);
                  set v_filter_value = fn_get_dsfieldnamecast(v_lookup_dataset_code,v_filter_value);
                else
                  set v_filter_value = concat('a.',v_filter_value);
                  set v_filter_value = fn_get_reconfieldnamecast(in_recon_code,v_filter_value);
                end if;
              end if;
            elseif v_process_method = 'A' then
              set v_filter_field = concat('a.',v_filter_field);
              set v_filter_field = fn_get_dsfieldnamecast(v_lookup_dataset_code,v_filter_field);
            end if;

            if v_filter_applied_on = 'LOOKUP' then
              set v_lookup_filter = concat(v_lookup_filter,
                                             v_open_parentheses_flag,
                                             fn_get_basefilterformat(v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value_flag,v_filter_value),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition,' ');
            else
              set v_preprocess_filter = concat(v_preprocess_filter,
                                             v_open_parentheses_flag,
                                             fn_get_basefilterformat(v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value_flag,v_filter_value),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition,' ');
            end if;

					end loop filter_loop;

					close filter_cursor;

				end filter_block;

        set v_preprocess_filter = concat(v_preprocess_filter,' 1 = 1) ');
        set v_lookup_filter = concat(v_lookup_filter,' 1 = 1) ');
      end if;

      -- lookup method
      if v_process_method = 'L' then
        -- lookup update fields
        if v_lookup_multi_return_flag = 'Y' then
          set v_lookup_update_fields = '';

					-- group field block
					updfield_block:begin
						declare updfield_done int default 0;

						declare updfield_cursor cursor for
							select
								set_recon_field,
								lookup_return_field,
                reverse_update_flag
							from recon_mst_tpreprocesslookup
							where preprocess_code = v_preprocess_code
							and active_status = 'Y'
							and delete_flag = 'N'
							order by lookup_seqno;

						declare continue handler for not found set updfield_done=1;

						open updfield_cursor;

						updfield_loop: loop
							fetch updfield_cursor into v_set_recon_field,v_lookup_return_field,v_reverse_update_flag;

							if updfield_done = 1 then leave updfield_loop; end if;

							set v_set_recon_field = ifnull(v_set_recon_field,'');
							set v_lookup_return_field = ifnull(v_lookup_return_field,'');
							set v_reverse_update_flag = ifnull(v_reverse_update_flag,'N');

							if v_set_recon_field <> '' and v_lookup_return_field <> '' then
                if v_reverse_update_flag = 'Y' then
                  -- update lookup dataset field
								  set v_lookup_update_fields = concat(v_lookup_update_fields,',b.',v_lookup_return_field,'=a.',v_set_recon_field);
                else
								  set v_lookup_update_fields = concat(v_lookup_update_fields,',a.',v_set_recon_field,'=b.',v_lookup_return_field);
                end if;
							end if;
						end loop updfield_loop;

						close updfield_cursor;
					end updfield_block;

          set v_lookup_update_fields = substr(v_lookup_update_fields,2);
        else
          set v_lookup_update_fields = concat('a.',v_set_recon_field,'=b.',v_lookup_return_field,' ');
        end if;

        -- lookup condition
        set v_lookup_condition = ' and (';

				-- condition block
				condition_block:begin
					declare condition_done int default 0;

					declare condition_cursor cursor for
					  select
              source_field_type,
              recon_field,
              extraction_criteria,
              lookup_field,
              comparison_criteria,
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
              v_source_field_type,
              v_recon_field,
              v_extraction_criteria,
              v_lookup_field,
              v_comparison_criteria,
              v_open_parentheses_flag,
              v_close_parentheses_flag,
              v_join_condition;

						if condition_done = 1 then leave condition_loop; end if;

            set v_source_field_type = ifnull(v_source_field_type,'RECON');
            set v_recon_field = concat('a.',ifnull(v_recon_field,''));
            set v_extraction_criteria = ifnull(v_extraction_criteria,'');
            set v_index_field = ifnull(v_lookup_field,'');
            set v_lookup_field = concat('b.',ifnull(v_lookup_field,''));
            set v_comparison_criteria = ifnull(v_comparison_criteria,'');
            set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
            set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
            set v_join_condition = ifnull(v_join_condition,'');

            if v_join_condition = '' then
              set v_join_condition = 'and';
            end if;

            set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
            set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

            if v_source_field_type = 'RECON' then
              set v_recon_field = fn_get_reconfieldnamecast(in_recon_code,v_recon_field);

              set v_recon_field_format = fn_get_fieldfilterformat(v_recon_field,v_extraction_criteria,0);

              set v_build_condition = concat(v_open_parentheses_flag,
                                           fn_get_comparisoncondition(in_recon_code,v_recon_field_format,v_lookup_field,v_comparison_criteria,0),
                                           v_close_parentheses_flag,' ',
                                           v_join_condition);
            else
              set v_lookup_field = fn_get_dsfieldnamecast(v_lookup_dataset_code,v_lookup_field);

              set v_lookup_field_format = fn_get_fieldfilterformat(v_lookup_field,v_extraction_criteria,0);

              set v_build_condition = concat(v_open_parentheses_flag,
                                           fn_get_comparisoncondition(in_recon_code,v_lookup_field_format,v_recon_field,v_comparison_criteria,0),
                                           v_close_parentheses_flag,' ',
                                           v_join_condition);
            end if;

            set v_lookup_condition = concat(v_lookup_condition,v_build_condition,' ');

            -- index lookup field
            set v_index_name = concat('idx_',v_index_field);

            -- table name
            if instr(v_lookup_dataset_code,'.') > 0 then
              set v_table_name = split(v_lookup_dataset_code,'.',2);
            else
              set v_table_name = v_lookup_dataset_code;
            end if;

            if not exists (select table_schema, table_name, index_name, column_name
              FROM information_schema.statistics
              WHERE table_schema = v_dataset_db_name
              and table_name = v_table_name
              and index_name = v_index_name) and v_index_field <> '' then

              if substr(v_index_field,1,3) = 'col' then
                set v_sql = concat('create index idx_',v_index_field,' on ',
                                 v_lookup_dataset_code,'(',v_index_field,'(255))');
              else
                set v_sql = concat('create index idx_',v_index_field,' on ',
                                 v_lookup_dataset_code,'(',v_index_field,')');
              end if;

              call pr_run_sql(v_sql,@msg,@result);
            end if;
					end loop condition_loop;

					close condition_cursor;
				end condition_block;

        if v_lookup_condition = ' and (' then
          set v_lookup_condition = 'and 1 = 2 ';
        else
          set v_lookup_condition = concat(v_lookup_condition,' 1 = 1) ');
        end if;

        -- group field
        if v_lookup_group_flag = 'Y' and v_lookup_multi_return_flag <> 'Y' then
					-- create temporary table
          drop temporary table if exists recon_tmp_tlookup;

					create temporary table recon_tmp_tlookup select * from recon_tmp_tdatasetstru where 1 = 2;
					alter table recon_tmp_tlookup add primary key(dataset_gid);
					alter table recon_tmp_tlookup modify dataset_gid integer unsigned AUTO_INCREMENT;

          set v_lookup_grp_field = '';

					-- look field block
					lookupfield_block:begin
						declare lookupfield_done int default 0;

						declare lookupfield_cursor cursor for
							select
								distinct lookup_field
							from recon_mst_tpreprocesscondition
							where preprocess_code = v_preprocess_code
							and lookup_field <> v_lookup_return_field
							and lookup_field <> ''
							and active_status = 'Y'
							and delete_flag = 'N';

						declare continue handler for not found set lookupfield_done=1;

						open lookupfield_cursor;

						lookupfield_loop: loop
							fetch lookupfield_cursor into v_lookup_field;

							if lookupfield_done = 1 then leave lookupfield_loop; end if;

							if v_lookup_grp_field = '' then
								set v_lookup_grp_field = v_lookup_field;
							else
								set v_lookup_grp_field = concat(v_lookup_grp_field,',',v_lookup_field);
							end if;

							-- index lookup field
							set v_index_name = concat('idx_',v_lookup_field);

							if substr(v_lookup_field,1,3) = 'col' then
								set v_sql = concat('create index idx_',v_lookup_field,' on recon_tmp_tlookup(',v_lookup_field,'(255))');
							else
								set v_sql = concat('create index idx_',v_lookup_field,' on recon_tmp_tlookup(',v_lookup_field,')');
							end if;

							call pr_run_sql(v_sql,@msg,@result);
						end loop lookupfield_loop;

						close lookupfield_cursor;
					end lookupfield_block;

					set v_sql = 'insert into recon_tmp_tlookup(';
					set v_sql = concat(v_sql,v_lookup_grp_field,',',v_lookup_return_field,') ');
					set v_sql = concat(v_sql,'select ',v_lookup_grp_field,',');
					set v_sql = concat(v_sql,'replace(group_concat(distinct cast(',v_lookup_return_field,' as nchar)),'','','';'')     ');
					set v_sql = concat(v_sql,'from ',v_lookup_dataset_code,' ');
					set v_sql = concat(v_sql,'where ',v_lookup_return_field,' <> '''' ');
          set v_sql = concat(v_sql,replace(v_lookup_filter,'b.',''));
					set v_sql = concat(v_sql,'and delete_flag = ''N'' ');
					set v_sql = concat(v_sql,'group by ',v_lookup_grp_field);

					call pr_run_sql(v_sql,@msg,@result);

          set v_lookup_table = 'recon_tmp_tlookup';
        end if;
      end if;

      if v_process_method = 'F' or v_process_method = 'E' then
        if v_recon_date_flag = 'Y' then
          if v_recon_date_field <> 'tran_date' then
            set v_recon_date_field = concat('cast(',v_recon_date_field,' as date)');
          end if;

          set v_recon_date_condition = '';
          set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' >= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
          set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' <= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
        end if;

        set v_sql = 'update $TABLENAME$ set ';
        set v_sql = concat(v_sql,v_set_recon_field,' = ',replace(v_process_function,'$FIELD$',v_get_recon_field),' ');
        set v_sql = concat(v_sql,'where recon_code = ',char(39),in_recon_code,char(39),' ');
        set v_sql = concat(v_sql,v_recon_date_condition);
        set v_sql = concat(v_sql,v_preprocess_filter);
        set v_sql = concat(v_sql,'and tran_gid > 0 ');
        set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' ');

        call pr_run_sql('set @sno := 0',@msg,@result);

        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
      elseif v_process_method = 'C' then
        set v_orderby_field = '';

				-- order by field block
				orderbyfield_block:begin
					declare orderbyfield_done int default 0;

					declare orderbyfield_cursor cursor for
						select
							distinct recorder_field
						from recon_mst_tpreprocessrecorder
						where preprocess_code = v_preprocess_code
						and active_status = 'Y'
						and delete_flag = 'N'
						order by recorder_seqno;

					declare continue handler for not found set orderbyfield_done=1;

					open orderbyfield_cursor;

					orderbyfield_loop: loop
						fetch orderbyfield_cursor into v_field;

						if orderbyfield_done = 1 then leave orderbyfield_loop; end if;

						if v_orderby_field = '' then
							set v_orderby_field = v_field;
						else
							set v_orderby_field = concat(v_orderby_field,',',v_field);
						end if;
					end loop orderbyfield_loop;

					close orderbyfield_cursor;
				end orderbyfield_block;

        if v_recon_date_flag = 'Y' then
          if v_recon_date_field <> 'tran_date' then
            set v_recon_date_field = concat('cast(',v_recon_date_field,' as date)');
          end if;

          set v_recon_date_condition = '';
          set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' >= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
          set v_recon_date_condition = concat(v_recon_date_condition,' and ',v_recon_date_field,' <= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
        end if;

        if v_orderby_field <> '' then
          set v_orderby_field = concat('order by ',v_orderby_field,',');
        else
          set v_orderby_field = 'order by ';
        end if;

        set v_sql = 'update $TABLENAME$ set ';
        set v_sql = concat(v_sql,v_set_recon_field,' = ',replace(v_process_function,'$FIELD$',v_get_recon_field),' ');
        set v_sql = concat(v_sql,'where recon_code = ',char(39),in_recon_code,char(39),' ');
        set v_sql = concat(v_sql,v_recon_date_condition);
        set v_sql = concat(v_sql,v_preprocess_filter);
        set v_sql = concat(v_sql,'and tran_gid > 0 ');
        set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' ');
        set v_sql = concat(v_sql,v_orderby_field,' ');

        set @cumulative_value := 0;
        call pr_run_sql(replace(concat(v_sql,'tran_gid ',v_recorderby_type),'$TABLENAME$',v_tran_table),@msg,@result);

        set @cumulative_value := 0;
        call pr_run_sql(replace(concat(v_sql,'tranbrkp_gid ',v_recorderby_type),'$TABLENAME$',v_tranbrkp_table),@msg,@result);

        if v_opening_flag = 'Y' then
          set v_field_expression = concat(fn_get_fieldnamecast(in_recon_code,v_set_recon_field),'-',
                               fn_get_castexpression(in_recon_code,v_process_expression));

          set v_field_expression = fn_get_expressionformat(in_recon_code,v_set_recon_field,v_field_expression,false);

          set v_sql = 'update $TABLENAME$ set ';
          set v_sql = concat(v_sql,v_set_recon_field,' = ',replace(v_field_expression,'$FIELD$',v_get_recon_field),' ');
          set v_sql = concat(v_sql,'where recon_code = ',char(39),in_recon_code,char(39),' ');
          set v_sql = concat(v_sql,v_recon_date_condition);
          set v_sql = concat(v_sql,v_preprocess_filter);
          set v_sql = concat(v_sql,'and tran_gid > 0 ');
          set v_sql = concat(v_sql,'and delete_flag = ',char(39),'N',char(39),' ');
          set v_sql = concat(v_sql,v_orderby_field,' ');

          call pr_run_sql(replace(concat(v_sql,',tran_gid ',v_recorderby_type),'$TABLENAME$',v_tran_table),@msg,@result);
          call pr_run_sql(replace(concat(v_sql,',tranbrkp_gid ',v_recorderby_type),'$TABLENAME$',v_tranbrkp_table),@msg,@result);
        end if;
      elseif v_process_method = 'A' then
        set v_aggjoin_condition = ' 1 = 1 ';
        set v_grp_field = '';
        set v_idx_grp_field = '';

				-- grp by field block
				grpfield_block:begin
					declare grpfield_done int default 0;

					declare grpfield_cursor cursor for
						select
							distinct grp_field
						from recon_mst_tpreprocessgrpfield
						where preprocess_code = v_preprocess_code
						and active_status = 'Y'
						and delete_flag = 'N'
						order by grpfield_seqno;

					declare continue handler for not found set grpfield_done=1;

					open grpfield_cursor;

					grpfield_loop: loop
						fetch grpfield_cursor into v_field;

						if grpfield_done = 1 then leave grpfield_loop; end if;

						if v_grp_field = '' then
							set v_grp_field = v_field;
						else
							set v_grp_field = concat(v_grp_field,',',v_field);
						end if;

            -- agg group condition
            set v_aggjoin_condition = concat(v_aggjoin_condition,' and a.',v_field,' = b.',v_field,' ');

            if substr(v_field,1,3) = 'col' then
              set v_field = concat(v_field,'(255)');
            end if;

            if v_idx_grp_field = '' then
							set v_idx_grp_field = v_field;
            else
							set v_idx_grp_field = concat(v_idx_grp_field,',',v_field);
            end if;
					end loop grpfield_loop;

					close grpfield_cursor;
				end grpfield_block;

        if v_recon_date_flag = 'Y' then
          if v_recon_date_field <> 'tran_date' then
            set v_recon_date_field = concat('cast(a.',v_recon_date_field,' as date)');
          end if;

          set v_recon_date_condition = '';
          set v_recon_date_condition = concat(v_recon_date_condition,' and a.',v_recon_date_field,' >= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
          set v_recon_date_condition = concat(v_recon_date_condition,' and a.',v_recon_date_field,' <= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
        end if;

        -- recon condition
        set v_recon_condition = concat("and a.recon_code = '",in_recon_code,"'
          ",v_recon_date_condition,"
          ",v_preprocess_filter," and
          a.delete_flag = 'N'
          ");

        -- agg temporary tables
        drop temporary table if exists recon_tmp_ttranagg;
        drop temporary table if exists recon_tmp_ttranbrkpagg;

				create temporary table recon_tmp_ttranagg select * from recon_trn_ttranwithbrkp where 1 = 2;
				alter table recon_tmp_ttranagg ENGINE = MyISAM;
				alter table recon_tmp_ttranagg add agg_gid int not null primary key AUTO_INCREMENT FIRST;
				create index idx_excp_value on recon_tmp_ttranagg(excp_value);
				create index idx_tran_date on recon_tmp_ttranagg(tran_date);
				create index idx_recon_code on recon_tmp_ttranagg(recon_code);
				create index idx_dataset_code on recon_tmp_ttranagg(recon_code,dataset_code);

				create temporary table recon_tmp_ttranbrkpagg select * from recon_trn_ttranwithbrkp where 1 = 2;
				alter table recon_tmp_ttranbrkpagg ENGINE = MyISAM;
				alter table recon_tmp_ttranbrkpagg add agg_gid int not null primary key AUTO_INCREMENT FIRST;
				create index idx_excp_value on recon_tmp_ttranbrkpagg(excp_value);
				create index idx_tran_date on recon_tmp_ttranbrkpagg(tran_date);
				create index idx_recon_code on recon_tmp_ttranbrkpagg(recon_code);
				create index idx_dataset_code on recon_tmp_ttranbrkpagg(recon_code,dataset_code);

        set v_field_expression = fn_get_expressionformat(in_recon_code,v_set_recon_field,v_process_expression,false);

        -- create index
        if v_idx_grp_field <> '' then
          set v_sql = concat("create index idx_grp_field on recon_tmp_ttranagg(",v_idx_grp_field,")");
          call pr_run_sql2(v_sql,@msg,@result);

          set v_sql = concat("create index idx_grp_field on recon_tmp_ttranbrkpagg(",v_idx_grp_field,")");
          call pr_run_sql2(v_sql,@msg,@result);

          -- insert records in agg table
          set v_sql = concat("insert into recon_tmp_ttranagg(",v_grp_field,",",v_set_recon_field,",col128)
            select ",v_grp_field,",",v_process_function,",",v_field_expression," from ",v_tran_table,"
            where 1 = 1
            ",replace(v_recon_condition,'a.',''),"
            group by ",v_grp_field,"
            ");

          call pr_run_sql2(v_sql,@msg,@result);

          set v_sql = concat("insert into recon_tmp_ttranbrkpagg(",v_grp_field,",",v_set_recon_field,",col128)
            select ",v_grp_field,",",v_process_function,",",v_field_expression," from ",v_tranbrkp_table,"
            where 1 = 1
            ",replace(v_recon_condition,'a.',''),"
            and tran_gid > 0
            group by ",v_grp_field,"
            ");

          call pr_run_sql2(v_sql,@msg,@result);
        else
          -- insert records in agg table
          set v_sql = concat("insert into recon_tmp_ttranagg(",v_set_recon_field,",col128)
            select ",v_process_function,",",v_field_expression," from ",v_tran_table,"
            where 1 = 1
            ",replace(v_recon_condition,'a.',''),"
            and tran_gid > 0
            ");

          call pr_run_sql2(v_sql,@msg,@result);

          set v_sql = concat("insert into recon_tmp_ttranbrkpagg(",v_set_recon_field,",col128)
            select ",v_process_function,",",v_field_expression," from ",v_tranbrkp_table,"
            where 1 = 1
            ",replace(v_recon_condition,'a.',''),"
            and tran_gid > 0
            ");

          call pr_run_sql2(v_sql,@msg,@result);
        end if;


        if v_cumulative_flag = 'Y' then
          set v_cumulative_expression = fn_get_expressionformat(in_recon_code,
                                                                v_set_recon_field,
                                                                concat('[',fn_get_reconfieldname(in_recon_code,v_set_recon_field),']'),
                                                                true);

					set v_sql = 'update recon_tmp_ttranagg set ';
					set v_sql = concat(v_sql,v_set_recon_field,' = ',v_cumulative_expression,' ');
					set v_sql = concat(v_sql,'order by agg_gid ');

					set @cumulative_value := 0;
					call pr_run_sql2(v_sql,@msg,@result);

					set v_sql = 'update recon_tmp_ttranbrkpagg set ';
					set v_sql = concat(v_sql,v_set_recon_field,' = ',v_cumulative_expression,' ');
					set v_sql = concat(v_sql,'order by agg_gid ');

					set @cumulative_value := 0;
					call pr_run_sql2(v_sql,@msg,@result);
        end if;

        if v_opening_flag = 'Y' then
          set v_field_expression = concat(fn_get_fieldnamecast(in_recon_code,v_set_recon_field),
                               ' - cast(col128 as decimal(15,2))');

          set v_field_expression = fn_get_expressionformat(in_recon_code,v_set_recon_field,v_field_expression,false);

					set v_sql = 'update recon_tmp_ttranagg set ';
					set v_sql = concat(v_sql,v_set_recon_field,' = ',v_field_expression,' ');
					set v_sql = concat(v_sql,'order by agg_gid ');

					call pr_run_sql2(v_sql,@msg,@result);

					set v_sql = 'update recon_tmp_ttranbrkpagg set ';
					set v_sql = concat(v_sql,v_set_recon_field,' = ',v_field_expression,' ');
					set v_sql = concat(v_sql,'order by agg_gid ');

					call pr_run_sql2(v_sql,@msg,@result);
        end if;

        -- update in tran table
        set v_sql = concat('update ',v_tran_table,' as a ');
        set v_sql = concat(v_sql,'inner join recon_tmp_ttranagg as b on ',v_aggjoin_condition,' ');
        set v_sql = concat(v_sql,'set a.',v_set_recon_field,' = b.',v_set_recon_field,' ');
        set v_sql = concat(v_sql,'where 1 = 1 ');
        set v_sql = concat(v_sql,v_recon_condition);

        call pr_run_sql2(v_sql,@msg,@result);

        -- update in tran table
        set v_sql = concat('update ',v_tranbrkp_table,' as a ');
        set v_sql = concat(v_sql,'inner join recon_tmp_ttranbrkpagg as b on ',v_aggjoin_condition,' ');
        set v_sql = concat(v_sql,'set a.',v_set_recon_field,' = b.',v_set_recon_field,' ');
        set v_sql = concat(v_sql,'where 1 = 1 ');
        set v_sql = concat(v_sql,v_recon_condition);
        set v_sql = concat(v_sql,'and a.tran_gid ');

        call pr_run_sql2(v_sql,@msg,@result);

        drop temporary table if exists recon_tmp_ttranagg;
        drop temporary table if exists recon_tmp_ttranbrkpagg;
      elseif v_process_method = 'Q' then
        set v_sql = v_process_query;
        -- set v_sql = concat(v_sql,v_recon_date_condition);

        call pr_run_sql1(v_sql,@msg,@result);
      elseif v_process_method = 'L' then
        if v_recon_date_flag = 'Y' then
          set v_recon_date_condition = '';
          set v_recon_date_condition = concat(v_recon_date_condition,' and a.',v_recon_date_field,' >= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_from,'%Y-%m-%d'),char(39),' ');
          set v_recon_date_condition = concat(v_recon_date_condition,' and a.',v_recon_date_field,' <= ');
          set v_recon_date_condition = concat(v_recon_date_condition,char(39),date_format(in_period_to,'%Y-%m-%d'),char(39),' ');
        end if;

        set v_sql = 'update $TABLENAME$ as a ';
        set v_sql = concat(v_sql,'inner join ',v_lookup_table,' as b ');
        set v_sql = concat(v_sql,'on 1 = 1 ');
        set v_sql = concat(v_sql,v_lookup_condition);

        if v_lookup_group_flag <> 'Y' then
          set v_sql = concat(v_sql,v_lookup_filter);
        end if;

        set v_sql = concat(v_sql,'and b.delete_flag = ',char(39),'N',char(39),' ');
        set v_sql = concat(v_sql,'set ',v_lookup_update_fields,' ');
        set v_sql = concat(v_sql,'where a.recon_code = ',char(39),in_recon_code,char(39),' ');
        set v_sql = concat(v_sql,v_recon_date_condition);
        set v_sql = concat(v_sql,v_preprocess_filter);
        set v_sql = concat(v_sql,'and a.tran_gid > 0 ');
        set v_sql = concat(v_sql,'and a.delete_flag = ',char(39),'N',char(39),' ');

        set v_count_sql = 'select count(*) into @base_count from $TABLENAME$ as a ';
        set v_count_sql = concat(v_count_sql,'where a.recon_code = ',char(39),in_recon_code,char(39),' ');
        set v_count_sql = concat(v_count_sql,v_recon_date_condition);
        set v_count_sql = concat(v_count_sql,v_preprocess_filter);
        set v_count_sql = concat(v_count_sql,'and a.tran_gid > 0 ');
        set v_count_sql = concat(v_count_sql,'and a.delete_flag = ',char(39),'N',char(39),' ');

        call pr_run_sql(replace(v_count_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        set @base_count = ifnull(@base_count,0);

        if @base_count > 0 then
          call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tran_table),@msg,@result);
        end if;

        select v_sql;

        call pr_run_sql(replace(v_count_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
        set @base_count = ifnull(@base_count,0);

        if @base_count > 0 then
          call pr_run_sql(replace(v_sql,'$TABLENAME$',v_tranbrkp_table),@msg,@result);
        end if;

        set @base_count = 0;
      elseif v_process_method = 'QCD_COMPARISONEXP' then
        call pr_run_preprocess_comparison(in_recon_code,
                                          v_preprocess_code,
                                          in_job_gid,
                                          in_postprocess_flag,
                                          in_period_from,
                                          in_period_to,
                                          in_automatch_flag,
                                          @msg1,
                                          @result1);
      end if;
    end loop process_loop;

    close process_cursor;
  end process_block;

  drop temporary table if exists recon_tmp_tlookup;

  set out_result = 1;
  set out_msg = 'Success';

end $$

DELIMITER ;