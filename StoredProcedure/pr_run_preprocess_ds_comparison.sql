DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_preprocess_ds_comparison` $$
CREATE PROCEDURE `pr_run_preprocess_ds_comparison`
(
  in in_recon_code text,
  in in_preprocess_code text,
  in in_job_gid int,
  in in_postprocess_flag text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 13-02-2026

    Updated By : Vijayavel
    Updated Date :

    Version : 1
  */

  declare v_recon_version text default '';
  declare v_preprocess_code text default '';
  declare v_preprocess_desc text default '';
  declare v_process_method text default '';
  declare v_recorderby_type text default '';

  declare v_dataset_db_name text default '';
  declare v_table_name text default '';
  declare v_index_name text default '';
  declare v_index_field text default '';

  declare v_recon_date_flag text default '';
  declare v_recon_date_field text default '';
  declare v_recon_date_condition text default '';
  declare v_recon_condition text default '';

  declare v_field_expression text default '';
  declare v_field text default '';
  declare v_orderby_field text default '';

  declare v_source_field_type text default '';
  declare v_ds_update_fields text default '';
  declare v_reverse_update_flag text default '';
  declare v_value_flag text default '';

  declare v_filter_applied_on text default '';
  declare v_filter_field text default '';
  declare v_filter_criteria text default '';
  declare v_filter_value_flag text default '';
  declare v_filter_value text default '';

  declare v_extraction_criteria text default '';
  declare v_extraction_filter int default 0;
  declare v_extraction_value text default '';

  declare v_source_filter int default 0;

  declare v_comparison_criteria text default '';
  declare v_comparison_filter int default 0;
  declare v_comparison_value text default '';

  declare v_open_parentheses_flag text default '';
  declare v_close_parentheses_flag text default '';
  declare v_join_condition text default '';

  declare v_source_field_format text default '';
  declare v_build_condition text default '';

  declare v_preprocess_filter text default '';
  declare v_comparison_condition text default '';

  declare v_sql text default '';

  declare v_sysdatetime text default '';

  declare i int default 0;
  declare n int default 0;

  declare v_ds_database varchar(32) default '';

  declare v_source_field text default '';
  declare v_comparison_field text default '';

  declare v_source_dataset_code varchar(32) default '';
  declare v_comparison_dataset_code varchar(32) default '';

  declare v_source_table text default '';
  declare v_comparison_table text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  set in_job_gid = ifnull(in_job_gid,0);

  if in_preprocess_code = '' then
    set in_preprocess_code = null;
  end if;

  if in_preprocess_code <> '' then
    set in_postprocess_flag = null;
  end if;

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
      recon_date_field,
      recon_rule_version
    into
      v_recon_date_flag,
      v_recon_date_field,
      v_recon_version
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_recon_date_flag = ifnull(v_recon_date_flag,'');
    set v_recon_date_field = ifnull(v_recon_date_field,'');
    set v_recon_version = ifnull(v_recon_version,'');
  end if;

  -- get dataset db name
  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  -- process
  process_block:begin
    declare process_done int default 0;
    declare process_cursor cursor for
      select
        preprocess_code,
        preprocess_desc,
        source_dataset_code,
        comparison_dataset_code
      from recon_mst_tpreprocesshistory
      where recon_code = in_recon_code
      and recon_version = v_recon_version
      and preprocess_code = ifnull(in_preprocess_code,preprocess_code)
      and postprocess_flag = ifnull(in_postprocess_flag,postprocess_flag)
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
        v_source_dataset_code,
        v_comparison_dataset_code;

      if process_done = 1 then leave process_loop; end if;

      set v_preprocess_code = ifnull(v_preprocess_code,'');
      set v_preprocess_desc = ifnull(v_preprocess_desc,'');

      set v_source_dataset_code = ifnull(v_source_dataset_code,'');
      set v_comparison_dataset_code = ifnull(v_comparison_dataset_code,'');

      if v_dataset_db_name <> '' then
        set v_source_table = concat(v_dataset_db_name,'.',v_source_dataset_code);
        set v_comparison_table = concat(v_dataset_db_name,'.',v_comparison_dataset_code);
      end if;

      if in_postprocess_flag = 'Y' then
        call pr_upd_job(in_job_gid,'P',concat('Applying Postprocess - ',v_preprocess_desc),@msg,@result);
      else
        call pr_upd_job(in_job_gid,'P',concat('Applying Preprocess - ',v_preprocess_desc),@msg,@result);
      end if;

      -- lookup method
      if v_process_method = 'QCD_LOOKUP_COMPARISON' then
        set v_source_filter = ' and (';
        set v_comparison_filter = ' and (';

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
            from recon_mst_tpreprocessfilterhistory
            where preprocess_code = v_preprocess_code
            and recon_version = v_recon_version
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
              set v_filter_applied_on = 'SOURCE';
            end if;

            set v_filter_field = ifnull(v_filter_field,'');
            set v_filter_criteria = ifnull(v_filter_criteria,'');

            set v_filter_value_flag = ifnull(v_filter_value_flag,'Y');
            set v_filter_value = ifnull(v_filter_value,'');

            if v_filter_value_flag = 'Y' then
              set v_filter_value = fn_get_filtervalue(in_recon_code,v_filter_value,'');
            end if;

            set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
            set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
            set v_join_condition = ifnull(v_join_condition,'');

            if v_join_condition = '' then
              set v_join_condition = 'and';
            end if;

            if v_filter_field = '' then
              set v_filter_value_flag = '';
              set v_filter_value = '';
            end if;

            set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
            set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

            if v_filter_applied_on = 'SOURCE' then
              set v_filter_field = concat('a.',v_filter_field);
              set v_filter_field = fn_get_dsfieldnamecast(v_source_dataset_code,v_filter_field);
            elseif v_filter_applied_on = 'COMPARISON' then
              set v_filter_field = concat('b.',v_filter_field);
              set v_filter_field = fn_get_dsfieldnamecast(v_comparison_dataset_code,v_filter_field);
            end if;

            if v_filter_value_flag <> 'Y' then
              if v_filter_applied_on = 'SOURCE' then
                set v_filter_value = concat('a.',v_filter_value);
                set v_filter_value = fn_get_dsfieldnamecast(v_source_dataset_code,v_filter_value);
              elseif v_filter_applied_on = 'COMPARISON' then
                set v_filter_value = concat('b.',v_filter_value);
                set v_filter_value = fn_get_dsfieldnamecast(v_comparison_dataset_code,v_filter_value);
              end if;
            end if;

            if v_filter_applied_on = 'SOURCE' then
              set v_source_filter = concat(v_source_filter,
                                             v_open_parentheses_flag,
                                             fn_get_basefilterdsformat(v_source_dataset_code,v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value_flag,v_filter_value),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition,' ');
            elseif v_filter_applied_on = 'COMPARISON' then
              set v_comparison_filter = concat(v_comparison_filter,
                                             v_open_parentheses_flag,
                                             fn_get_basefilterdsformat(v_comparison_dataset_code,v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value_flag,v_filter_value),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition,' ');
            end if;

					end loop filter_loop;

					close filter_cursor;

				end filter_block;

        set v_source_filter = concat(v_source_filter,' 1 = 1) ');
        set v_comparison_filter = concat(v_comparison_filter,' 1 = 1) ');

        call pr_get_reconstaticvaluesql(v_source_filter,'',in_recon_code,'',in_user_code,@v_source_filter,@msg22,@result22);
        set v_source_filter = @v_source_filter;

        call pr_get_reconstaticvaluesql(v_comparison_filter,'',in_recon_code,'',in_user_code,@v_comparison_filter,@msg22,@result22);
        set v_comparison_filter = @v_comparison_filter;

        -- dataset update fields
        set v_ds_update_fields = '';

        -- update field block
				updfield_block:begin
					declare updfield_done int default 0;

					declare updfield_cursor cursor for
						select
							source_field,
							comparison_field,
							reverse_update_flag,
							value_flag
						from recon_mst_tpreprocessdsupdatehistory
						where preprocess_code = v_preprocess_code
						and recon_version = v_recon_version
						and active_status = 'Y'
						and delete_flag = 'N'
						order by rec_seqno;

					declare continue handler for not found set updfield_done=1;

					open updfield_cursor;

					updfield_loop: loop
						fetch updfield_cursor into v_source_field,v_comparison_field,
																			 v_reverse_update_flag,v_value_flag;

						if updfield_done = 1 then leave updfield_loop; end if;

						set v_source_field = ifnull(v_source_field,'');
						set v_comparison_field = ifnull(v_comparison_field,'');
						set v_reverse_update_flag = ifnull(v_reverse_update_flag,'N');
						set v_value_flag = ifnull(v_value_flag,'N');

						if v_source_field <> '' and v_comparison_field <> '' then
							if v_reverse_update_flag = 'Y' then
								-- update lookup dataset field
								if v_value_flag = 'Y' then
									set v_ds_update_fields = concat(v_ds_update_fields,',b.',v_comparison_field,' = ',char(39),v_source_field,char(39),' ');
								else
									set v_ds_update_fields = concat(v_ds_update_fields,',b.',v_comparison_field,' = cast(a.',v_source_field,' as nchar) ');
								end if;
							else
								if v_value_flag = 'Y' then
									set v_ds_update_fields = concat(v_ds_update_fields,',a.',v_source_field,' = ',char(39),v_comparison_field,char(39),' ');
								else
									set v_ds_update_fields = concat(v_ds_update_fields,',a.',v_source_field,' = b.',v_comparison_field,' ');
								end if;
							end if;
						end if;
					end loop updfield_loop;

					close updfield_cursor;
				end updfield_block;

				set v_ds_update_fields = substr(v_ds_update_fields,2);

        call pr_get_reconstaticvaluesql(v_ds_update_fields,'',in_recon_code,'',in_user_code,@v_ds_update_fields,@msg22,@result22);
        set v_ds_update_fields = @v_ds_update_fields;

        -- lookup condition
        set v_comparison_condition = ' and (';

				-- condition block
				condition_block:begin
					declare condition_done int default 0;

					declare condition_cursor cursor for
					  select
              source_field_type,
              source_field,
              extraction_criteria,
              comparison_field,
              comparison_criteria,
              open_parentheses_flag,
              close_parentheses_flag,
              join_condition
            from recon_mst_tpreprocessconditionhistory
            where preprocess_code = v_preprocess_code
            and recon_version = v_recon_version
            and active_status = 'Y'
            and delete_flag = 'N'
            order by condition_seqno;

					declare continue handler for not found set condition_done=1;

					open condition_cursor;

					condition_loop: loop
						fetch condition_cursor into
              v_source_field_type,
              v_source_field,
              v_extraction_criteria,
              v_comparison_field,
              v_comparison_criteria,
              v_open_parentheses_flag,
              v_close_parentheses_flag,
              v_join_condition;

						if condition_done = 1 then leave condition_loop; end if;

            set v_source_field = concat('a.',ifnull(v_source_field,''));
            set v_extraction_criteria = ifnull(v_extraction_criteria,'');
            set v_index_field = ifnull(v_comparison_field,'');
            set v_comparison_field = concat('b.',ifnull(v_comparison_field,''));
            set v_comparison_criteria = ifnull(v_comparison_criteria,'');
            set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
            set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
            set v_join_condition = ifnull(v_join_condition,'');

            if v_join_condition = '' then
              set v_join_condition = 'and';
            end if;

            set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
            set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

            set v_source_field = fn_get_dsfieldnamecast(v_source_dataset_code,v_source_field);
            set v_comparison_field = fn_get_dsfieldnamecast(v_comparison_dataset_code,v_comparison_field);

            set v_source_field_format = fn_get_fieldfilterformat(v_source_field,v_extraction_criteria,0);

            set v_build_condition = concat(v_open_parentheses_flag,
                                           fn_get_comparisoncondition_ds(in_recon_code,v_source_field_format,v_comparison_dataset_code,v_comparison_field,v_comparison_criteria,0),
                                           v_close_parentheses_flag,' ',
                                           v_join_condition);

            set v_comparison_condition = concat(v_comparison_condition,v_build_condition,' ');
					end loop condition_loop;

					close condition_cursor;
				end condition_block;

        if v_comparison_condition = ' and (' then
          set v_comparison_condition = 'and 1 = 2 ';
        else
          set v_comparison_condition = concat(v_comparison_condition,' 1 = 1) ');
        end if;

        call pr_get_reconstaticvaluesql(v_comparison_condition,'',in_recon_code,'',in_user_code,@v_comparison_condition,@msg22,@result22);
        set v_comparison_condition = @v_comparison_condition;

        -- update
        set v_sql = concat('update ',v_source_table,' as a ');
        set v_sql = concat(v_sql,'inner join ',v_comparison_table,' as b ');
        set v_sql = concat(v_sql,'on 1 = 1 ');
        set v_sql = concat(v_sql,v_comparison_condition);
        set v_sql = concat(v_sql,v_comparison_filter);
        set v_sql = concat(v_sql,'and b.delete_flag = ',char(39),'N',char(39),' ');
        set v_sql = concat(v_sql,'set ',v_ds_update_fields,' ');
        set v_sql = concat(v_sql,'where 1 = 1 ');
        set v_sql = concat(v_sql,v_source_filter);
        set v_sql = concat(v_sql,'and a.delete_flag = ',char(39),'N',char(39),' ');

        call pr_run_sql(v_sql,@msg,@result);
      end if;
    end loop process_loop;

    close process_cursor;
  end process_block;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;