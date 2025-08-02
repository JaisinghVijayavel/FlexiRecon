DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_preprocess_agglookup` $$
CREATE PROCEDURE `pr_run_preprocess_agglookup`(
  in in_recon_code text,
  in in_preprocess_code varchar(32),
  in in_job_gid int,
  in in_postprocess_flag varchar(32),
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    Updated Date : 02-08-2025

    Version : 1
  */

  declare v_recon_version text default '';
  declare v_get_recon_field text default '';
  declare v_set_recon_field text default '';

  declare v_set_dataset_field text default '';

  declare v_cumulative_flag text default '';
  declare v_opening_flag text default '';
  declare v_agg_flag text default '';
  declare v_group_flag text default '';
  declare v_preprocess_code text default '';
  declare v_preprocess_desc text default '';
  declare v_process_method text default '';
  declare v_process_query text default '';
  declare v_process_function text default '';
  declare v_process_expression text default '';
  declare v_process_expression1 text default '';
  declare v_recorderby_type text default '';

  declare v_dataset_db_name text default '';
  declare v_dataset_table text default '';
  declare v_dataset_code text default '';

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
  declare v_dataset_condition text default '';

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
  declare v_value_flag text default '';

  declare v_filter_applied_on text default '';
  declare v_filter_field text default '';
  declare v_filter_criteria text default '';
  declare v_filter_value_flag text default '';
  declare v_filter_value text default '';

  declare v_open_parentheses_flag text default '';
  declare v_close_parentheses_flag text default '';
  declare v_join_condition text default '';

  declare v_recon_field_format text default '';
  declare v_lookup_field_format text default '';
  declare v_build_condition text default '';

  declare v_preprocess_filter text default '';
  declare v_lookup_condition text default '';

  declare v_aggjoin_condition text default '';
  declare v_cumulative_expression text default '';
  declare v_cumulative_variable text default '';
  declare v_value_variable text default '';
  declare v_col128_variable text default '';

  declare v_sysdatetime text default '';

  declare v_sql text default '';
  declare v_tran_sql text default '';
  declare v_tranbrkp_sql text default '';

  declare v_count_sql text default '';

  declare i int default 0;
  declare n int default 0;

  declare v_concurrent_ko_flag text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  drop temporary table if exists recon_tmp_tlookup;

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

  -- set cumulative variable
  set v_sysdatetime = cast(cast(sysdate() as unsigned) as nchar);
  set v_cumulative_variable = concat('@cumulative_value_',v_sysdatetime);

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
        agg_flag,
        group_flag,
        lookup_dataset_code,
        lookup_return_field,
        lookup_group_flag,
        lookup_multi_return_flag,
        lookup_agg_return_function,
        recorderby_type
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
        v_get_recon_field,
        v_set_recon_field,
        v_process_method,
        v_process_query,
        v_process_function,
        v_process_expression,
        v_cumulative_flag,
        v_opening_flag,
        v_agg_flag,
        v_group_flag,
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
      set v_process_function = replace(v_process_function,'@cumulative_value',v_cumulative_variable);

      set v_process_expression = ifnull(v_process_expression,'');

      set v_cumulative_flag = ifnull(v_cumulative_flag,'N');
      set v_opening_flag = ifnull(v_opening_flag,'N');
      set v_agg_flag = ifnull(v_agg_flag,'N');
      set v_group_flag = ifnull(v_group_flag,'N');

      set v_lookup_dataset_code = ifnull(v_lookup_dataset_code,'');
      set v_lookup_return_field = ifnull(v_lookup_return_field,'');
      set v_lookup_group_flag = ifnull(v_lookup_group_flag,'N');
      set v_lookup_multi_return_flag = ifnull(v_lookup_multi_return_flag,'N');
      set v_lookup_agg_return_function = ifnull(v_lookup_agg_return_function,'');
      set v_recorderby_type = ifnull(v_recorderby_type,'asc');

      -- set dataset code
      set v_dataset_code = v_lookup_dataset_code;
      set v_dataset_table = v_dataset_code;

      set v_set_dataset_field = v_lookup_return_field;

      if v_dataset_db_name <> '' then
        set v_dataset_table = concat(v_dataset_db_name,'.',v_lookup_dataset_code);
      end if;

      set v_lookup_table = v_lookup_dataset_code;

      if v_process_method = 'QCD_LOOKUPAGGEXP' then
        set v_process_method = 'LA';
      end if;

      -- Lookup Aggregation
      if v_process_method = 'LA' then
        -- filter condition
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
            from recon_mst_tpreprocessfilterhistory
            where preprocess_code = v_preprocess_code
            and recon_version = v_recon_version
            and filter_applied_on = 'LOOKUP'
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

            if v_filter_value_flag = 'Y' then
              set v_filter_value = fn_get_filtervalue(v_dataset_code,v_filter_value,'');
            end if;

            set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
            set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
            set v_join_condition = ifnull(v_join_condition,'');

            if v_join_condition = '' then
              set v_join_condition = 'and';
            end if;

            if v_filter_field = '' then
              set v_join_condition = '';
              set v_filter_value_flag = '';
              set v_filter_value = '';
            else
              if v_join_condition = '' then
                set v_join_condition = 'and';
              end if;
            end if;

            set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
            set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

            if v_process_method = 'LA' then
              if v_filter_applied_on = 'LOOKUP' then
                set v_filter_field = concat('a.',v_filter_field);
                set v_filter_field = fn_get_dsfieldnamecast(v_dataset_code,v_filter_field);

                set v_lookup_filter = concat(v_lookup_filter,
                                             v_open_parentheses_flag,
                                             fn_get_basefilterformat(v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value_flag,v_filter_value),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition,' ');
              end if;
            end if;
					end loop filter_loop;

					close filter_cursor;
				end filter_block;

        set v_lookup_filter = concat(v_lookup_filter,' 1 = 1) ');
        set v_dataset_condition = v_lookup_filter;

				-- order by field block
        set v_orderby_field = '';

				orderbyfield_block:begin
					declare orderbyfield_done int default 0;

					declare orderbyfield_cursor cursor for
						select
							distinct recorder_field
						from recon_mst_tpreprocessrecorderhistory
						where preprocess_code = v_preprocess_code
            and recon_version = v_recon_version
            and recorder_on = 'LOOKUP' 
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

        if v_orderby_field <> '' then
          set v_orderby_field = concat('order by ',v_orderby_field,',');
        else
          set v_orderby_field = 'order by ';
        end if;

        -- aggregation validation starts
        set v_aggjoin_condition = ' 1 = 1 ';
        set v_grp_field = '';
        set v_idx_grp_field = '';

				-- grp by field block
				grpfield_block:begin
					declare grpfield_done int default 0;

					declare grpfield_cursor cursor for
						select
							distinct grp_field
						from recon_mst_tpreprocessgrpfieldhistory
						where preprocess_code = v_preprocess_code
            and recon_version = v_recon_version
            and grpfield_on = 'LOOKUP'
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

        -- agg temporary tables
        drop temporary table if exists recon_tmp_ttranagg;

				create temporary table recon_tmp_ttranagg select * from recon_tmp_tdatasetstru where 1 = 2;
				alter table recon_tmp_ttranagg ENGINE = MyISAM;
				alter table recon_tmp_ttranagg add agg_gid int not null primary key AUTO_INCREMENT FIRST;
				create index idx_dataset_gid on recon_tmp_ttranagg(dataset_gid);

        if v_group_flag = 'Y' then
          -- idx agg_gid
				  create index idx_col128 on recon_tmp_ttranagg(col128(255));
        end if;

        set v_field_expression = fn_get_expressionformat_ds(v_dataset_code,v_set_dataset_field,
                                                            v_process_expression,false,'');

        -- create index
        if v_grp_field <> '' then
          set v_sql = concat("create index idx_grp_field on recon_tmp_ttranagg(",v_idx_grp_field,")");
          call pr_run_sql2(v_sql,@msg,@result);

          -- insert records in agg table
          set v_sql = concat("insert into recon_tmp_ttranagg(",v_grp_field,",",v_set_dataset_field,",col128)
            select ",v_grp_field,",",v_field_expression,",",v_field_expression," from ",v_dataset_table,"
            where 1 = 1
            ",replace(v_dataset_condition,'a.',''),"
            group by ",v_grp_field,"
            ");

          call pr_run_sql2(v_sql,@msg,@result);
        else
          -- insert records in agg table
          set v_sql = concat("insert into recon_tmp_ttranagg(dataset_gid,",v_set_dataset_field,",col128)
            select dataset_gid,",v_field_expression,",",v_field_expression," from ",v_dataset_table,"
            where 1 = 1
            ",replace(v_dataset_condition,'a.',''),"
            ");

          call pr_run_sql2(v_sql,@msg,@result);
        end if;

        if v_cumulative_flag = 'Y' and v_group_flag = 'N' then
          -- col128 - Agg Value
          set v_cumulative_expression = fn_get_expressionformat_ds(v_dataset_code,
                                                                v_set_dataset_field,
                                                                'col128',
                                                                true,
                                                                v_cumulative_variable);

					set v_sql = 'update recon_tmp_ttranagg set ';
					set v_sql = concat(v_sql,v_set_dataset_field,' = ',v_cumulative_expression,' ');
					set v_sql = concat(v_sql,'order by agg_gid ');

          call pr_run_sql1(concat('set ',v_cumulative_variable,' := 0'),@msgg1,@resultt1);
					call pr_run_sql2(v_sql,@msg,@result);
        end if;

        if v_opening_flag = 'Y' and v_group_flag = 'N' then
          set v_field_expression = concat('cast(',v_set_dataset_field,' as declmal(15,2)',
                               ' - cast(col128 as decimal(15,2))');

          set v_field_expression = fn_get_expressionformat(v_dataset_code,v_set_dataset_field,v_field_expression,false);

					set v_sql = 'update recon_tmp_ttranagg set ';
					set v_sql = concat(v_sql,v_set_dataset_field,' = ',v_field_expression,' ');
					set v_sql = concat(v_sql,'order by agg_gid ');

					call pr_run_sql2(v_sql,@msg,@result);
        end if;

        if v_group_flag = 'Y' then
          -- clear col128
          -- update in dataset table
          set v_sql = concat('update ',v_dataset_table,' ');
          set v_sql = concat(v_sql,'set col128 = null ');
          set v_sql = concat(v_sql,'where 1 = 1 ');
          set v_sql = concat(v_sql,v_dataset_condition);

          call pr_run_sql2(v_sql,@msg,@result);
        end if;

        -- update in dataset table
        set v_sql = concat('update ',v_dataset_table,' as a ');
        set v_sql = concat(v_sql,'inner join recon_tmp_ttranagg as b on ',v_aggjoin_condition,' ');
        set v_sql = concat(v_sql,'set a.',v_set_dataset_field,' = b.',v_set_dataset_field,' ');

        if v_group_flag = 'Y' then
          -- update agg_gid in col128 column to handle grouping fields
          set v_sql = concat(v_sql,',a.col128 = cast(b.agg_gid as nchar) ');
        end if;

        set v_sql = concat(v_sql,'where 1 = 1 ');
        set v_sql = concat(v_sql,v_dataset_condition);

        if v_grp_field = '' then
          set v_sql = concat(v_sql,'and a.dataset_gid = b.dataset_gid ');
        end if;

        call pr_run_sql2(v_sql,@msg,@result);

        -- update group_flag and opening_flag set cases and agg_flag = 'N' cases
        if v_group_flag = 'Y' and v_agg_flag = 'N' and
          (v_opening_flag = 'Y' or v_cumulative_flag = 'Y') then
          drop temporary table if exists recon_tmp_tgid;

          create temporary table recon_tmp_tgid
          (
            dataset_gid int unsigned NOT NULL default 0,
            cumulative_value decimal(15,2) not null default 0,
            opening_value  decimal(15,2) not null default 0,
            agg_gid text,
            PRIMARY KEY (dataset_gid)
          ) ENGINE = MyISAM;

          set v_field_expression = fn_get_expressionformat(v_dataset_code,v_set_dataset_field,v_process_expression,false);

          set v_value_variable = concat("@value_",v_sysdatetime);
          set v_col128_variable = concat("@col128_",v_sysdatetime);

          call pr_run_sql2(concat("set ",v_value_variable," := 0"),@msg22,@result22);
          call pr_run_sql2(concat("set ",v_col128_variable," := ''"),@msg22,@result22);

          -- calculate tran table
          set v_sql = concat("insert into recon_tmp_tgid(dataset_gid,cumulative_value,opening_value,agg_gid)
            select dataset_gid,
              ",v_value_variable," := if(",v_col128_variable,"=col128,",v_value_variable,"+",v_field_expression,",",v_field_expression,"),
              ",v_value_variable,"-",v_field_expression,",
              ",v_col128_variable," := col128
            from ",v_dataset_table,"
            where 1 = 1
            ",replace(v_dataset_condition,'a.',''),"
            ",v_orderby_field,"dataset_gid ",v_recorderby_type,"
            ");

          call pr_run_sql2(v_sql,@msg,@result);

          call pr_run_sql2(concat("set ",v_value_variable," := 0"),@msg22,@result22);
          call pr_run_sql2(concat("set ",v_col128_variable," := ''"),@msg22,@result22);

          -- update value
          if v_opening_flag = 'Y' then
            -- update in tran table
            set v_sql = concat("update ",v_dataset_table ," as a
              inner join recon_tmp_tgid as b on a.dataset_gid = b.dataset_gid
              set a.",v_set_dataset_field," = cast(b.opening_value as nchar)
              ");

            call pr_run_sql2(v_sql,@msg,@result);
          elseif v_cumulative_flag = 'Y' then
            -- update in tran table
            set v_sql = concat("update ",v_dataset_table ," as a
              inner join recon_tmp_tgid as b on a.dataset_gid = b.dataset_gid
                and b.tranbrkp_gid = 0
              set a.",v_set_dataset_field," = cast(b.cumulative_value as nchar)
              ");

            call pr_run_sql2(v_sql,@msg,@result);
          end if;

          drop temporary table if exists recon_tmp_tgid;

          -- clear col128
          -- update in tran table
          set v_sql = concat('update ',v_dataset_table,' ');
          set v_sql = concat(v_sql,'set col128 = null ');
          set v_sql = concat(v_sql,'where delete_flag = ''N''');

          call pr_run_sql2(v_sql,@msg,@result);
        end if;

        -- update group_flag and opening_flag and agg_flag set cases
        if v_group_flag = 'Y' and v_agg_flag = 'Y' and
          (v_opening_flag = 'Y' or v_cumulative_flag = 'Y') then
          drop temporary table if exists recon_tmp_tgid;

          create temporary table recon_tmp_tgid
          (
            dataset_gid int unsigned NOT NULL default 0,
            cumulative_value decimal(15,2) not null default 0,
            opening_value  decimal(15,2) not null default 0,
            agg_gid int unsigned NOT NULL default 0,
            PRIMARY KEY (agg_gid)
          ) ENGINE = MyISAM;

          set v_field_expression = fn_get_expressionformat(v_dataset_code,v_set_dataset_field,v_set_dataset_field,false);

          set v_value_variable = concat("@value_",v_sysdatetime);
          set v_col128_variable = concat("@col128_",v_sysdatetime);

          -- update agg_gid in col128 column
          update recon_tmp_ttranagg set col128 = cast(agg_gid as nchar);

          call pr_run_sql2(concat("set ",v_value_variable," := 0"),@msg22,@result22);
          call pr_run_sql2(concat("set ",v_col128_variable," := ''"),@msg22,@result22);

          -- calculate tran table
          set v_sql = concat("insert into recon_tmp_tgid(cumulative_value,opening_value,agg_gid)
            select
              ",v_value_variable," := ",v_value_variable,"+",v_field_expression,",
              ",v_value_variable,"-",v_field_expression,",
              agg_gid
            from recon_tmp_ttranagg
            where 1 = 1
            order by agg_gid
            ");

          call pr_run_sql2(v_sql,@msg,@result);

          -- update value
          if v_opening_flag = 'Y' then
            -- update in tran table
            set v_sql = concat("update recon_tmp_ttranagg as a
              inner join recon_tmp_tgid as b on a.agg_gid = b.agg_gid
              set a.",v_set_dataset_field," = cast(b.opening_value as nchar)
              ");

            call pr_run_sql2(v_sql,@msg,@result);

						-- update in tran table
						set v_sql = concat('update ',v_dataset_table,' as a ');
						set v_sql = concat(v_sql,'inner join recon_tmp_ttranagg as b on a.col128 = b.col128 ');
						set v_sql = concat(v_sql,'set a.',v_set_dataset_field,' = b.',v_set_dataset_field,' ');

						call pr_run_sql2(v_sql,@msg,@result);
          elseif v_cumulative_flag = 'Y' then
            -- update in tran table
            set v_sql = concat("update recon_tmp_ttranagg as a
              inner join recon_tmp_tgid as b on a.agg_gid = b.agg_gid
              set a.",v_set_dataset_field," = cast(b.cumulative_value as nchar)
              ");

            call pr_run_sql2(v_sql,@msg,@result);

						-- update in tran table
						set v_sql = concat('update ',v_dataset_table,' as a ');
						set v_sql = concat(v_sql,'inner join recon_tmp_ttranagg as b on a.col128 = b.col128 ');
						set v_sql = concat(v_sql,'set a.',v_set_dataset_field,' = b.',v_set_dataset_field,' ');

						call pr_run_sql2(v_sql,@msg,@result);
          end if;

          truncate recon_tmp_tgid;

          -- clear col128
          -- update in tran table
          set v_sql = concat('update ',v_dataset_table,' ');
          set v_sql = concat(v_sql,'set col128 = null ');

          call pr_run_sql2(v_sql,@msg,@result);
        end if;

        drop temporary table if exists recon_tmp_ttranagg;
      end if;
    end loop process_loop;

    close process_cursor;
  end process_block;

  drop temporary table if exists recon_tmp_tlookup;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;