DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_posttranbrkprule` $$
CREATE PROCEDURE `pr_run_posttranbrkprule`
(
  in in_recon_code text,
  in in_rule_code text,
  in in_job_gid int,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_recon_name text default '';
  declare v_recontype_code varchar(32);

  declare v_source_head_sql text default '';
  declare v_comparison_head_sql text default '';

  declare v_sql text default '';
  declare v_tmp_sql text default '';
  declare v_source_sql text default '';
  declare v_comparison_sql text default '';
  declare v_match_sql text default '';
  declare v_trangid_sql text default '';
  declare v_index_sql text default '';

  declare v_source_acc_mode text default '';
  declare v_comparison_acc_mode text default '';

  declare v_rule_code text default '';
  declare v_rule_name text default '';
  declare v_group_flag varchar(32) default '';
  declare v_group_method_flag varchar(32) default '';
  declare v_field_group_flag varchar(32) default '';
  declare v_txt varchar(255) default '';
  declare v_result int default 0;

  declare v_tran_acc_mode char(1) default '';

  declare v_source_field varchar(128) default '';
  declare v_extraction_criteria varchar(32) default '';
  declare v_extraction_filter int default 0;
  declare v_comparison_field varchar(128) default '';
  declare v_comparison_criteria varchar(32) default '';
  declare v_comparison_condition text default '';
  declare v_source_condition text default '';
  declare v_group_field text default '';
  declare v_group_condition text default '';

  declare v_basefilter_condition text default '';
  declare v_source_filter text default '';
  declare v_comparison_filter text default '';

  declare v_sourcebase_filter text default '';
  declare v_comparisonbase_filter text default '';

  declare v_rule_condition text default '';
  declare v_rule_notnull_condition text default '';

  declare v_fieldfilter_format text default '';
  declare v_comparisonfilter_format text default '';
  declare v_rule_groupby text default '';
  declare v_rule_comparison_groupby text default '';

  declare v_tran_gid int default 0;
  declare v_tran_mult tinyint default 0;
  declare v_excp_value double(15,2) default 0;

  declare v_txt_tran_gid text default '';

  declare v_source_dataset_code text default '';
  declare v_comparison_dataset_code text default '';

  declare v_source_tran_gid text default '';
  declare v_comparison_tran_gid text default '';
  declare v_count int default 0;

  declare v_system_match_flag char(1) default null;
  declare v_manual_match_flag char(1) default null;

  declare v_filter_applied_on char(1) default '';
  declare v_filter_field varchar(128) default '';
  declare v_filter_criteria text default '';
  declare v_add_filter int default 0;
  declare v_ident_criteria text default '';
  declare v_ident_value_flag text default '';
  declare v_ident_value text default '';

  declare v_open_parentheses_flag text default '';
  declare v_close_parentheses_flag text default '';
  declare v_join_condition text default '';

  declare v_tran_fields text default '';
  declare v_tranbrkp_fields text default '';

  declare v_grp_field text default '';

  declare v_source_field_org_type text default '';
  declare v_comparison_field_org_type text default '';

  declare v_build_condition text default '';
  declare v_source_field_format text default '';
  declare v_field_format text default '';
  declare v_field text default '';

  declare v_database_name text default '';
  declare v_table_name text default '';
  declare v_index_name text default '';
  declare v_sys_index_name text default '';

  declare v_preview_gid int default 0;

  declare v_system_matchoff char(1) default null;
  declare v_manual_matchoff char(1) default null;

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

	-- set tran table
  /*
	set v_tran_table = concat(in_recon_code,'_tran');
	set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  */

	set v_tran_table = concat('recon_trn_ttran');
	set v_tranbrkp_table = concat('recon_trn_ttranbrkp');

  if in_automatch_flag = 'Y' then
    set v_system_matchoff = 'Y';
  else
    set v_manual_matchoff = 'Y';
  end if;

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (until_active_flag = 'Y' or period_to >= curdate())
    and active_status = 'Y'
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Invalid recon !';
    leave me;
  else
    select
      recon_name,recontype_code
    into
      v_recon_name,v_recontype_code
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_recon_name = ifnull(v_recon_name,'');
    set v_recontype_code = ifnull(v_recontype_code,'');
  end if;

  select database() into v_database_name;

  drop temporary table if exists recon_tmp_t3source;
  drop temporary table if exists recon_tmp_t3comparison;
  drop temporary table if exists recon_tmp_t3match;
  drop temporary table if exists recon_tmp_t3matchdtl;
  drop temporary table if exists recon_tmp_t3pseudorows;
  drop temporary table if exists recon_tmp_t3trangid;
  drop temporary table if exists recon_tmp_t3tranbrkpgid;
  drop temporary table if exists recon_tmp_t3value;
  drop temporary table if exists recon_tmp_t3index;
  drop temporary table if exists recon_tmp_t3sql;

  CREATE TEMPORARY TABLE recon_tmp_t3index(
    table_name varchar(128) not null,
    index_name varchar(128) not null,
    sys_flag char(1) not null default 'N',
    PRIMARY KEY (table_name,index_name),
    key idx_sys_flag(sys_flag)
  ) ENGINE = MyISAM;

  insert into recon_tmp_t3index select 'recon_tmp_t3source','idx_tran_date','Y';
  insert into recon_tmp_t3index select 'recon_tmp_t3comparison','idx_tran_date','Y';

  CREATE temporary TABLE recon_tmp_t3match(
    tran_gid int(10) unsigned NOT NULL,
    tranbrkp_gid int(10) unsigned not null default 0,
    matched_count int not null default 0,
    matched_value double(15,2) not null default 0,
    matched_json json default null,
    scheduler_gid int not null default 0,
    PRIMARY KEY (tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t3matchdtl(
    tran_gid int(10) unsigned NOT NULL,
    tranbrkp_gid int(10) unsigned NOT NULL,
    excp_value double(15,2) not null default 0,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE TEMPORARY TABLE recon_tmp_t3pseudorows(
    row int(10) unsigned NOT NULL,
    PRIMARY KEY (row)
  ) ENGINE = MyISAM;


  insert into recon_tmp_t3pseudorows select 0 union select 1;

  CREATE temporary TABLE recon_tmp_t3trangid(
    tran_gid int(10) unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t3tranbrkpgid(
    tranbrkp_gid int(10) unsigned NOT NULL,
    PRIMARY KEY (tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t3value(
    tran_value double(15,2) unsigned NOT NULL,
    PRIMARY KEY (tran_value)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t3sql(
    sql_gid int(10) unsigned NOT NULL AUTO_INCREMENT,
    sql_query text default null,
    PRIMARY KEY (sql_gid)
  ) ENGINE = MyISAM;

  if in_rule_code = '' then set in_rule_code = null; end if;

  select
    group_concat(field_name)
  into
    v_tran_fields
  from recon_mst_ttablestru
  where table_name = 'recon_trn_ttranwithbrkp'
  and field_name <> 'tranbrkp_gid'
  and delete_flag = 'N'
  order by display_order;

  select
    group_concat(field_name)
  into
    v_tranbrkp_fields
  from recon_mst_ttablestru
  where table_name = 'recon_trn_ttranwithbrkp'
  and field_name <> 'tran_gid'
  and delete_flag = 'N'
  order by display_order;

  applyrule_block:begin
    declare applyrule_done int default 0;
    declare applyrule_cursor cursor for
      select
        a.rule_code,
        a.rule_name,
        a.source_dataset_code,
        a.source_acc_mode,
        a.comparison_dataset_code,
        a.comparison_acc_mode,
        a.group_flag,
        a.group_method_flag
      from recon_mst_trule as a
      where a.recon_code = in_recon_code
      and a.rule_code = in_rule_code 
      and a.period_from <= curdate()
      and (a.until_active_flag = 'Y'
      or a.period_to >= curdate())
      and a.rule_apply_on = 'S'
      and a.active_status = 'Y'
      and a.system_match_flag = ifnull(v_system_matchoff,a.system_match_flag)
      and a.manual_match_flag = ifnull(v_manual_matchoff,a.manual_match_flag)
      and a.delete_flag = 'N';
    declare continue handler for not found set applyrule_done=1;

    open applyrule_cursor;

    applyrule_loop: loop
      fetch applyrule_cursor into v_rule_code,v_rule_name,
                                  v_source_dataset_code,v_source_acc_mode,
                                  v_comparison_dataset_code,v_comparison_acc_mode,
                                  v_group_flag,v_group_method_flag;

      if applyrule_done = 1 then leave applyrule_loop; end if;

      set v_rule_code = ifnull(v_rule_code,0);
      set v_rule_name = ifnull(v_rule_name,'');

      set v_source_dataset_code = ifnull(v_source_dataset_code,'');
      set v_comparison_dataset_code = ifnull(v_comparison_dataset_code,'');

      set v_source_acc_mode = ifnull(v_source_acc_mode,'');
      set v_comparison_acc_mode = ifnull(v_comparison_acc_mode,'');

      if v_recontype_code = 'V' then
        set v_source_acc_mode = 'V';
      end if;

      set v_tran_acc_mode = v_source_acc_mode;

      if v_tran_acc_mode = 'D' then
        set v_tran_mult = -1;
      else
        set v_tran_mult = 1;
      end if;

      if v_group_flag = 'OTO' then
        set v_group_flag = 'N';
      elseif v_group_flag = 'OTM' then
        set v_group_flag = 'Y';
      elseif v_group_flag = 'MTM' then
        set v_group_flag = 'Y';
      end if;

      set v_group_flag = ifnull(v_group_flag,'N');
      set v_group_method_flag = ifnull(v_group_method_flag,'N');

      set v_txt = concat('Applying Rule - ',v_rule_name);

      call pr_upd_job(in_job_gid,'P',v_txt,@msg,@result);

      set v_source_head_sql = concat('insert into recon_tmp_t3source (',v_tran_fields,',scheduler_gid,excp_mult_value) ');

      if in_automatch_flag = 'Y' then
        set v_source_head_sql = concat(v_source_head_sql,' select ',v_tran_fields ,',scheduler_gid,excp_value*tran_mult from ',v_tran_table,' ');
      else
        set v_source_head_sql = concat(v_source_head_sql,' select ',v_tran_fields ,',scheduler_gid,excp_value*tran_mult from recon_tmp_ttran ');
      end if;

      set v_source_head_sql = concat(v_source_head_sql,' where recon_code = ',char(39),in_recon_code,char(39));
      set v_source_head_sql = concat(v_source_head_sql,' and dataset_code = ',char(39),v_source_dataset_code,char(39),' ');
      set v_source_head_sql = concat(v_source_head_sql,' and excp_value > 0 and excp_value = tran_value and mapped_value = 0 ');

      set v_comparison_head_sql = concat('insert into recon_tmp_t3comparison (',v_tranbrkp_fields,',scheduler_gid,excp_mult_value) ');

      if in_automatch_flag = 'Y' then
        set v_comparison_head_sql = concat(v_comparison_head_sql,' select ',v_tranbrkp_fields ,',scheduler_gid,excp_value*tran_mult from ',v_tranbrkp_table,' ');
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' select ',v_tranbrkp_fields ,',scheduler_gid,excp_value*tran_mult from recon_tmp_ttranbrkp ');
      end if;

      set v_comparison_head_sql = concat(v_comparison_head_sql,' where recon_code = ',char(39),in_recon_code,char(39));
      set v_comparison_head_sql = concat(v_comparison_head_sql,' and dataset_code = ',char(39),v_source_dataset_code,char(39),' ');
      set v_comparison_head_sql = concat(v_comparison_head_sql,' and tranbrkp_dataset_code = ',char(39),v_comparison_dataset_code,char(39),' ');
      set v_comparison_head_sql = concat(v_comparison_head_sql,' and excp_value > 0 and tran_gid = 0 ');

          basefilter_block:begin
            declare basefilter_done int default 0;
            declare basefilter_cursor cursor for
            select
              filter_applied_on,filter_field,filter_criteria,add_filter,ident_criteria,
              ident_value_flag,ident_value,
              open_parentheses_flag,close_parentheses_flag,join_condition
            from recon_mst_truleselefilter
            where rule_code = v_rule_code
            and active_status = 'Y'
            and delete_flag = 'N'
            order by filter_applied_on,ruleselefilter_seqno,ruleselefilter_gid;

            declare continue handler for not found set basefilter_done=1;

            open basefilter_cursor;

            set v_sourcebase_filter = ' and (';
            set v_comparisonbase_filter = ' and (';

            basefilter_loop: loop
              fetch basefilter_cursor into v_filter_applied_on,v_filter_field,
                                    v_filter_criteria,v_add_filter,v_ident_criteria,
                                    v_ident_value_flag,v_ident_value,
                                    v_open_parentheses_flag,v_close_parentheses_flag,
                                    v_join_condition;
              if basefilter_done = 1 then leave basefilter_loop; end if;

              set v_ident_value_flag = ifnull(v_ident_value_flag,'Y');
              set v_ident_value = ifnull(v_ident_value,'');

              set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
              set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
              set v_join_condition = ifnull(v_join_condition,'');

              if v_join_condition = '' then
                set v_join_condition = 'and';
              end if;

              set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
              set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

              set v_basefilter_condition = concat(v_open_parentheses_flag,
                                                  fn_get_basefilterformat(v_filter_field,v_filter_criteria,v_add_filter,v_ident_criteria,v_ident_value_flag,v_ident_value),
                                                  v_close_parentheses_flag,
                                                  ' ',v_join_condition,' ');

              if v_filter_applied_on = 'S' then
                set v_sourcebase_filter = concat(v_sourcebase_filter,v_basefilter_condition);
              elseif v_filter_applied_on = 'C' then
                set v_comparisonbase_filter = concat(v_comparisonbase_filter,v_basefilter_condition);
              end if;
            end loop basefilter_loop;

            close basefilter_cursor;
          end basefilter_block;

          set v_sourcebase_filter = concat(v_sourcebase_filter,' 1 = 1) ');
          set v_comparisonbase_filter = concat(v_comparisonbase_filter,' 1 = 1) ');

          set v_rule_condition = ' and ';
          set v_rule_notnull_condition = ' and (1 = 1 ';
          set v_rule_groupby = '';

          set v_source_condition = ' and ';
          set v_comparison_condition = ' and ';

          drop temporary table if exists recon_tmp_t3source;
          drop temporary table if exists recon_tmp_t3comparison;

          /*
          drop table if exists recon_tmp_t3source;
          drop table if exists recon_tmp_t3comparison;
          */

          create temporary table recon_tmp_t3source select * from recon_trn_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_t3source add primary key(tran_gid);
          create index idx_recon_code on recon_tmp_t3source(recon_code);
          create index idx_excp_value on recon_tmp_t3source(recon_code,dataset_code,excp_value);
          create index idx_excp_mult_value on recon_tmp_t3source(excp_mult_value);
          create index idx_tran_date on recon_tmp_t3source(tran_date);
          create index idx_dataset_code on recon_tmp_t3source(recon_code,dataset_code,tran_acc_mode);
          alter table recon_tmp_t3source ENGINE = MyISAM;

          create temporary table recon_tmp_t3comparison select * from recon_trn_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_t3comparison add primary key(tranbrkp_gid);
          create index idx_recon_code on recon_tmp_t3comparison(recon_code);
          create index idx_excp_value on recon_tmp_t3comparison(recon_code,dataset_code,excp_value);
          create index idx_excp_mult_value on recon_tmp_t3comparison(excp_mult_value);
          create index idx_tran_date on recon_tmp_t3comparison(tran_date);
          create index idx_tran_gid on recon_tmp_t3comparison(tran_gid);
          create index idx_dataset_code on recon_tmp_t3comparison(recon_code,dataset_code,tran_acc_mode);
          alter table recon_tmp_t3comparison ENGINE = MyISAM;

          delete from recon_tmp_t3index where index_name <> 'idx_tran_date';

          rule_block:begin
            declare rule_done int default 0;
            declare rule_cursor cursor for
            select
              a.source_field,a.extraction_criteria,a.extraction_filter,
              a.comparison_field,a.comparison_criteria,a.comparison_filter,
              a.open_parentheses_flag,a.close_parentheses_flag,
              a.join_condition
            from recon_mst_trulecondition as a
            where a.rule_code = v_rule_code
            and a.active_status = 'Y'
            and a.delete_flag = 'N'
            order by rulecondition_seqno,rulecondition_gid;

            declare continue handler for not found set rule_done=1;

            open rule_cursor;

            rule_loop: loop
              fetch rule_cursor into v_source_field,v_extraction_criteria,v_extraction_filter,
                                     v_comparison_field,v_comparison_criteria,v_comparison_filter,
                                     v_open_parentheses_flag,v_close_parentheses_flag,v_join_condition;

              if rule_done = 1 then leave rule_loop; end if;


              set v_index_name = concat('idx_',v_source_field);

              if not exists(select index_name from recon_tmp_t3index
                            WHERE table_name = 'recon_tmp_t3source'
                            and index_name = v_index_name) then

                if subtr(v_source_field,1,3) = 'col' then
                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_t3source(',v_source_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_t3source(',v_source_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_t3index(table_name,index_name) select 'recon_tmp_t3source',v_index_name;
              end if;

              set v_index_name = concat('idx_',v_comparison_field);

              if not exists(select index_name from recon_tmp_t3index
                            WHERE table_name = 'recon_tmp_t3comparison'
                            and index_name = v_index_name) then

                if substr(v_comparison_field,1,3) = 'col' then
                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_t3comparison(',v_comparison_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_t3comparison(',v_comparison_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_t3index(table_name,index_name) select 'recon_tmp_t3comparison',v_index_name;
              end if;

              set v_source_field_org_type = fn_get_fieldorgtype(in_recon_code,v_source_field);
              set v_comparison_field_org_type = fn_get_fieldorgtype(in_recon_code,v_comparison_field);

              -- fetch field

              set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
              set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
              set v_join_condition = ifnull(v_join_condition,'');
              if v_join_condition = '' then set v_join_condition = 'and'; end if;

              set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
              set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

              set v_source_field = ifnull(v_source_field,'');
              set v_extraction_criteria = ifnull(v_extraction_criteria,'');
              set v_extraction_filter = ifnull(v_extraction_filter,0);
              set v_comparison_field = ifnull(v_comparison_field,'');
              set v_comparison_criteria = ifnull(v_comparison_criteria,'');
              set v_comparison_filter = ifnull(v_comparison_filter,0);

              -- source condition
              set v_source_condition = concat(v_source_condition,' ',v_open_parentheses_flag);

              if v_source_field_org_type = 'TEXT' then
                set v_source_condition = concat(v_source_condition,' ',v_source_field ,' <> '''' ');
              else
                set v_source_condition = concat(v_source_condition,' ',v_source_field ,' is not null ');
              end if;

              set v_source_condition = concat(v_source_condition,' ',v_close_parentheses_flag);
              set v_source_condition = concat(v_source_condition,' ',v_join_condition);

              -- comparison condition
              set v_comparison_condition = concat(v_comparison_condition,' ',v_open_parentheses_flag);

              if v_comparison_field_org_type = 'TEXT' then
                set v_comparison_condition = concat(v_comparison_condition,' ',v_comparison_field ,' <> '''' ');
              else
                set v_comparison_condition = concat(v_comparison_condition,' ',v_comparison_field ,' is not null ');
              end if;

              set v_comparison_condition = concat(v_comparison_condition,' ',v_close_parentheses_flag);
              set v_comparison_condition = concat(v_comparison_condition,' ',v_join_condition);

              set v_source_field = ifnull(concat('a.',v_source_field),'');
              set v_comparison_field = ifnull(concat('b.',v_comparison_field),'');

              if (instr(v_extraction_criteria,'$FIELD$') > 0 or v_extraction_filter > 0)
                and v_open_parentheses_flag <> '('
                and v_join_condition <> 'OR'
                and v_close_parentheses_flag <> ')' then

                set v_field = replace(v_source_field,'a.','');
                set v_field_format = fn_get_fieldfilterformat(v_field,v_extraction_criteria,v_extraction_filter);

                set v_sql = '';
                set v_sql = concat(v_sql,'update recon_tmp_t3source set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format);

                insert into recon_tmp_t3sql(sql_query) values (v_sql);

                set v_extraction_criteria = 'EXACT';
                set v_extraction_filter = 0;
              end if;

              if (instr(v_comparison_criteria,'$FIELD$') > 0 or v_comparison_filter > 0)
                and v_open_parentheses_flag <> '('
                and v_join_condition <> 'OR'
                and v_close_parentheses_flag <> ')' then

                set v_field = replace(v_comparison_field,'b.','');
                set v_field_format = fn_get_fieldfilterformat(v_field,v_comparison_criteria,v_comparison_filter);

                set v_sql = '';
                set v_sql = concat(v_sql,'update recon_tmp_t3comparison set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format);

                insert into recon_tmp_t3sql(sql_query) values (v_sql);

                set v_comparison_criteria = 'EXACT';
                set v_comparison_filter = 0;
              end if;

              set v_source_field_format = fn_get_fieldfilterformat(v_source_field,v_extraction_criteria,v_extraction_filter);

              set v_build_condition = concat(v_open_parentheses_flag,
                                             fn_get_comparisoncondition(in_recon_code,v_source_field_format,v_comparison_field,v_comparison_criteria,v_comparison_filter),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition);

              set v_rule_condition = concat(v_rule_condition,' ',v_build_condition,' ');

              if v_source_field_org_type = 'TEXT' then
                set v_rule_notnull_condition = concat(v_rule_notnull_condition,'or ',v_source_field ,' <> '''' ');
              else
                set v_rule_notnull_condition = concat(v_rule_notnull_condition,'or ',v_source_field ,' is not null ');
              end if;

              if v_comparison_field_org_type = 'TEXT' then
                set v_rule_notnull_condition = concat(v_rule_notnull_condition,'or ',v_comparison_field ,' <> '''' ');
              else
                set v_rule_notnull_condition = concat(v_rule_notnull_condition,'or ',v_comparison_field ,' is not null ');
              end if;

              set v_rule_groupby = concat(v_rule_groupby,',',v_source_field);
              set v_rule_comparison_groupby = concat(v_rule_comparison_groupby,',',v_comparison_field);
            end loop rule_loop;

            close rule_cursor;
          end rule_block;

          truncate recon_tmp_t3source;
          truncate recon_tmp_t3comparison;

          if v_source_condition = ' and ' or v_comparison_condition = ' and ' then
            set v_source_condition = ' and 1 = 2 ';
            set v_comparison_condition  = ' and 1 = 2 ';
            set v_rule_condition = ' and 1 = 2 ';
            set v_rule_groupby = ',tran_gid';
          else
            set v_source_condition = concat(v_source_condition,' 1 = 1 ');
            set v_comparison_condition  = concat(v_comparison_condition,' 1 = 1 ');
            set v_rule_condition = concat(v_rule_condition,' 1 = 1 ');
          end if;

          set v_rule_notnull_condition = concat(v_rule_notnull_condition,') ');
          set v_rule_condition = concat(v_rule_condition,v_rule_notnull_condition);

          set v_source_sql = concat(v_source_head_sql,' and tran_date >= ',char(39),in_period_from,char(39));
          set v_source_sql = concat(v_source_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          set v_source_sql = concat(v_source_sql,' and tran_acc_mode = ',char(39),v_tran_acc_mode,char(39));
          set v_source_sql = concat(v_source_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_source_sql = concat(v_source_sql,' ',v_source_condition,' ',v_sourcebase_filter);

          call pr_run_sql(v_source_sql,@result,@msg);

          set v_comparison_sql = concat(v_comparison_head_sql,' and tran_date >= ',char(39),in_period_from,char(39));
          set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));

          if v_comparison_acc_mode <> 'B' or v_group_flag = 'N' then
            set v_comparison_sql = concat(v_comparison_sql,' and tran_acc_mode = ',char(39),v_tran_acc_mode,char(39));
          end if;

          set v_comparison_sql = concat(v_comparison_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_comparison_sql = concat(v_comparison_sql,' ',v_comparison_condition,' ',v_comparisonbase_filter);

          call pr_run_sql(v_comparison_sql,@result,@msg);

          sql_block:begin
            declare sql_done int default 0;
            declare sql_cursor cursor for
            select sql_query from recon_tmp_t3sql;
            declare continue handler for not found set sql_done=1;

            open sql_cursor;

            sql_loop: loop
              fetch sql_cursor into v_sql;
              if sql_done = 1 then leave sql_loop; end if;

              call pr_run_sql(v_sql,@result,@msg);
            end loop sql_loop;
            close sql_cursor;
          end sql_block;

          truncate recon_tmp_t3sql;

          if v_group_flag = 'N' then

            truncate recon_tmp_t3value;

            insert into recon_tmp_t3value select distinct excp_value from recon_tmp_t3source;

            delete from recon_tmp_t3comparison where excp_value not in (select tran_value from recon_tmp_t3value);

            truncate recon_tmp_t3value;


            set v_match_sql = 'insert into recon_tmp_t3match (tran_gid,tranbrkp_gid,matched_count,matched_value,scheduler_gid) ';
            set v_match_sql = concat(v_match_sql,'select m.tran_gid,m.tranbrkp_gid,m.matched_count,m.matched_value,m.scheduler_gid from (');
            set v_match_sql = concat(v_match_sql,'select a.tran_gid,b.tranbrkp_gid,a.excp_value,a.tran_mult,b.scheduler_gid,1 as matched_count,a.excp_value as matched_value ');
            set v_match_sql = concat(v_match_sql,'from recon_tmp_t3source as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t3comparison as b ');
            set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');
            set v_match_sql = concat(v_match_sql,'and a.dataset_code = b.dataset_code ');
            set v_match_sql = concat(v_match_sql,'and a.excp_value = b.excp_value ');
            set v_match_sql = concat(v_match_sql,'and a.tran_acc_mode = b.tran_acc_mode ');
            set v_match_sql = concat(v_match_sql,v_rule_condition,') as m ');

            call pr_run_sql(v_match_sql,@msg,@result);

            set v_sql = '';
            set v_sql = concat(v_sql,'update recon_tmp_t3match set matched_json = ');
            set v_sql = concat(v_sql,'cast(concat(',char(39),'[');
            set v_sql = concat(v_sql,'{');
            set v_sql = concat(v_sql,'"tran_gid":',char(39),',cast(tran_gid as nchar),',char(39),',');
            set v_sql = concat(v_sql,'"tranbrkp_gid":0,');
            set v_sql = concat(v_sql,'"tran_mult":',cast(v_tran_mult as nchar),',');
            set v_sql = concat(v_sql,'"src_comp_flag":"S",');
            set v_sql = concat(v_sql,'"excp_value":', char(39),',cast(matched_value as nchar),',char(39));
            set v_sql = concat(v_sql,'},');
            set v_sql = concat(v_sql,'{');
            set v_sql = concat(v_sql,'"tran_gid":',char(39),',cast(tran_gid as nchar),',char(39),',');
            set v_sql = concat(v_sql,'"tranbrkp_gid":',char(39),',cast(tranbrkp_gid as nchar),',char(39),',');
            set v_sql = concat(v_sql,'"tran_mult":',cast(v_tran_mult as nchar),',');
            set v_sql = concat(v_sql,'"src_comp_flag":"C",');
            set v_sql = concat(v_sql,'"excp_value":',char(39),',cast(matched_value as nchar),',char(39));
            set v_sql = concat(v_sql,'}');
            set v_sql = concat(v_sql,']',char(39),') as json)');

            call pr_run_sql(v_sql,@msg,@result);
          else
            -- get target addtional group field
            select group_concat(concat('b.',grp_field)) into v_grp_field from recon_mst_trulegrpfield
            where rule_code = v_rule_code
            and active_status = 'Y'
            and delete_flag = 'N';

            set v_grp_field = ifnull(v_grp_field,'');

            if v_grp_field <> '' then
              set v_rule_groupby = concat(v_rule_groupby,',',v_grp_field);
            end if;

            -- match the record(s) added scheduler_gid
            set v_match_sql = 'insert into recon_tmp_t3match (tran_gid,matched_count,matched_value,matched_json,scheduler_gid) ';
            set v_match_sql = concat(v_match_sql,'select m.tran_gid,m.matched_count,m.matched_value,m.matched_json,m.scheduler_gid from (');
            set v_match_sql = concat(v_match_sql,'select a.tran_gid,a.excp_value,a.excp_mult_value,a.tran_mult,b.scheduler_gid,count(*) as matched_count,a.excp_value as matched_value,');

            set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[');
            set v_match_sql = concat(v_match_sql,'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":0,');
            set v_match_sql = concat(v_match_sql,'"tran_mult":', char(39),',cast(a.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"excp_value":', char(39),',cast(a.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'},',char(39),',');
            set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tran_mult":', char(39),',cast(b.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"excp_value":',char(39),',cast(b.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'}',char(39),'),');
            set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json) as matched_json ');

            set v_match_sql = concat(v_match_sql,'from recon_tmp_t3source as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t3comparison as b ');
            set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');
            set v_match_sql = concat(v_match_sql,'and a.dataset_code = b.dataset_code ');
            -- set v_match_sql = concat(v_match_sql,'and a.tran_acc_mode = b.tran_acc_mode ');

            if v_comparison_acc_mode <> 'B' then
              set v_match_sql = concat(v_match_sql,'and a.tran_acc_mode = b.tran_acc_mode ');
            end if;

            set v_match_sql = concat(v_match_sql,v_rule_condition,' ');
            set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.excp_value,a.excp_mult_value,a.tran_mult,b.scheduler_gid',v_rule_groupby,' ');
            set v_match_sql = concat(v_match_sql,'having true ');
            set v_match_sql = concat(v_match_sql,'and count(*) > 1 ');
            set v_match_sql = concat(v_match_sql,'and a.excp_mult_value = sum(b.excp_mult_value)) as m');

            call pr_run_sql(v_match_sql,@msg,@result);

            -- select v_match_sql;
            -- leave me;

            -- remove the matched tran_gid from the source
            truncate recon_tmp_t3trangid;

            insert into recon_tmp_t3trangid
              select
                distinct tran_gid
              FROM recon_tmp_t3match;

            delete a.* from recon_tmp_t3source as a
            where a.tran_gid in (select b.tran_gid from recon_tmp_t3trangid as b where a.tran_gid = b.tran_gid);

            truncate recon_tmp_t3tranbrkpgid;

            truncate recon_tmp_t3pseudorows;
            select max(matched_count) into v_count from recon_tmp_t3match;
            set v_count = ifnull(v_count,0);

            insert into recon_tmp_t3pseudorows select row from pseudo_rows1 where row <= v_count;

            -- remove the matched tranbrkp_gid from the comparison
            insert into recon_tmp_t3tranbrkpgid
              select
                distinct JSON_UNQUOTE(JSON_EXTRACT(matched_json, CONCAT('$[', recon_tmp_t3pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid
              FROM recon_tmp_t3match
              JOIN recon_tmp_t3pseudorows
              having tranbrkp_gid IS NOT NULL;

            delete a.* from recon_tmp_t3comparison as a
            where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_t3tranbrkpgid as b where a.tranbrkp_gid = b.tranbrkp_gid);

            -- match the record(s) removed scheduler_gid
            set v_match_sql = 'insert into recon_tmp_t3match (tran_gid,matched_count,matched_value,matched_json,scheduler_gid) ';
            set v_match_sql = concat(v_match_sql,'select m.tran_gid,m.matched_count,m.matched_value,m.matched_json,m.scheduler_gid from (');
            set v_match_sql = concat(v_match_sql,'select a.tran_gid,a.excp_value,a.excp_mult_value,a.tran_mult,0 as scheduler_gid,count(*) as matched_count,a.excp_value as matched_value,');

            set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[');
            set v_match_sql = concat(v_match_sql,'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":0,');
            set v_match_sql = concat(v_match_sql,'"tran_mult":', char(39),',cast(a.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"excp_value":', char(39),',cast(a.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'},',char(39),',');
            set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tran_mult":', char(39),',cast(b.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"excp_value":',char(39),',cast(b.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'}',char(39),'),');
            set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json) as matched_json ');

            set v_match_sql = concat(v_match_sql,'from recon_tmp_t3source as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t3comparison as b ');
            set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');
            set v_match_sql = concat(v_match_sql,'and a.dataset_code = b.dataset_code ');

            if v_comparison_acc_mode <> 'B' then
              set v_match_sql = concat(v_match_sql,'and a.tran_acc_mode = b.tran_acc_mode ');
            end if;

            set v_match_sql = concat(v_match_sql,v_rule_condition,' ');
            set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.excp_value,a.excp_mult_value,a.tran_mult',v_rule_groupby,' ');
            set v_match_sql = concat(v_match_sql,'having true ');
            set v_match_sql = concat(v_match_sql,'and count(*) > 1 ');
            set v_match_sql = concat(v_match_sql,'and a.excp_mult_value = sum(b.excp_mult_value)) as m');

            call pr_run_sql(v_match_sql,@msg,@result);

            -- remove the matched tran_gid from the source
            truncate recon_tmp_t3trangid;

            insert into recon_tmp_t3trangid
              select
                distinct tran_gid
              FROM recon_tmp_t3match;

            delete a.* from recon_tmp_t3source as a
            where a.tran_gid in (select b.tran_gid from recon_tmp_t3trangid as b where a.tran_gid = b.tran_gid);

            truncate recon_tmp_t3trangid;

            -- remove the matched tranbrkp_gid from the comparison
            truncate recon_tmp_t3tranbrkpgid;

            truncate recon_tmp_t3pseudorows;
            select max(matched_count) into v_count from recon_tmp_t3match;
            set v_count = ifnull(v_count,0);

            insert into recon_tmp_t3pseudorows select row from pseudo_rows1 where row <= v_count;

            insert into recon_tmp_t3tranbrkpgid
              select
                distinct JSON_UNQUOTE(JSON_EXTRACT(matched_json, CONCAT('$[', recon_tmp_t3pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid
              FROM recon_tmp_t3match
              JOIN recon_tmp_t3pseudorows
              having tranbrkp_gid IS NOT NULL;

            delete a.* from recon_tmp_t3comparison as a
            where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_t3tranbrkpgid as b where a.tranbrkp_gid = b.tranbrkp_gid);

            truncate recon_tmp_t3tranbrkpgid;

            -- one to one match
            set v_match_sql = 'insert into recon_tmp_t3match (tran_gid,matched_count,matched_value,matched_json,scheduler_gid) ';
            set v_match_sql = concat(v_match_sql,'select m.tran_gid,m.matched_count,m.matched_value,m.matched_json,m.scheduler_gid from (');
            set v_match_sql = concat(v_match_sql,'select a.tran_gid,a.excp_value,a.excp_mult_value,a.tran_mult,b.scheduler_gid,count(*) as matched_count,a.excp_value as matched_value,');

            set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[');
            set v_match_sql = concat(v_match_sql,'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":0,');
            set v_match_sql = concat(v_match_sql,'"tran_mult":', char(39),',cast(a.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"excp_value":', char(39),',cast(a.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'},',char(39),',');
            set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tran_mult":', char(39),',cast(b.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"excp_value":',char(39),',cast(b.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'}',char(39),'),');
            set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json) as matched_json ');

            set v_match_sql = concat(v_match_sql,'from recon_tmp_t3source as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t3comparison as b ');
            set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');
            set v_match_sql = concat(v_match_sql,'and a.dataset_code = b.dataset_code ');
            set v_match_sql = concat(v_match_sql,'and a.tran_acc_mode = b.tran_acc_mode ');
            set v_match_sql = concat(v_match_sql,v_rule_condition,' ');
            set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.excp_value,a.excp_mult_value,a.tran_mult,b.scheduler_gid',v_rule_groupby,' ');
            set v_match_sql = concat(v_match_sql,'having true ');
            set v_match_sql = concat(v_match_sql,'and count(*) = 1 ');
            set v_match_sql = concat(v_match_sql,'and a.excp_mult_value = sum(b.excp_mult_value)) as m');

            call pr_run_sql(v_match_sql,@msg,@result);
          end if;

          truncate recon_tmp_t3pseudorows;
          select max(matched_count) into v_count from recon_tmp_t3match;
          set v_count = ifnull(v_count,0);

          insert into recon_tmp_t3pseudorows select row from pseudo_rows1 where row <= v_count;

          -- remove the duplicate by tran_gid in the match
          truncate recon_tmp_t3trangid;

          insert into recon_tmp_t3trangid
            select tran_gid FROM recon_tmp_t3match
            group by tran_gid
            having count(*) > 1;

          delete a.* from recon_tmp_t3match as a
          where a.tran_gid in (select b.tran_gid from recon_tmp_t3trangid as b where a.tran_gid = b.tran_gid);

          truncate recon_tmp_t3trangid;


          insert into recon_tmp_t3matchdtl
          ( tran_gid,tranbrkp_gid,excp_value)
          select
            JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t3match.matched_json, CONCAT('$[', recon_tmp_t3pseudorows.row, '].tran_gid'))) AS tran_gid,
            JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t3match.matched_json, CONCAT('$[', recon_tmp_t3pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
            JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t3match.matched_json, CONCAT('$[', recon_tmp_t3pseudorows.row, '].excp_value'))) AS excp_value
          FROM recon_tmp_t3match
          JOIN recon_tmp_t3pseudorows
          HAVING tran_gid IS NOT NULL;

          -- remove the duplicate by tranbrkp_gid in the matchdtl
          truncate recon_tmp_t3trangid;
          truncate recon_tmp_t3tranbrkpgid;

          insert into recon_tmp_t3tranbrkpgid
            select tranbrkp_gid FROM recon_tmp_t3matchdtl
            where tranbrkp_gid > 0
            group by tranbrkp_gid
            having count(*) > 1;

          insert into recon_tmp_t3trangid
            select distinct tran_gid FROM recon_tmp_t3matchdtl as a
            where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_t3tranbrkpgid as b
              where a.tranbrkp_gid = b.tranbrkp_gid);

          delete a.* from recon_tmp_t3match as a
          where a.tran_gid in (select b.tran_gid from recon_tmp_t3trangid as b where a.tran_gid = b.tran_gid);

          delete a.* from recon_tmp_t3matchdtl as a
          where a.tran_gid in (select b.tran_gid from recon_tmp_t3trangid as b where a.tran_gid = b.tran_gid);

          truncate recon_tmp_t3trangid;
          truncate recon_tmp_t3tranbrkpgid;

          if in_automatch_flag = 'Y' then
						set v_sql = concat("
							update ",v_tran_table," as a
							inner join recon_tmp_t3matchdtl as b on a.tran_gid = b.tran_gid and a.excp_value= b.excp_value and b.tranbrkp_gid = 0
							set a.mapped_value = a.excp_value
							where a.excp_value > 0
							and a.mapped_value = 0
							and a.delete_flag = 'N'");
							
						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("
							update ",v_tranbrkp_table," as a
							inner join recon_tmp_t3matchdtl as b on a.tranbrkp_gid = b.tranbrkp_gid and a.excp_value= b.excp_value and b.tran_gid > 0
							set a.tran_gid = b.tran_gid,
									a.posted_job_gid = ",cast(in_job_gid as nchar),",
									a.mapped_value = a.tran_value 
							where a.excp_value > 0
							and a.tran_gid = 0
							and a.delete_flag = 'N'");
							
						call pr_run_sql(v_sql,@msg,@result);
          else
            -- update in temporary table
            update recon_tmp_ttran as a
            inner join recon_tmp_t3matchdtl as b on a.tran_gid = b.tran_gid and a.excp_value= b.excp_value and b.tranbrkp_gid = 0
            set a.mapped_value = a.excp_value
            where a.excp_value > 0
            and a.mapped_value = 0
            and a.delete_flag = 'N';

            update recon_tmp_ttranbrkp as a
            inner join recon_tmp_t3matchdtl as b on a.tranbrkp_gid = b.tranbrkp_gid and a.excp_value= b.excp_value and b.tran_gid > 0
            set a.tran_gid = b.tran_gid,
                a.posted_job_gid = in_job_gid
            where a.excp_value > 0
            and a.tran_gid = 0
            and a.delete_flag = 'N';

            -- move to preview table
            select max(preview_gid) into v_preview_gid from recon_trn_tpreview
            where job_gid = in_job_gid
            and delete_flag = 'N';

            set v_preview_gid = ifnull(v_preview_gid,0);
            set @preview_gid = v_preview_gid;

            insert into recon_trn_tpreview
            (
              preview_gid,job_gid,preview_date,preview_value,recon_code,rule_code,
              previewdtl_json,previewdtl_post_flag,insert_date,insert_by
            )
            select
              @preview_gid:=@preview_gid+1,in_job_gid,sysdate(),matched_value,in_recon_code,
              v_rule_code,matched_json,'N',sysdate(),in_user_code
            from recon_tmp_t3match;

            insert into recon_trn_tpreviewdtl
            ( previewdtl_gid,preview_gid,job_gid,tran_gid,tranbrkp_gid,excp_value,tran_mult,src_comp_flag)
            select
              recon_tmp_t3pseudorows.row+1,
              preview_gid,
              job_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t3pseudorows.row, '].tran_gid'))) AS tran_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t3pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t3pseudorows.row, '].excp_value'))) AS excp_value,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t3pseudorows.row, '].tran_mult'))) AS tran_mult,
              'C'
            FROM recon_trn_tpreview
            JOIN recon_tmp_t3pseudorows
            where job_gid = in_job_gid
            and previewdtl_post_flag = 'N'
            HAVING tran_gid IS NOT NULL;

            update recon_trn_tpreviewdtl
            set src_comp_flag = 'S'
            where job_gid = in_job_gid
            and tran_gid > 0
            and tranbrkp_gid = 0
            and delete_flag = 'N';

            update recon_trn_tpreview
            set previewdtl_post_flag = 'Y'
            where job_gid = in_job_gid
            and previewdtl_post_flag = 'N'
            and delete_flag = 'N';
          end if;

          truncate recon_tmp_t3source;
          truncate recon_tmp_t3comparison;
          truncate recon_tmp_t3match;
          truncate recon_tmp_t3matchdtl;
          truncate recon_tmp_t3trangid;
          truncate recon_tmp_t3tranbrkpgid;
          truncate recon_tmp_t3pseudorows;
          truncate recon_tmp_t3value;

          insert into recon_tmp_t3pseudorows select 0 union select 1;

    end loop applyrule_loop;

    close applyrule_cursor;
  end applyrule_block;

  set out_result = v_count;
  set out_msg = 'Supporting file posted successfully !';

  drop temporary table if exists recon_tmp_t3source;
  drop temporary table if exists recon_tmp_t3comparison;
  drop temporary table if exists recon_tmp_t3match;
  drop temporary table if exists recon_tmp_t3matchdtl;
  drop temporary table if exists recon_tmp_t3pseudorows;
  drop temporary table if exists recon_tmp_t3trangid;
  drop temporary table if exists recon_tmp_t3tranbrkpgid;
  drop temporary table if exists recon_tmp_t3value;
  drop temporary table if exists recon_tmp_t3index;
  drop temporary table if exists recon_tmp_t3sql;
end $$

DELIMITER ;