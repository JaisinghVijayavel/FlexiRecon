DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_theme_comparisonagg` $$
CREATE PROCEDURE `pr_run_theme_comparisonagg`
(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_recontype_code varchar(32) default '';

  declare v_source_head_sql text default '';
  declare v_comparison_head_sql text default '';

  declare v_source_headbrkp_sql text default '';
  declare v_comparison_headbrkp_sql text default '';

  declare v_theme_name text default '';

  declare v_sql text default '';
  declare v_tmp_sql text default '';
  declare v_source_sql text default '';
  declare v_comparison_sql text default '';
  declare v_match_sql text default '';
  declare v_trangid_sql text default '';
  declare v_index_sql text default '';

  declare v_theme_code text default '';

  declare v_reversal_flag char(1) default '';
  declare v_group_flag varchar(32) default '';
  declare v_group_desc text default '';
  declare v_group_method_flag char(1) default '';
  declare v_manytomany_match_flag char(1) default '';
  declare v_field_group_flag char(1) default '';
  declare v_txt text default '';
  declare v_result int default 0;

  declare v_source_dataset_code varchar(32) default '';
  declare v_source_dataset_type varchar(32) default '';

  declare v_comparison_dataset_code varchar(32) default '';
  declare v_comparison_dataset_type varchar(32) default '';

  declare v_source_tranbrkp_code varchar(32) default '';
  declare v_comparison_tranbrkp_code varchar(32) default '';

  declare v_source_field text default '';
  declare v_source_field_format text default '';
  declare v_extraction_criteria text default '';
  declare v_extraction_filter int default 0;
  declare v_comparison_field text default '';
  declare v_comparison_criteria text default '';
  declare v_group_field text default '';
  declare v_group_condition text default '';

  declare v_source_condition text default '';
  declare v_comparison_condition text default '';
  declare v_build_condition text default '';

  declare v_themefilter_condition text default '';
  declare v_sourcebase_filter text default '';
  declare v_comparisonbase_filter text default '';
  declare v_comparison_filter text default '';

  declare v_theme_condition text default '';
  declare v_theme_notnull_condition text default '';

  declare v_fieldfilter_format text default '';
  declare v_comparisonfilter_format text default '';

  declare v_theme_groupby text default '';
  declare v_source_groupby text default '';
  declare v_comparison_groupby text default '';

  declare v_source_agg_field text default '';
  declare v_comparison_agg_field text default '';

  declare v_source_aggfunction_field text default '';
  declare v_comparison_aggfunction_field text default '';

  declare v_field_format text default '';
  declare v_field text default '';

  declare v_tran_gid int default 0;
  declare v_tran_mult tinyint default 0;
  declare v_diff_value double(15,2) default 0;
  declare v_mapped_value double(15,2) default 0;

  declare v_matched_value double(15,2) default 0;
  declare v_matched_count int default 0;

  declare v_parent_tran_gid int default 0;
  declare v_parent_tranbrkp_gid int default 0;

  declare v_excp_value double(15,2) default 0;

  declare v_txt_tran_gid text default '';

  declare v_source_tran_gid text default '';
  declare v_comparison_tran_gid text default '';
  declare v_count int default 0;

  declare v_filter_applied_on char(1) default '';
  declare v_filter_field text default '';
  declare v_filter_criteria text default '';
  declare v_filter_value text default '';

  declare v_open_parentheses_flag text default '';
  declare v_close_parentheses_flag text default '';
  declare v_join_condition text default '';

  declare v_tran_fields text default '';
  declare v_tranbrkp_fields text default '';

  declare v_grp_field text default '';
  declare v_grp_field_condition text default '';

  declare v_source_field_org_type text default '';
  declare v_comparison_field_org_type text default '';

  declare v_recon_field text default '';
  declare v_themeagg_function text default '';
  declare v_themeagg_field text default '';
  declare v_themeagg_field_type text default '';

  declare v_themeagg_applied_on text default '';
  declare v_themeagg_criteria text default '';
  declare v_themeagg_value_flag text default '';
  declare v_themeagg_value text default '';

  declare v_themeagg_condition text default '';

  declare v_database_name text default '';
  declare v_table_name text default '';
  declare v_index_name text default '';
  declare v_sys_index_name text default '';

  declare v_recon_name text default '';
  declare v_recon_value_flag text default '';
  declare v_recon_date_flag text default '';
  declare v_field_type text default '';

  declare v_preview_gid int default 0;

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Invalid recon !';
    leave me;
  end if;

  select database() into v_database_name;

  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;
  drop temporary table if exists recon_tmp_ttranwithbrkpgid;

  drop temporary table if exists recon_tmp_tindex;
  drop temporary table if exists recon_tmp_tsql;

  drop temporary table if exists recon_tmp_tsourceagg;
  drop temporary table if exists recon_tmp_tcomparisonagg;
  drop temporary table if exists recon_tmp_ttranagg;

  CREATE TEMPORARY TABLE recon_tmp_tindex(
    table_name varchar(255) not null,
    index_name varchar(255) not null,
    sys_flag char(1) not null default 'N',
    PRIMARY KEY (table_name,index_name),
    key idx_sys_flag(sys_flag)
  ) ENGINE = MyISAM;

  insert into recon_tmp_tindex select 'recon_tmp_tsource','idx_tran_date','Y';
  insert into recon_tmp_tindex select 'recon_tmp_tcomparison','idx_tran_date','Y';

  CREATE temporary TABLE recon_tmp_tpseudorows(
    row int unsigned NOT NULL,
    PRIMARY KEY (row)
  ) ENGINE = MyISAM;

  insert into recon_tmp_tpseudorows select 0 union select 1;

  CREATE temporary TABLE recon_tmp_ttrangid(
    tran_gid int unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttranbrkpgid(
    tranbrkp_gid int unsigned NOT NULL,
    excp_value double(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    tran_gid int not null default 0,
    PRIMARY KEY (tranbrkp_gid),
    key idx_tran_gid(tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttranwithbrkpgid(
    tran_gid int unsigned not null,
    tranbrkp_gid int unsigned NOT NULL,
    rec_count int not null default 0,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;


  CREATE temporary TABLE recon_tmp_tsql(
    sql_gid int(10) unsigned NOT NULL AUTO_INCREMENT,
    table_type char(1) default null,
    acc_mode char(1) default null,
    sql_query text default null,
    PRIMARY KEY (sql_gid)
  ) ENGINE = MyISAM;

  if in_automatch_flag <> 'Y' then
    leave me;
  end if;

  -- tran fields
  select
    group_concat(field_name)
  into
    v_tran_fields
  from recon_mst_ttablestru
  where table_name = 'recon_tmp_ttranwithbrkp'
  and field_name <> 'tranbrkp_gid'
  and delete_flag = 'N'
  order by display_order;

  select
    group_concat(field_name)
  into
    v_tranbrkp_fields
  from recon_mst_ttablestru
  where table_name = 'recon_tmp_ttranwithbrkp'
  and delete_flag = 'N'
  order by display_order;

  -- recon retails
  select
    recon_name,recontype_code
  into
    v_recon_name,v_recontype_code
  from recon_mst_trecon
  where recon_code = in_recon_code
  and period_from <= curdate()
  and (until_active_flag = 'Y'
  or period_to >= curdate())
  and delete_flag = 'N';

  set v_recontype_code = ifnull(v_recontype_code,'');
  set v_recon_name = ifnull(v_recon_name,'');

  if v_recontype_code <> 'N' then
    set v_recon_value_flag = 'Y';
  else
    set v_recon_value_flag = 'N';
    leave me;
  end if;

  theme_block:begin
    declare theme_done int default 0;
    declare theme_cursor cursor for
      select
		    a.theme_code,
        a.theme_name,
        a.source_dataset_code,
        a.comparison_dataset_code
      from recon_mst_ttheme as a
      where a.recon_code = in_recon_code
      and a.theme_type_code = 'QCD_THEME_COMPARE_AGG'
      and a.hold_flag = 'N'
      and a.active_status = 'Y'
      and a.delete_flag = 'N'
      order by a.theme_order;
    declare continue handler for not found set theme_done=1;

    open theme_cursor;

    theme_loop: loop
      fetch theme_cursor into v_theme_code,
                              v_theme_name,
                              v_source_dataset_code,
                              v_comparison_dataset_code;

      if theme_done = 1 then leave theme_loop; end if;

      set v_theme_code = ifnull(v_theme_code,'');
      set v_theme_name = ifnull(v_theme_name,'');

      set v_group_flag = 'Y';
      set v_manytomany_match_flag = 'Y';

      -- v_source_dataset_type
      select
        dataset_type into v_source_dataset_type
      from recon_mst_trecondataset
      where recon_code = in_recon_code
      and dataset_code = v_source_dataset_code
      and active_status = 'Y'
      and delete_flag = 'N';

      set v_source_dataset_type = ifnull(v_source_dataset_type,'B');

      -- v_comparison_dataset_type
      select
        dataset_type into v_comparison_dataset_type
      from recon_mst_trecondataset
      where recon_code = in_recon_code
      and dataset_code = v_comparison_dataset_code
      and active_status = 'Y'
      and delete_flag = 'N';

      set v_comparison_dataset_type = ifnull(v_comparison_dataset_type,'T');

      -- source from tran table
      set v_source_head_sql = concat('insert into recon_tmp_tsource (',v_tran_fields,') ');

      set v_source_head_sql = concat(v_source_head_sql,' select ',v_tran_fields ,' from recon_trn_ttran ');

      set v_source_head_sql = concat(v_source_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_source_head_sql = concat(v_source_head_sql,' and excp_value <> 0 and mapped_value = 0 ');
      else
        set v_source_head_sql = concat(v_source_head_sql,' and ko_gid = 0 ');
      end if;

      -- comparison from tran table
      set v_comparison_head_sql = concat('insert into recon_tmp_tcomparison (',v_tran_fields,') ');

      set v_comparison_head_sql = concat(v_comparison_head_sql,' select ',v_tran_fields ,' from recon_trn_ttran ');

      set v_comparison_head_sql = concat(v_comparison_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and excp_value <> 0 and mapped_value = 0 ');
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and ko_gid = 0 ');
      end if;

      -- source from tranbrkp table
      set v_source_headbrkp_sql = concat('insert into recon_tmp_tsource (',v_tranbrkp_fields,') ');

      set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' select ',v_tranbrkp_fields ,' from recon_trn_ttranbrkp ');

      set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and excp_value <> 0 and tran_gid > 0 ');
      else
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and ko_gid = 0 ');
      end if;

      -- comparison from tranbrkp table
      set v_comparison_headbrkp_sql = concat('insert into recon_tmp_tcomparison (',v_tranbrkp_fields,') ');

      set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' select ',v_tranbrkp_fields ,' from recon_trn_ttranbrkp ');

      set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and excp_value <> 0 and tran_gid > 0 ');
      else
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and ko_gid = 0 ');
      end if;

          themefilter_block:begin
            declare themefilter_done int default 0;
            declare themefilter_cursor cursor for
            select
              filter_applied_on,
              filter_field,
              filter_criteria,
              filter_value,
              open_parentheses_flag,
              close_parentheses_flag,
              join_condition
            from recon_mst_tthemefilter
            where theme_code = v_theme_code
            and active_status = 'Y'
            and delete_flag = 'N'
            order by filter_applied_on,themefilter_seqno,themefilter_gid;

            declare continue handler for not found set themefilter_done=1;

            open themefilter_cursor;

            set v_sourcebase_filter = ' and ';
            set v_comparisonbase_filter = ' and ';

            themefilter_loop: loop
              fetch themefilter_cursor into v_filter_applied_on,
                                            v_filter_field,
                                            v_filter_criteria,
                                            v_filter_value,
                                            v_open_parentheses_flag,
                                            v_close_parentheses_flag,
                                            v_join_condition;

              if themefilter_done = 1 then leave themefilter_loop; end if;

              set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
              set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
              set v_join_condition = ifnull(v_join_condition,'');

              if v_join_condition = '' then
                set v_join_condition = 'and';
              end if;

              set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
              set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

              set v_themefilter_condition = concat(v_open_parentheses_flag,
                                                  fn_get_basefilterformat(v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value),
                                                  v_close_parentheses_flag,' ',
                                                  v_join_condition,' ');

              if v_filter_applied_on = 'S' then
                set v_sourcebase_filter = concat(v_sourcebase_filter,v_themefilter_condition);
              elseif v_filter_applied_on = 'C' then
                set v_comparisonbase_filter = concat(v_comparisonbase_filter,v_themefilter_condition);
              end if;
            end loop themefilter_loop;

            close themefilter_cursor;
          end themefilter_block;

          set v_sourcebase_filter = concat(v_sourcebase_filter,' 1 = 1 ');
          set v_comparisonbase_filter = concat(v_comparisonbase_filter,' 1 = 1 ');

          set v_theme_condition = ' and ';
          set v_theme_notnull_condition = ' and ';
          set v_theme_groupby = '';

          set v_source_condition = ' and ';
          set v_comparison_condition = ' and ';

          set v_source_groupby = '';
          set v_comparison_groupby = '';
          set v_source_agg_field = '';
          set v_source_aggfunction_field = '';
          set v_comparison_agg_field = '';
          set v_comparison_aggfunction_field = '';

          drop temporary table if exists recon_tmp_tsource;
          drop temporary table if exists recon_tmp_tcomparison;
          drop temporary table if exists recon_tmp_tsourcedup;

          /*
          drop table if exists recon_tmp_tsource;
          drop table if exists recon_tmp_tcomparison;
          */

          create temporary table recon_tmp_tsource select * from recon_tmp_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tsource ENGINE = MyISAM;
          alter table recon_tmp_tsource add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tsource(excp_value);
          create index idx_tran_date on recon_tmp_tsource(tran_date);
          create index idx_recon_code on recon_tmp_tsource(recon_code);
          create index idx_dataset_code on recon_tmp_tsource(recon_code,dataset_code);

          create temporary table recon_tmp_tcomparison select * from recon_tmp_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tcomparison ENGINE = MyISAM;
          alter table recon_tmp_tcomparison add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tcomparison(excp_value);
          create index idx_tran_date on recon_tmp_tcomparison(tran_date);
          create index idx_recon_code on recon_tmp_tcomparison(recon_code);
          create index idx_dataset_cdoe on recon_tmp_tcomparison(recon_code,dataset_code);

          create temporary table recon_tmp_tsourcedup select * from recon_tmp_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tsourcedup add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tsourcedup(excp_value);
          create index idx_tran_date on recon_tmp_tsourcedup(tran_date);
          create index idx_dataset_code on recon_tmp_tsourcedup(recon_code,dataset_code);
          alter table recon_tmp_tsourcedup ENGINE = MyISAM;

          drop temporary table if exists recon_tmp_tsourceagg;
          drop temporary table if exists recon_tmp_tcomparisonagg;
          drop temporary table if exists recon_tmp_ttranagg;

          /*
          drop table if exists recon_tmp_tsourceagg;
          drop table if exists recon_tmp_tcomparisonagg;
          drop table if exists recon_tmp_ttranagg;
          */

          -- grouping temp table movement
          -- create agg temp tables
          create temporary table recon_tmp_tsourceagg select * from recon_rpt_tthemeagg where 1 = 2;
          alter table recon_tmp_tsourceagg ENGINE = MyISAM;
          alter table recon_tmp_tsourceagg add primary key(themeagg_gid);
          alter table recon_tmp_tsourceagg modify column themeagg_gid int unsigned AUTO_INCREMENT;

          create index idx_tran_value on recon_tmp_tsourceagg(tran_value);
          create index idx_excp_value on recon_tmp_tsourceagg(excp_value);
          create index idx_tran_date on recon_tmp_tsourceagg(tran_date);

          create temporary table recon_tmp_tcomparisonagg select * from recon_rpt_tthemeagg where 1 = 2;
          alter table recon_tmp_tcomparisonagg ENGINE = MyISAM;
          alter table recon_tmp_tcomparisonagg add primary key(themeagg_gid);
          alter table recon_tmp_tcomparisonagg modify column themeagg_gid int unsigned AUTO_INCREMENT;

          create index idx_tran_value on recon_tmp_tcomparisonagg(tran_value);
          create index idx_excp_value on recon_tmp_tcomparisonagg(excp_value);
          create index idx_tran_date on recon_tmp_tcomparisonagg(tran_date);

          create temporary table recon_tmp_ttranagg select * from recon_rpt_tthemeagg where 1 = 2;
          alter table recon_tmp_ttranagg ENGINE = MyISAM;
          alter table recon_tmp_ttranagg add primary key(themeagg_gid);
          alter table recon_tmp_ttranagg modify column themeagg_gid int unsigned AUTO_INCREMENT;

          create index idx_rec_count on recon_tmp_ttranagg(rec_count);

          -- index table
          delete from recon_tmp_tindex where index_name <> 'idx_tran_date';
          truncate recon_tmp_tsql;

          condition_block:begin
            declare condition_done int default 0;
            declare condition_cursor cursor for
            select
              a.source_field,a.extraction_criteria,
              a.comparison_field,a.comparison_criteria,
              a.open_parentheses_flag,a.close_parentheses_flag,
              a.join_condition
            from recon_mst_tthemecondition as a
            where a.theme_code = v_theme_code
            and a.active_status = 'Y'
            and a.delete_flag = 'N'
            order by themecondition_seqno,themecondition_gid;

            declare continue handler for not found set condition_done=1;

            open condition_cursor;

            condition_loop: loop
              fetch condition_cursor into v_source_field,v_extraction_criteria,
                                     v_comparison_field,v_comparison_criteria,
                                     v_open_parentheses_flag,v_close_parentheses_flag,v_join_condition;
              if condition_done = 1 then leave condition_loop; end if;

              set v_index_name = concat('idx_',v_source_field);

              -- recon_tmp_tsource
              if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_tsource'
                            and index_name = v_index_name) then

                set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tsource(',v_source_field,')');
                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_tsource',v_index_name;
              end if;

              -- recon_tmp_tsourceagg
              if mid(v_source_field,1,3) = 'col' then
                if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_tsourceagg'
                            and index_name = v_index_name) then

                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tsourceagg(',v_source_field,')');
                  call pr_run_sql(v_index_sql,@msg,@result);

                  insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_tsourceagg',v_index_name;
                end if;

                -- recon_tmp_ttranagg
                if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_ttranagg'
                            and index_name = v_index_name) then

                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_ttranagg(',v_source_field,')');
                  call pr_run_sql(v_index_sql,@msg,@result);

                  insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_ttranagg',v_index_name;
                end if;
              end if;

              -- recon_tmp_tcomparison
              set v_index_name = concat('idx_',v_comparison_field);

              if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_tcomparison'
                            and index_name = v_index_name) then

                set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tcomparison(',v_comparison_field,')');

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_tcomparison',v_index_name;
              end if;

              -- recon_tmp_tcomparisonagg
              if mid(v_comparison_field,1,3) = 'col' then
                if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_tcomparisonagg'
                            and index_name = v_index_name) then

                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tcomparisonagg(',v_comparison_field,')');
                  call pr_run_sql(v_index_sql,@msg,@result);

                  insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_tcomparisonagg',v_index_name;
                end if;

                -- recon_tmp_ttranagg
                if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_ttranagg'
                            and index_name = v_index_name) then

                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_ttranagg(',v_comparison_field,')');
                  call pr_run_sql(v_index_sql,@msg,@result);

                  insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_ttranagg',v_index_name;
                end if;
              end if;

              set v_source_field_org_type = fn_get_fieldorgtype(in_recon_code,v_source_field);
              set v_comparison_field_org_type = fn_get_fieldorgtype(in_recon_code,v_comparison_field);

              set v_extraction_criteria = ifnull(v_extraction_criteria,'');
              set v_comparison_criteria = ifnull(v_comparison_criteria,'');

              set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
              set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');

              set v_join_condition = ifnull(v_join_condition,'');
              if v_join_condition = '' then set v_join_condition = ' and '; end if;

              set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
              set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

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

              -- source
              if (instr(v_extraction_criteria,'$FIELD$') > 0)
                and v_open_parentheses_flag <> '('
                and v_join_condition <> 'OR'
                and v_close_parentheses_flag <> ')' then
                set v_field = replace(v_source_field,'a.','');
                set v_field_format = fn_get_fieldfilterformat(v_field,v_extraction_criteria,v_extraction_filter);

                set v_sql = '';
                set v_sql = concat(v_sql,'update recon_tmp_tsource set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format);

                insert into recon_tmp_tsql(table_type,sql_query) values ('S',v_sql);

                set v_extraction_criteria = 'EXACT';
                set v_extraction_filter = 0;
              end if;

              -- comparison
              if (instr(v_comparison_criteria,'$FIELD$') > 0)
                and v_open_parentheses_flag <> '('
                and v_join_condition <> 'OR'
                and v_close_parentheses_flag <> ')' then

                set v_field = replace(v_comparison_field,'b.','');
                set v_field_format = fn_get_fieldfilterformat(v_field,v_comparison_criteria,v_comparison_filter);

                set v_sql = '';
                set v_sql = concat(v_sql,'update recon_tmp_tcomparison set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');

                if v_recontype_code <> 'N' then
                  set v_sql = concat(v_sql,'where true ');
                end if;

                insert into recon_tmp_tsql(table_type,sql_query) values ('C',v_sql);

                /*
								if v_manytomany_match_flag = 'Y' and v_recontype_code <> 'N' and v_recontype_code <> 'V' then
									set v_sql = '';
									set v_sql = concat(v_sql,'update recon_tmp_tcomparison set ');
									set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');
                  set v_sql = concat(v_sql,'where true ');

									insert into recon_tmp_tsql(table_type,sql_query) values ('C',v_sql);
								end if;
                */

                set v_comparison_criteria = 'EXACT';
                set v_comparison_filter = 0;
              end if;

              set v_source_field_format = fn_get_fieldfilterformat(v_source_field,v_extraction_criteria,0);
              set v_build_condition = concat(v_open_parentheses_flag,
                                             fn_get_comparisoncondition(in_recon_code,v_source_field_format,v_comparison_field,v_comparison_criteria,0),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition);

              set v_theme_condition = concat(v_theme_condition,' ',v_build_condition,' ');

              -- build condition for not null
              set v_build_condition = concat(' ',v_open_parentheses_flag);
              set v_build_condition = concat(v_build_condition,' (');

              if v_source_field_org_type = 'TEXT' then
                set v_build_condition = concat(v_build_condition,v_source_field ,' <> '''' ');
              else
                set v_build_condition = concat(v_build_condition,v_source_field ,' is not null ');
              end if;

              set v_build_condition = concat(v_build_condition,' and ');

              if v_comparison_field_org_type = 'TEXT' then
                set v_build_condition = concat(v_build_condition,v_comparison_field ,' <> '''' ');
              else
                set v_build_condition = concat(v_build_condition,v_comparison_field ,' is not null ');
              end if;

              set v_build_condition = concat(v_build_condition,')');

              set v_build_condition = concat(v_build_condition,' ',v_close_parentheses_flag,' ',v_join_condition);

              set v_theme_notnull_condition = concat(v_theme_notnull_condition,v_build_condition);

              set v_theme_groupby = concat(v_theme_groupby,',',v_source_field);

              set v_source_groupby = concat(v_source_groupby,',',v_source_field);
              set v_comparison_groupby = concat(v_comparison_groupby,',',v_comparison_field);
            end loop condition_loop;

            close condition_cursor;
          end condition_block;

          truncate recon_tmp_tsource;
          truncate recon_tmp_tcomparison;

          if v_source_condition = ' and ' or v_comparison_condition = ' and ' then
            set v_source_condition = ' and 1 = 2 ';
            set v_comparison_condition  = ' and 1 = 2 ';
            set v_theme_condition = ' and 1 = 2 ';
            set v_theme_notnull_condition = ' and 1 =2 ';
            set v_theme_groupby = ',tran_gid';
          else
            set v_source_condition = concat(v_source_condition, ' 1 = 1 ');
            set v_comparison_condition  = concat(v_comparison_condition,' 1 = 1 ');
            set v_theme_condition  = concat(v_theme_condition,' 1 = 1 ');
          end if;

          -- source from tran table
          set v_source_sql = v_source_head_sql;

          if v_source_dataset_type <> 'S' then
            set v_source_sql = concat(v_source_sql,' and dataset_code = ',char(39),v_source_dataset_code,char(39));
          else
            set v_source_sql = concat(v_source_sql,' and tranbrkp_dataset_code = ',char(39),v_source_dataset_code,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_source_sql = concat(v_source_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_source_sql = concat(v_source_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

          set v_source_sql = concat(v_source_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_source_sql = concat(v_source_sql,' ',v_source_condition);
          set v_source_sql = concat(v_source_sql,' ',v_sourcebase_filter);

          call pr_run_sql(v_source_sql,@result,@msg);

          -- source from tranbrkp table
          set v_source_sql = v_source_headbrkp_sql;

          if v_source_dataset_type <> 'S' then
            set v_source_sql = concat(v_source_sql,' and dataset_code = ',char(39),v_source_dataset_code,char(39));
          else
            set v_source_sql = concat(v_source_sql,' and tranbrkp_dataset_code = ',char(39),v_source_dataset_code,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_source_sql = concat(v_source_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_source_sql = concat(v_source_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

          set v_source_sql = concat(v_source_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_source_sql = concat(v_source_sql,' ',v_source_condition);
          set v_source_sql = concat(v_source_sql,' ',v_sourcebase_filter);

          call pr_run_sql(v_source_sql,@result,@msg);

          -- comparison from tran table
          set v_comparison_sql = v_comparison_head_sql;

          if v_comparison_dataset_type <> 'S' then
            set v_comparison_sql = concat(v_comparison_sql,' and dataset_code = ',char(39),v_comparison_dataset_code,char(39));
          else
            set v_comparison_sql = concat(v_comparison_sql,' and tranbrkp_dataset_code = ',char(39),v_comparison_dataset_code,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

          set v_comparison_sql = concat(v_comparison_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_comparison_sql = concat(v_comparison_sql,' ',v_comparison_condition);
          set v_comparison_sql = concat(v_comparison_sql,' ',v_comparisonbase_filter);

          call pr_run_sql(v_comparison_sql,@result,@msg);

          -- comparison from tranbrkp table
          set v_comparison_sql = v_comparison_headbrkp_sql;

          if v_comparison_dataset_type <> 'S' then
            set v_comparison_sql = concat(v_comparison_sql,' and dataset_code = ',char(39),v_comparison_dataset_code,char(39));
          else
            set v_comparison_sql = concat(v_comparison_sql,' and tranbrkp_dataset_code = ',char(39),v_comparison_dataset_code,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

          set v_comparison_sql = concat(v_comparison_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_comparison_sql = concat(v_comparison_sql,' ',v_comparison_condition);
          set v_comparison_sql = concat(v_comparison_sql,' ',v_comparisonbase_filter);

          call pr_run_sql(v_comparison_sql,@result,@msg);

          -- sql block
          sql_block:begin
            declare sql_done int default 0;
            declare sql_cursor cursor for
            select sql_query from recon_tmp_tsql;
            declare continue handler for not found set sql_done=1;

            open sql_cursor;

            sql_loop: loop
              fetch sql_cursor into v_sql;
              if sql_done = 1 then leave sql_loop; end if;

              call pr_run_sql(v_sql,@result,@msg);
            end loop sql_loop;
            close sql_cursor;
          end sql_block;

          -- preload pseudorows
          truncate recon_tmp_tpseudorows;
          insert into recon_tmp_tpseudorows select 0 union select 1;

          -- get target addtional group field
          if v_group_flag = 'Y' then
            -- grp on source
            select group_concat(concat('a.',grp_field)) into v_grp_field from recon_mst_tthemegrpfield
            where theme_code = v_theme_code
            and grpfield_applied_on = 'S'
            and active_status = 'Y'
            and delete_flag = 'N';

            set v_grp_field = ifnull(v_grp_field,'');

            if v_grp_field <> '' then
              if v_theme_groupby <> '' then
                set v_theme_groupby = concat(v_theme_groupby,',',v_grp_field);
              else
                set v_theme_groupby = v_grp_field;
              end if;

              if v_source_groupby <> '' then
                set v_source_groupby = concat(v_source_groupby,',',v_grp_field);
              else
                set v_source_groupby = v_grp_field;
              end if;
            end if;

            -- grp on comparison
            select group_concat(concat('b.',grp_field)) into v_grp_field from recon_mst_tthemegrpfield
            where theme_code = v_theme_code
            and grpfield_applied_on = 'C'
            and active_status = 'Y'
            and delete_flag = 'N';

            set v_grp_field = ifnull(v_grp_field,'');

            if v_grp_field <> '' then
              if v_theme_groupby <> '' then
                set v_theme_groupby = concat(v_theme_groupby,',',v_grp_field);
              else
                set v_theme_groupby = v_grp_field;
              end if;

              if v_comparison_groupby <> '' then
                set v_comparison_groupby = concat(v_comparison_groupby,',',v_grp_field);
              else
                set v_comparison_groupby = v_grp_field;
              end if;
            end if;
					end if;

          alter table recon_tmp_tcomparison ENGINE = MyISAM;
          alter table recon_tmp_tsource ENGINE = MyISAM;

          -- source agg block
          agg_source_block:begin
            declare agg_source_done int default 0;
            declare agg_source_cursor cursor for
            select
              recon_field,
              themeagg_function,
              themeagg_field,
              themeagg_field_type
            from recon_mst_tthemeaggfield
            where theme_code = v_theme_code
            and themeaggfield_applied_on = 'S'
            and active_status = 'Y'
            and delete_flag = 'N'
            order by themeaggfield_seqno;

            declare continue handler for not found set agg_source_done=1;

            open agg_source_cursor;

            agg_source_loop: loop
              fetch agg_source_cursor into v_recon_field,
                                           v_themeagg_function,
                                           v_themeagg_field,
                                           v_themeagg_field_type;
              if agg_source_done = 1 then leave agg_source_loop; end if;

              set v_txt = fn_get_fieldfunctionformat(v_recon_field,v_themeagg_field_type,v_themeagg_function);
              set v_txt = concat('cast(',v_txt,' as nchar)');

              set v_source_agg_field = concat(v_source_agg_field,',',v_themeagg_field);
              set v_source_aggfunction_field = concat(v_source_aggfunction_field,',',v_txt);

            end loop agg_source_loop;

            close agg_source_cursor;
          end agg_source_block;

					-- move records in source agg table
					set v_sql = concat('insert into recon_tmp_tsourceagg (',substr(replace(v_source_groupby,'a.',''),2),v_source_agg_field,',');
					set v_sql = concat(v_sql,'rec_count,themeagg_json,src_comp_flag) ');
					set v_sql = concat(v_sql,'select ',substr(replace(v_source_groupby,'a.',''),2),v_source_aggfunction_field,',');
					set v_sql = concat(v_sql,'count(*),');

					set v_sql = concat(v_sql,'cast(concat(',char(39),'[',char(39),',');
					set v_sql = concat(v_sql,'group_concat(',char(39),'{');
					set v_sql = concat(v_sql,'"tran_gid":',char(39),',cast(tran_gid as nchar),',char(39),',');
					set v_sql = concat(v_sql,'"tranbrkp_gid":',char(39),',cast(tranbrkp_gid as nchar),',char(39),',');
					set v_sql = concat(v_sql,'"tran_mult":',char(39),',cast(tran_mult as nchar),',char(39),',');
					set v_sql = concat(v_sql,'"src_comp_flag":"S",');
					set v_sql = concat(v_sql,'"ko_value":',char(39),',cast(excp_value as nchar),',char(39));
					set v_sql = concat(v_sql,'}',char(39),' order by tran_gid,tranbrkp_gid),');
					set v_sql = concat(v_sql,char(39), ']',char(39),') as json) as matched_json,');
					set v_sql = concat(v_sql,char(39), 'S',char(39),' as src_comp_flag ');

					set v_sql = concat(v_sql,'from recon_tmp_tsource ');
					set v_sql = concat(v_sql,'group by ',substr(replace(v_source_groupby,'a.',''),2));

          call pr_run_sql(v_sql,@result,@msg);

          -- comparison agg block
          agg_comparison_block:begin
            declare agg_comparison_done int default 0;
            declare agg_comparison_cursor cursor for
            select
              recon_field,
              themeagg_function,
              themeagg_field,
              themeagg_field_type
            from recon_mst_tthemeaggfield
            where theme_code = v_theme_code
            and themeaggfield_applied_on = 'C'
            and active_status = 'Y'
            and delete_flag = 'N'
            order by themeaggfield_seqno;

            declare continue handler for not found set agg_comparison_done=1;

            open agg_comparison_cursor;

            agg_comparison_loop: loop
              fetch agg_comparison_cursor into v_recon_field,
                                           v_themeagg_function,
                                           v_themeagg_field,
                                           v_themeagg_field_type;
              if agg_comparison_done = 1 then leave agg_comparison_loop; end if;

              set v_txt = fn_get_fieldfunctionformat(v_recon_field,v_themeagg_field_type,v_themeagg_function);
              set v_txt = concat('cast(',v_txt,' as nchar)');

              set v_comparison_agg_field = concat(v_comparison_agg_field,',',v_themeagg_field);
              set v_comparison_aggfunction_field = concat(v_comparison_aggfunction_field,',',v_txt);
            end loop agg_comparison_loop;

            close agg_comparison_cursor;
          end agg_comparison_block;

					-- move records in comparison agg table
					set v_sql = concat('insert into recon_tmp_tcomparisonagg (',substr(replace(v_comparison_groupby,'b.',''),2),v_comparison_agg_field,',');
					set v_sql = concat(v_sql,'rec_count,themeagg_json,src_comp_flag) ');
					set v_sql = concat(v_sql,'select ',substr(replace(v_comparison_groupby,'b.',''),2),v_comparison_aggfunction_field,',');
					set v_sql = concat(v_sql,'count(*),');

					set v_sql = concat(v_sql,'cast(concat(',char(39),'[',char(39),',');
					set v_sql = concat(v_sql,'group_concat(',char(39),'{');
					set v_sql = concat(v_sql,'"tran_gid":',char(39),',cast(tran_gid as nchar),',char(39),',');
					set v_sql = concat(v_sql,'"tranbrkp_gid":',char(39),',cast(tranbrkp_gid as nchar),',char(39),',');
					set v_sql = concat(v_sql,'"tran_mult":',char(39),',cast(tran_mult as nchar),',char(39),',');
					set v_sql = concat(v_sql,'"src_comp_flag":"C",');
					set v_sql = concat(v_sql,'"ko_value":',char(39),',cast(excp_value as nchar),',char(39));
					set v_sql = concat(v_sql,'}',char(39),' order by tran_gid,tranbrkp_gid),');
					set v_sql = concat(v_sql,char(39), ']',char(39),') as json) as matched_json,');
					set v_sql = concat(v_sql,char(39), 'C',char(39),' as src_comp_flag ');

					set v_sql = concat(v_sql,'from recon_tmp_tcomparison ');
					set v_sql = concat(v_sql,'group by ',substr(replace(v_comparison_groupby,'b.',''),2));

          call pr_run_sql(v_sql,@result,@msg);

          set v_themeagg_condition = '';

          -- agg condition block
          agg_condition_block:begin
            declare agg_condition_done int default 0;
            declare agg_condition_cursor cursor for
            select
              a.themeagg_applied_on,
              a.themeagg_field,
              a.themeagg_criteria,
              a.themeagg_value_flag,
              a.themeagg_value,
              b.themeagg_field_type,
              a.open_parentheses_flag,
              a.close_parentheses_flag,
              a.join_condition
            from recon_mst_tthemeaggcondition as a
            left join recon_mst_tthemeaggfield as b on a.themeagg_field = b.themeagg_field
              and a.theme_code = b.theme_code
              and a.themeagg_applied_on = b.themeaggfield_applied_on
              and b.active_status = 'Y'
              and b.delete_flag = 'N'
            where a.theme_code = v_theme_code
            and a.active_status = 'Y'
            and a.delete_flag = 'N'
            order by a.themeaggcondition_seqno;

            declare continue handler for not found set agg_condition_done=1;

            open agg_condition_cursor;

            agg_condition_loop: loop
              fetch agg_condition_cursor into v_themeagg_applied_on,
                                              v_themeagg_field,
                                              v_themeagg_criteria,
                                              v_themeagg_value_flag,
                                              v_themeagg_value,
                                              v_themeagg_field_type,
                                              v_open_parentheses_flag,
                                              v_close_parentheses_flag,
                                              v_join_condition;

              if agg_condition_done = 1 then leave agg_condition_loop; end if;

              set v_themeagg_applied_on = ifnull(v_themeagg_applied_on,'');
              set v_themeagg_field = ifnull(v_themeagg_field,'');
              set v_themeagg_criteria = ifnull(v_themeagg_criteria,'');
              set v_themeagg_value_flag = ifnull(v_themeagg_value_flag,'');
              set v_themeagg_value = ifnull(v_themeagg_value,'');

              set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
              set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');

              set v_join_condition = ifnull(v_join_condition,'');
              if v_join_condition = '' then set v_join_condition = ' and '; end if;

              set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
              set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

              set v_txt = fn_get_themeaggcondition(v_themeagg_applied_on,
                                                   v_themeagg_field,
                                                   v_themeagg_field_type,
                                                   v_themeagg_criteria,
                                                   v_themeagg_value_flag,
                                                   v_themeagg_value);

              if v_themeagg_condition = '' then
                set v_themeagg_condition = ' and ';
              end if;

              set v_themeagg_condition = concat(v_themeagg_condition,
                                                v_open_parentheses_flag,
                                                v_txt,
                                                v_close_parentheses_flag,' ',v_join_condition,' ');
            end loop agg_condition_loop;

            close agg_condition_cursor;
          end agg_condition_block;

          if v_themeagg_condition <> '' then
            set v_themeagg_condition = concat(v_themeagg_condition,' 1 = 1 ');
          end if;

          -- inner join
          set v_sql = concat('insert into recon_tmp_ttranagg(rec_count,themeagg_json) ');
          set v_sql = concat(v_sql,'select (a.rec_count+b.rec_count),JSON_MERGE_PRESERVE(a.themeagg_json,b.themeagg_json) ');
          set v_sql = concat(v_sql,'from recon_tmp_tsourceagg as a ');
          set v_sql = concat(v_sql,'inner join recon_tmp_tcomparisonagg as b on 1 = 1 ',v_theme_condition);
          set v_sql = concat(v_sql,'where 1 = 1 ',v_themeagg_condition);

          call pr_run_sql(v_sql,@result,@msg);

          -- left join
          set v_sql = concat('insert into recon_tmp_ttranagg(rec_count,themeagg_json) ');
          set v_sql = concat(v_sql,'select a.rec_count,a.themeagg_json ');
          set v_sql = concat(v_sql,'from recon_tmp_tsourceagg as a ');
          set v_sql = concat(v_sql,'left join recon_tmp_tcomparisonagg as b on 1 = 1 ',v_theme_condition);
          set v_sql = concat(v_sql,'where 1 = 1 ',v_themeagg_condition);
          set v_sql = concat(v_sql,'and b.themeagg_gid is null ');

          call pr_run_sql(v_sql,@result,@msg);

          -- right join
          set v_sql = concat('insert into recon_tmp_ttranagg(rec_count,themeagg_json) ');
          set v_sql = concat(v_sql,'select b.rec_count,b.themeagg_json ');
          set v_sql = concat(v_sql,'from recon_tmp_tsourceagg as a ');
          set v_sql = concat(v_sql,'right join recon_tmp_tcomparisonagg as b on 1 = 1 ',v_theme_condition);
          set v_sql = concat(v_sql,'where 1 = 1 ',v_themeagg_condition);
          set v_sql = concat(v_sql,'and a.themeagg_gid is null ');

          call pr_run_sql(v_sql,@result,@msg);

          -- insert in tranbrkp_gid table
          select max(rec_count) into v_count from recon_tmp_ttranagg;
          set v_count = ifnull(v_count,0);

          truncate recon_tmp_tpseudorows;

          if v_count >= 2 then
            insert into recon_tmp_tpseudorows select row from pseudo_rows1 where row <= v_count;
          else
            insert into recon_tmp_tpseudorows select 0 union select 1;
          end if;

          truncate recon_tmp_ttranwithbrkpgid;

					insert ignore into recon_tmp_ttranwithbrkpgid (tran_gid,tranbrkp_gid)
						select
							JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_ttranagg.themeagg_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
							JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_ttranagg.themeagg_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid
						FROM recon_tmp_ttranagg
						JOIN recon_tmp_tpseudorows
						where rec_count > 0
						HAVING tran_gid IS NOT NULL;

          -- update theme
          -- tran table
          update recon_trn_ttran set
            theme_code = concat(if(theme_code is null,v_theme_name,concat(theme_code,',',v_theme_name)))
          where tran_gid in (select tran_gid from recon_tmp_ttranwithbrkpgid where tranbrkp_gid = 0);

          -- tranbrkp table
          update recon_trn_ttranbrkp set
            theme_code = concat(if(theme_code is null,v_theme_name,concat(theme_code,',',v_theme_name)))
          where (tran_gid,tranbrkp_gid) in (select tran_gid,tranbrkp_gid from recon_tmp_ttranwithbrkpgid
                                                      where tranbrkp_gid > 0);
          -- delete in index table
          delete from recon_tmp_tindex
          where table_name in ('recon_tmp_tsourceagg','recon_tmp_tcomparisonagg','recon_tmp_ttranagg');

          truncate recon_tmp_tsource;
          truncate recon_tmp_tcomparison;
          truncate recon_tmp_tsourcedup;
          truncate recon_tmp_ttrangid;
          truncate recon_tmp_ttranbrkpgid;

          truncate recon_tmp_tsourceagg;
          truncate recon_tmp_tcomparisonagg;
          truncate recon_tmp_ttranagg;

          drop temporary table if exists recon_tmp_tsource;
          drop temporary table if exists recon_tmp_tcomparison;

          drop temporary table if exists recon_tmp_tsourceagg;
          drop temporary table if exists recon_tmp_tcomparisonagg;
          drop temporary table if exists recon_tmp_ttranagg;
    end loop theme_loop;

    close theme_cursor;
  end theme_block;

  set out_result = v_count;

  set out_msg = 'Theme updated successfully !';

  drop temporary table if exists recon_tmp_tsource;
  drop temporary table if exists recon_tmp_tcomparison;
  drop temporary table if exists recon_tmp_tsourcedup;
  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;
  drop temporary table if exists recon_tmp_ttranwithbrkpgid;
  drop temporary table if exists recon_tmp_tindex;
  drop temporary table if exists recon_tmp_tsql;
end $$

DELIMITER ;