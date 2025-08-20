DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_theme_comparisonagg_new` $$
CREATE PROCEDURE `pr_run_theme_comparisonagg_new`
(
  in in_recon_code text,
  in in_theme_code text,
  in in_job_gid int,
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
    Updated Date : 20-08-2025

    Version : 2
  */

  declare v_recon_version text default '';
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
  declare v_filter_value_flag text default '';
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

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

  declare v_concurrent_ko_flag text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

	-- set tran table
  /*
	set v_tran_table = concat(in_recon_code,'_tran');
	set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  */

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(in_recon_code,'_tran');
	  set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Invalid recon !';
    leave me;
  end if;

  select database() into v_database_name;

  drop temporary table if exists recon_tmp_tthemepseudorows;
  drop temporary table if exists recon_tmp_tthemetrangid;
  drop temporary table if exists recon_tmp_tthemetranbrkpgid;
  drop temporary table if exists recon_tmp_tthemetranwithbrkpgid;

  drop temporary table if exists recon_tmp_tthemeindex;
  drop temporary table if exists recon_tmp_tthemesql;

  drop temporary table if exists recon_tmp_tthemesourceagg;
  drop temporary table if exists recon_tmp_tthemecomparisonagg;
  drop temporary table if exists recon_tmp_tthemetranagg;

  drop temporary table if exists recon_tmp_tthemematch;
  drop temporary table if exists recon_tmp_tthemematchdtl;
  drop temporary table if exists recon_tmp_tthemematchdtlgid;
  drop temporary table if exists recon_tmp_tthemematchdup;
  drop temporary table if exists recon_tmp_tthemematchparentgid;
  drop temporary table if exists recon_tmp_tthememanymatch;

  CREATE temporary TABLE recon_tmp_tthemematch(
    match_gid int unsigned NOT NULL AUTO_INCREMENT,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    matched_count int not null default 0,
    matched_value double(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    matched_json json NOT NULL,
    group_flag char(1) not null default 'N',
    dup_flag char(1) not null default 'N',
    ko_flag char(1) not null default 'N',
    PRIMARY KEY (match_gid),
    constraint idx_unique unique (tran_gid,tranbrkp_gid),
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid),
    key idx_group_flag(group_flag),
    key idx_dup_flag(dup_flag),
    key idx_ko_flag(ko_flag)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tthemematchdtl(
    matchdtl_gid int unsigned NOT NULL AUTO_INCREMENT,
    match_gid int unsigned NOT NULL default 0,
    parent_tran_gid int unsigned NOT NULL default 0,
    parent_tranbrkp_gid int unsigned NOT NULL default 0,
    tran_gid int unsigned NOT NULL default 0,
    tranbrkp_gid int unsigned not null default 0,
    ko_value decimal(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    src_comp_flag char(1) default null,
    dup_flag char(1) not null default 'N',
    ko_flag char(1) not null default 'N',
    PRIMARY KEY (matchdtl_gid),
    key idx_parent_tran_gid(parent_tran_gid),
    key idx_parent_gid(parent_tran_gid,parent_tranbrkp_gid),
    key idx_tran_gid(tran_gid),
    key idx_gid(tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tthememanymatch(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    source_value double(15,2) not null default 0,
    comparison_value double(15,2) not null default 0,
    matched_count int not null default 0,
    tran_mult tinyint not null default 0,
    matched_txt_json json NOT NULL,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tthemematchdtlgid(
    matchdtl_gid int unsigned NOT NULL,
    PRIMARY KEY (matchdtl_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tthemematchdup(
    matchdup_gid int unsigned NOT NULL AUTO_INCREMENT,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    rec_count int unsigned not null default 0,
    PRIMARY KEY (matchdup_gid),
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tthemematchparentgid(
    parent_tran_gid int unsigned NOT NULL,
    parent_tranbrkp_gid int unsigned not null default 0,
    rec_count int unsigned not null default 0,
    PRIMARY KEY (parent_tran_gid,parent_tranbrkp_gid),
    key idx_parent_tran_gid(parent_tran_gid),
    key idx_parent_tranbrkp_gid(parent_tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE TEMPORARY TABLE recon_tmp_tthemeindex(
    table_name varchar(255) not null,
    index_name varchar(255) not null,
    sys_flag char(1) not null default 'N',
    PRIMARY KEY (table_name,index_name),
    key idx_sys_flag(sys_flag)
  ) ENGINE = MyISAM;

  insert into recon_tmp_tthemeindex select 'recon_tmp_tthemesource','idx_tran_date','Y';
  insert into recon_tmp_tthemeindex select 'recon_tmp_tthemesource','idx_excp_value','Y';
  insert into recon_tmp_tthemeindex select 'recon_tmp_tthemesource','idx_recon_code','Y';
  insert into recon_tmp_tthemeindex select 'recon_tmp_tthemesource','idx_dataset_date','Y';

  insert into recon_tmp_tthemeindex select 'recon_tmp_tthemecomparison','idx_tran_date','Y';
  insert into recon_tmp_tthemeindex select 'recon_tmp_tthemecomparison','idx_excp_value','Y';
  insert into recon_tmp_tthemeindex select 'recon_tmp_tthemecomparison','idx_recon_code','Y';
  insert into recon_tmp_tthemeindex select 'recon_tmp_tthemecomparison','idx_dataset_date','Y';

  CREATE temporary TABLE recon_tmp_tthemepseudorows(
    row int unsigned NOT NULL,
    PRIMARY KEY (row)
  ) ENGINE = MyISAM;

  insert into recon_tmp_tthemepseudorows select 0 union select 1;

  CREATE temporary TABLE recon_tmp_tthemetrangid(
    tran_gid int unsigned NOT NULL,
    match_gid int not null default 0,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tthemetranbrkpgid(
    tranbrkp_gid int unsigned NOT NULL,
    excp_value double(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    tran_gid int not null default 0,
    match_gid int not null default 0,
    PRIMARY KEY (tranbrkp_gid),
    key idx_tran_gid(tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tthemetranwithbrkpgid(
    tran_gid int unsigned not null,
    tranbrkp_gid int unsigned NOT NULL,
    rec_count int not null default 0,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;


  CREATE temporary TABLE recon_tmp_tthemesql(
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
  and delete_flag = 'N'
  order by display_order;

  -- recon retails
  select
    recon_name,recontype_code,recon_rule_version
  into
    v_recon_name,v_recontype_code,v_recon_version
  from recon_mst_trecon
  where recon_code = in_recon_code
  and period_from <= curdate()
  and (until_active_flag = 'Y'
  or period_to >= curdate())
  and delete_flag = 'N';

  set v_recontype_code = ifnull(v_recontype_code,'');
  set v_recon_name = ifnull(v_recon_name,'');
  set v_recon_version = ifnull(v_recon_version,'');

  if v_recontype_code <> 'N' then
    set v_recon_value_flag = 'Y';
  else
    set v_recon_value_flag = 'N';
  end if;

  theme_block:begin
    declare theme_done int default 0;
    declare theme_cursor cursor for
      select
		    a.theme_code,
        a.theme_name,
        a.source_dataset_code,
        a.comparison_dataset_code
      from recon_mst_tthemehistory as a
      where a.recon_code = in_recon_code
      and a.theme_code = in_theme_code
      and a.recon_version = v_recon_version
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

      call pr_upd_job(in_job_gid,'P',concat('Applying Theme - ',v_theme_name),@msg,@result);

      -- v_source_dataset_type
      select
        dataset_type into v_source_dataset_type
      from recon_mst_trecondataset
      where recon_code = in_recon_code
      and dataset_code = v_source_dataset_code
      and dataset_type in ('B','T','S')
      and active_status = 'Y'
      and delete_flag = 'N';

      set v_source_dataset_type = ifnull(v_source_dataset_type,'B');

      -- v_comparison_dataset_type
      select
        dataset_type into v_comparison_dataset_type
      from recon_mst_trecondataset
      where recon_code = in_recon_code
      and dataset_code = v_comparison_dataset_code
      and dataset_type in ('B','T','S')
      and active_status = 'Y'
      and delete_flag = 'N';

      set v_comparison_dataset_type = ifnull(v_comparison_dataset_type,'T');

      -- source from tran table
      set v_source_head_sql = concat('insert into recon_tmp_tthemesource (',v_tran_fields,') ');

      set v_source_head_sql = concat(v_source_head_sql,' select ',v_tran_fields ,' from ',v_tran_table,' ');

      set v_source_head_sql = concat(v_source_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_source_head_sql = concat(v_source_head_sql,' and excp_value <> 0 and mapped_value = 0 ');
      else
        set v_source_head_sql = concat(v_source_head_sql,' and ko_gid = 0 ');
      end if;

      -- comparison from tran table
      set v_comparison_head_sql = concat('insert into recon_tmp_tthemecomparison (',v_tran_fields,') ');

      set v_comparison_head_sql = concat(v_comparison_head_sql,' select ',v_tran_fields ,' from ',v_tran_table,' ');

      set v_comparison_head_sql = concat(v_comparison_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and excp_value <> 0 and mapped_value = 0 ');
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and ko_gid = 0 ');
      end if;

      -- source from tranbrkp table
      set v_source_headbrkp_sql = concat('insert into recon_tmp_tthemesource (',v_tranbrkp_fields,') ');

      set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' select ',v_tranbrkp_fields ,' from ',v_tranbrkp_table,' ');

      set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and excp_value <> 0 and tran_gid > 0 ');
      else
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and ko_gid = 0 ');
      end if;

      -- comparison from tranbrkp table
      set v_comparison_headbrkp_sql = concat('insert into recon_tmp_tthemecomparison (',v_tranbrkp_fields,') ');

      set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' select ',v_tranbrkp_fields ,' from ',v_tranbrkp_table,' ');

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
              filter_value_flag,
              filter_value,
              open_parentheses_flag,
              close_parentheses_flag,
              join_condition
            from recon_mst_tthemefilterhistory
            where theme_code = v_theme_code
            and recon_version = v_recon_version
            and active_status = 'Y'
            and delete_flag = 'N'
            order by filter_applied_on,themefilter_seqno;

            declare continue handler for not found set themefilter_done=1;

            open themefilter_cursor;

            set v_sourcebase_filter = ' and (';
            set v_comparisonbase_filter = ' and (';

            themefilter_loop: loop
              fetch themefilter_cursor into v_filter_applied_on,
                                            v_filter_field,
                                            v_filter_criteria,
                                            v_filter_value_flag,
                                            v_filter_value,
                                            v_open_parentheses_flag,
                                            v_close_parentheses_flag,
                                            v_join_condition;

              if themefilter_done = 1 then leave themefilter_loop; end if;

              set v_filter_field = ifnull(v_filter_field,'');
              set v_filter_value_flag = ifnull(v_filter_value_flag,'Y');
              set v_filter_value = ifnull(v_filter_value,'');

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

              set v_themefilter_condition = concat(v_open_parentheses_flag,
                                                  fn_get_basefilterreconformat(in_recon_code,v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value_flag,v_filter_value),
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

          set v_sourcebase_filter = concat(v_sourcebase_filter,' 1 = 1) ');
          set v_comparisonbase_filter = concat(v_comparisonbase_filter,' 1 = 1) ');

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

          drop temporary table if exists recon_tmp_tthemesource;
          drop temporary table if exists recon_tmp_tthemecomparison;
          drop temporary table if exists recon_tmp_tthemesourcedup;

          /*
          drop table if exists recon_tmp_tthemesource;
          drop table if exists recon_tmp_tthemecomparison;
          */

          create temporary table recon_tmp_tthemesource select * from recon_trn_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tthemesource ENGINE = MyISAM;
          alter table recon_tmp_tthemesource add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tthemesource(excp_value);
          create index idx_tran_date on recon_tmp_tthemesource(tran_date);
          create index idx_recon_code on recon_tmp_tthemesource(recon_code);
          create index idx_dataset_code on recon_tmp_tthemesource(recon_code,dataset_code);

          create temporary table recon_tmp_tthemecomparison select * from recon_trn_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tthemecomparison ENGINE = MyISAM;
          alter table recon_tmp_tthemecomparison add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tthemecomparison(excp_value);
          create index idx_tran_date on recon_tmp_tthemecomparison(tran_date);
          create index idx_recon_code on recon_tmp_tthemecomparison(recon_code);
          create index idx_dataset_cdoe on recon_tmp_tthemecomparison(recon_code,dataset_code);

          create temporary table recon_tmp_tthemesourcedup select * from recon_trn_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tthemesourcedup add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tthemesourcedup(excp_value);
          create index idx_tran_date on recon_tmp_tthemesourcedup(tran_date);
          create index idx_dataset_code on recon_tmp_tthemesourcedup(recon_code,dataset_code);
          alter table recon_tmp_tthemesourcedup ENGINE = MyISAM;

          drop temporary table if exists recon_tmp_tthemesourceagg;
          drop temporary table if exists recon_tmp_tthemecomparisonagg;
          drop temporary table if exists recon_tmp_tthemetranagg;

          /*
          drop table if exists recon_tmp_tthemesourceagg;
          drop table if exists recon_tmp_tthemecomparisonagg;
          drop table if exists recon_tmp_tthemetranagg;
          */

          -- grouping temp table movement
          -- create agg temp tables
          create temporary table recon_tmp_tthemesourceagg select * from recon_rpt_tthemeagg where 1 = 2;
          alter table recon_tmp_tthemesourceagg ENGINE = MyISAM;
          alter table recon_tmp_tthemesourceagg add primary key(themeagg_gid);
          alter table recon_tmp_tthemesourceagg modify column themeagg_gid int unsigned AUTO_INCREMENT;
          alter table recon_tmp_tthemesourceagg add match_gid int not null default 0;

          create index idx_tran_value on recon_tmp_tthemesourceagg(tran_value);
          create index idx_excp_value on recon_tmp_tthemesourceagg(excp_value);
          create index idx_tran_date on recon_tmp_tthemesourceagg(tran_date);
          create index idx_match_gid on recon_tmp_tthemesourceagg(match_gid);

          create temporary table recon_tmp_tthemecomparisonagg select * from recon_rpt_tthemeagg where 1 = 2;
          alter table recon_tmp_tthemecomparisonagg ENGINE = MyISAM;
          alter table recon_tmp_tthemecomparisonagg add primary key(themeagg_gid);
          alter table recon_tmp_tthemecomparisonagg modify column themeagg_gid int unsigned AUTO_INCREMENT;
          alter table recon_tmp_tthemecomparisonagg add match_gid int not null default 0;

          create index idx_tran_value on recon_tmp_tthemecomparisonagg(tran_value);
          create index idx_excp_value on recon_tmp_tthemecomparisonagg(excp_value);
          create index idx_tran_date on recon_tmp_tthemecomparisonagg(tran_date);
          create index idx_match_gid on recon_tmp_tthemecomparisonagg(match_gid);

          create temporary table recon_tmp_tthemetranagg select * from recon_rpt_tthemeagg where 1 = 2;
          alter table recon_tmp_tthemetranagg ENGINE = MyISAM;
          alter table recon_tmp_tthemetranagg add primary key(themeagg_gid);
          alter table recon_tmp_tthemetranagg modify column themeagg_gid int unsigned AUTO_INCREMENT;

          create index idx_rec_count on recon_tmp_tthemetranagg(rec_count);

          -- index table
          delete from recon_tmp_tthemeindex where sys_flag <> 'Y';
          truncate recon_tmp_tthemesql;

          condition_block:begin
            declare condition_done int default 0;
            declare condition_cursor cursor for
            select
              a.source_field,a.extraction_criteria,
              a.comparison_field,a.comparison_criteria,
              a.open_parentheses_flag,a.close_parentheses_flag,
              a.join_condition
            from recon_mst_tthemeconditionhistory as a
            where a.theme_code = v_theme_code
            and a.recon_version = v_recon_version
            and a.active_status = 'Y'
            and a.delete_flag = 'N'
            order by themecondition_seqno;

            declare continue handler for not found set condition_done=1;

            open condition_cursor;

            condition_loop: loop
              fetch condition_cursor into v_source_field,v_extraction_criteria,
                                     v_comparison_field,v_comparison_criteria,
                                     v_open_parentheses_flag,v_close_parentheses_flag,v_join_condition;
              if condition_done = 1 then leave condition_loop; end if;

              set v_index_name = concat('idx_',v_source_field);

              -- recon_tmp_tthemesource
              if not exists(select index_name from recon_tmp_tthemeindex
                            WHERE table_name = 'recon_tmp_tthemesource'
                            and index_name = v_index_name) then

                if substr(v_source_field,1,3) = 'col' then
                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tthemesource(',v_source_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tthemesource(',v_source_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tthemeindex(table_name,index_name) select 'recon_tmp_tthemesource',v_index_name;
              end if;

              -- recon_tmp_tthemesourceagg
              if mid(v_source_field,1,3) = 'col' then
                if not exists(select index_name from recon_tmp_tthemeindex
                            WHERE table_name = 'recon_tmp_tthemesourceagg'
                            and index_name = v_index_name) then

                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tthemesourceagg(',v_source_field,'(255))');

                  call pr_run_sql(v_index_sql,@msg,@result);

                  insert into recon_tmp_tthemeindex(table_name,index_name) select 'recon_tmp_tthemesourceagg',v_index_name;
                end if;

                -- recon_tmp_tthemetranagg
                if not exists(select index_name from recon_tmp_tthemeindex
                            WHERE table_name = 'recon_tmp_tthemetranagg'
                            and index_name = v_index_name) then

                  if substr(v_source_field,1,3) = 'col' then
                    set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tthemetranagg(',v_source_field,'(255))');
                  else
                    set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tthemetranagg(',v_source_field,')');
                  end if;

                  call pr_run_sql(v_index_sql,@msg,@result);

                  insert into recon_tmp_tthemeindex(table_name,index_name) select 'recon_tmp_tthemetranagg',v_index_name;
                end if;
              end if;

              -- recon_tmp_tthemecomparison
              set v_index_name = concat('idx_',v_comparison_field);

              if not exists(select index_name from recon_tmp_tthemeindex
                            WHERE table_name = 'recon_tmp_tthemecomparison'
                            and index_name = v_index_name) then

                if substr(v_comparison_field,1,3) = 'col' then
                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tthemecomparison(',v_comparison_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tthemecomparison(',v_comparison_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tthemeindex(table_name,index_name) select 'recon_tmp_tthemecomparison',v_index_name;
              end if;

              -- recon_tmp_tthemecomparisonagg
              if mid(v_comparison_field,1,3) = 'col' then
                if not exists(select index_name from recon_tmp_tthemeindex
                            WHERE table_name = 'recon_tmp_tthemecomparisonagg'
                            and index_name = v_index_name) then

                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tthemecomparisonagg(',v_comparison_field,'(255))');

                  call pr_run_sql(v_index_sql,@msg,@result);

                  insert into recon_tmp_tthemeindex(table_name,index_name) select 'recon_tmp_tthemecomparisonagg',v_index_name;
                end if;

                -- recon_tmp_tthemetranagg
                if not exists(select index_name from recon_tmp_tthemeindex
                            WHERE table_name = 'recon_tmp_tthemetranagg'
                            and index_name = v_index_name) then

                  if substr(v_comparison_field,1,3) = 'col' then
                    set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tthemetranagg(',v_comparison_field,'(255))');
                  else
                    set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tthemetranagg(',v_comparison_field,')');
                  end if;

                  call pr_run_sql(v_index_sql,@msg,@result);

                  insert into recon_tmp_tthemeindex(table_name,index_name) select 'recon_tmp_tthemetranagg',v_index_name;
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
                set v_sql = concat(v_sql,'update recon_tmp_tthemesource set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format);

                insert into recon_tmp_tthemesql(table_type,sql_query) values ('S',v_sql);

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
                set v_sql = concat(v_sql,'update recon_tmp_tthemecomparison set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');

                if v_recontype_code <> 'N' then
                  set v_sql = concat(v_sql,'where true ');
                end if;

                insert into recon_tmp_tthemesql(table_type,sql_query) values ('C',v_sql);

                /*
								if v_manytomany_match_flag = 'Y' and v_recontype_code <> 'N' and v_recontype_code <> 'V' then
									set v_sql = '';
									set v_sql = concat(v_sql,'update recon_tmp_tthemecomparison set ');
									set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');
                  set v_sql = concat(v_sql,'where true ');

									insert into recon_tmp_tthemesql(table_type,sql_query) values ('C',v_sql);
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

          truncate recon_tmp_tthemesource;
          truncate recon_tmp_tthemecomparison;

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
            select sql_query from recon_tmp_tthemesql;
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
          truncate recon_tmp_tthemepseudorows;
          insert into recon_tmp_tthemepseudorows select 0 union select 1;

          -- get target addtional group field
          if v_group_flag = 'Y' then
            -- grp on source
            select
              group_concat(concat('a.',grp_field)) into v_grp_field
            from recon_mst_tthemegrpfieldhistory
            where theme_code = v_theme_code
            and recon_version = v_recon_version
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
            select
              group_concat(concat('b.',grp_field)) into v_grp_field
            from recon_mst_tthemegrpfieldhistory
            where theme_code = v_theme_code
            and recon_version = v_recon_version
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

          alter table recon_tmp_tthemesource add match_flag char(1) not null default 'N';
          alter table recon_tmp_tthemesource add match_gid int not null default 0;

          alter table recon_tmp_tthemecomparison add match_flag char(1) not null default 'N';
          alter table recon_tmp_tthemecomparison add match_gid int not null default 0;

          alter table recon_tmp_tthemecomparison ENGINE = MyISAM;
          alter table recon_tmp_tthemesource ENGINE = MyISAM;

          -- start matching grouping part
					-- many to many match
					if v_manytomany_match_flag = 'Y' then
						set v_match_sql = 'insert ignore into recon_tmp_tthememanymatch (tran_gid,tranbrkp_gid,matched_count,';
						set v_match_sql = concat(v_match_sql,'tran_mult,source_value,comparison_value,matched_txt_json) ');
						set v_match_sql = concat(v_match_sql,'select ');
						set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,a.tran_mult,');
						set v_match_sql = concat(v_match_sql,'a.excp_value as source_value,sum(b.excp_value*b.tran_mult) as comparison_value,');

						set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[',char(39),',');
						set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
						set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(b.tran_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(b.tran_mult as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"src_comp_flag":"C",');
						set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(b.excp_value as nchar),',char(39));
						set v_match_sql = concat(v_match_sql,'}',char(39),' order by b.tran_gid,b.tranbrkp_gid),');
						set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json) as matched_json ');

						set v_match_sql = concat(v_match_sql,'from recon_tmp_tthemesource as a ');
						set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tthemecomparison as b ');
						set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

						set v_match_sql = concat(v_match_sql,v_theme_condition,' ');

            set v_match_sql = concat(v_match_sql,"where a.match_flag = 'N' and b.match_flag = 'N' ");

						set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_theme_groupby,' ');

						call pr_run_sql(v_match_sql,@msg,@result);

						-- insert in match table
						set v_match_sql = 'insert into recon_tmp_tthemematch (group_flag,tran_gid,tranbrkp_gid,matched_count,matched_value,matched_json) ';
						set v_match_sql = concat(v_match_sql,'select ',char(39),'M',char(39),',');
						set v_match_sql = concat(v_match_sql,'max(tran_gid),max(tranbrkp_gid),sum(matched_count)+count(*) as matched_count,');
						set v_match_sql = concat(v_match_sql,'comparison_value as matched_value,');

						set v_match_sql = concat(v_match_sql,'JSON_MERGE_PRESERVE(');
						set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[',char(39),',');
						set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
						set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(tran_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(tranbrkp_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(tran_mult as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');
						set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(source_value as nchar),',char(39));
						set v_match_sql = concat(v_match_sql,'}',char(39),'),');
						set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json),matched_txt_json) as matched_json ');

						set v_match_sql = concat(v_match_sql,'from recon_tmp_tthememanymatch ');

						if v_recontype_code <> 'N' then
							set v_match_sql = concat(v_match_sql,'group by matched_txt_json,comparison_value ');
              /*
              if (v_recontype_code <> 'I' and v_recontype_code <> 'V') or v_reversal_flag = 'Y' then
                -- contra
							  set v_match_sql = concat(v_match_sql,'having sum(source_value*tran_mult) <> (comparison_value*-1) ');
              else
                -- mirror
							  set v_match_sql = concat(v_match_sql,'having sum(source_value*tran_mult) <> comparison_value ');
              end if;
              */
						else
							set v_match_sql = concat(v_match_sql,'group by matched_txt_json ');
						end if;

						call pr_run_sql(v_match_sql,@msg,@result);

						select max(matched_count) into v_count from recon_tmp_tthemematch;
						set v_count = ifnull(v_count,0);

						truncate recon_tmp_tthemepseudorows;

						if v_count >= 2 then
							insert into recon_tmp_tthemepseudorows select row from pseudo_rows1 where row <= v_count;
						else
							insert into recon_tmp_tthemepseudorows select 0 union select 1;
						end if;

						insert into recon_tmp_tthemematchdtl (match_gid,parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult,src_comp_flag)
							select
                match_gid,
								tran_gid as parent_tran_gid,
								tranbrkp_gid as parent_tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tran_gid'))) AS tran_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].ko_value'))) AS ko_value,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tran_mult'))) AS tran_mult,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].src_comp_flag'))) AS src_comp_flag
							FROM recon_tmp_tthemematch
							JOIN recon_tmp_tthemepseudorows
							where group_flag = 'M'
							HAVING tran_gid IS NOT NULL;

						-- clear matched records
						truncate recon_tmp_tthemetrangid;

						insert into recon_tmp_tthemetrangid (match_gid,tran_gid)
							select distinct match_gid,tran_gid from recon_tmp_tthemematchdtl where tran_gid > 0 and tranbrkp_gid = 0;

						update recon_tmp_tthemesource as a
            inner join recon_tmp_tthemetrangid as b on a.tran_gid = b.tran_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						update recon_tmp_tthemecomparison as a
            inner join recon_tmp_tthemetrangid as b on a.tran_gid = b.tran_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						truncate recon_tmp_tthemetranbrkpgid;

						insert into recon_tmp_tthemetranbrkpgid (match_gid,tranbrkp_gid)
							select distinct match_gid,tranbrkp_gid from recon_tmp_tthemematchdtl where tranbrkp_gid > 0;

						update recon_tmp_tthemesource as a
            inner join recon_tmp_tthemetranbrkpgid as b on a.tranbrkp_gid = b.tranbrkp_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						update recon_tmp_tthemecomparison as a
            inner join recon_tmp_tthemetranbrkpgid as b on a.tranbrkp_gid = b.tranbrkp_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						truncate recon_tmp_tthemetrangid;
						truncate recon_tmp_tthemetranbrkpgid;
					end if;

					 -- one to many match
           set v_manytomany_match_flag = 'N';

					 if v_group_flag = 'Y' and v_manytomany_match_flag = 'N' then
            set v_match_sql = 'insert into recon_tmp_tthemematch (group_flag,tran_gid,tranbrkp_gid,matched_count,matched_value,tran_mult,matched_json) ';
            set v_match_sql = concat(v_match_sql,'select ',char(39),'Y',char(39),',');
            set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,');

            if v_recontype_code <> 'N' then
              set v_match_sql = concat(v_match_sql,'a.excp_value as matched_value,a.tran_mult,');
            else
              set v_match_sql = concat(v_match_sql,'0 as matched_value,0 as tran_mult,');
            end if;

            set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[');
            set v_match_sql = concat(v_match_sql,'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(a.tranbrkp_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(a.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');

            if v_recontype_code <> 'N' then
              set v_match_sql = concat(v_match_sql,'"ko_value":', char(39),',cast(a.excp_value as nchar),',char(39));
            else
              set v_match_sql = concat(v_match_sql,'"ko_value":', char(39),',cast(0 as nchar),',char(39));
            end if;

            set v_match_sql = concat(v_match_sql,'},',char(39),',');
            set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(b.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(b.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"src_comp_flag":"C",');

            if v_recontype_code <> 'N' then
              set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(b.excp_value as nchar),',char(39));
            else
              set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(0 as nchar),',char(39));
            end if;

            set v_match_sql = concat(v_match_sql,'}',char(39),'),');
            set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json) as matched_json ');

            set v_match_sql = concat(v_match_sql,'from recon_tmp_tthemesource as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tthemecomparison as b ');
            set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

						set v_match_sql = concat(v_match_sql,'where 1 = 1 ');
            set v_match_sql = concat(v_match_sql,v_theme_condition,' ');

            set v_match_sql = concat(v_match_sql,"and a.match_flag = 'N' and b.match_flag = 'N' ");

            if v_recontype_code <> 'N' then
              set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_theme_groupby,' ');
            else
              set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid',v_theme_groupby,' ');
            end if;

            set v_match_sql = concat(v_match_sql,'having count(*) > 1 ');

            /*
            if v_recontype_code <> 'N' then
              if (v_recontype_code <> 'I' and v_recontype_code <> 'V') or v_reversal_flag = 'Y' then
                -- contra
                set v_match_sql = concat(v_match_sql,'and a.excp_value*a.tran_mult <> sum(b.excp_value*b.tran_mult)*-1 ');
              else
                -- mirror
                set v_match_sql = concat(v_match_sql,'and a.excp_value*a.tran_mult <> sum(b.excp_value*b.tran_mult) ');
              end if;
            end if;
            */

            -- run match sql one to many
            call pr_run_sql(v_match_sql,@msg,@result);

            select max(matched_count) into v_count from recon_tmp_tthemematch;
            set v_count = ifnull(v_count,0);

            truncate recon_tmp_tthemepseudorows;

            if v_count >= 2 then
              insert into recon_tmp_tthemepseudorows select row from pseudo_rows1 where row <= v_count;
            else
              insert into recon_tmp_tthemepseudorows select 0 union select 1;
            end if;

            insert into recon_tmp_tthemematchdtl (match_gid,parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult,src_comp_flag)
              select
                match_gid,
                tran_gid as parent_tran_gid,
                tranbrkp_gid as parent_tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tran_gid'))) AS tran_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].ko_value'))) AS ko_value,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tran_mult'))) AS tran_mult,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].src_comp_flag'))) AS src_comp_flag
              FROM recon_tmp_tthemematch
              JOIN recon_tmp_tthemepseudorows
              where group_flag = 'Y'
              HAVING tran_gid IS NOT NULL;

						-- clear matched records
						truncate recon_tmp_tthemetrangid;

						insert into recon_tmp_tthemetrangid (match_gid,tran_gid)
							select distinct match_gid,tran_gid from recon_tmp_tthemematchdtl where tran_gid > 0 and tranbrkp_gid = 0;

						update recon_tmp_tthemesource as a
            inner join recon_tmp_tthemetrangid as b on a.tran_gid = b.tran_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						update recon_tmp_tthemecomparison as a
            inner join recon_tmp_tthemetrangid as b on a.tran_gid = b.tran_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						truncate recon_tmp_tthemetranbrkpgid;

						insert into recon_tmp_tthemetranbrkpgid (match_gid,tranbrkp_gid)
							select distinct match_gid,tranbrkp_gid from recon_tmp_tthemematchdtl where tranbrkp_gid > 0;

						update recon_tmp_tthemesource as a
            inner join recon_tmp_tthemetranbrkpgid as b on a.tranbrkp_gid = b.tranbrkp_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						update recon_tmp_tthemecomparison as a
            inner join recon_tmp_tthemetranbrkpgid as b on a.tranbrkp_gid = b.tranbrkp_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						truncate recon_tmp_tthemetrangid;
						truncate recon_tmp_tthemetranbrkpgid;
					 end if;

					-- one to one match
          if v_manytomany_match_flag = 'N' then
						set v_match_sql = 'insert into recon_tmp_tthemematch (group_flag,tran_gid,tranbrkp_gid,matched_count,matched_value,matched_json) ';
						set v_match_sql = concat(v_match_sql,'select ',char(39),'N',char(39),',');
						set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,');

						if v_recontype_code <> 'N' then
							set v_match_sql = concat(v_match_sql,'a.excp_value as matched_value,');
						else
							set v_match_sql = concat(v_match_sql,'0 as matched_value,');
						end if;

						set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[');
						set v_match_sql = concat(v_match_sql,'{');
						set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(a.tranbrkp_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(a.tran_mult as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');

						if v_recontype_code <> 'N' then
							set v_match_sql = concat(v_match_sql,'"ko_value":', char(39),',cast(a.excp_value as nchar),',char(39));
						else
							set v_match_sql = concat(v_match_sql,'"ko_value":', char(39),',cast(0 as nchar),',char(39));
						end if;

						set v_match_sql = concat(v_match_sql,'},');
						set v_match_sql = concat(v_match_sql,'{');
						set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(b.tran_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(b.tran_mult as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"src_comp_flag":"C",');

						if v_recontype_code <> 'N' then
							set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(b.excp_value as nchar),',char(39));
						else
							set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(0 as nchar),',char(39));
						end if;

						set v_match_sql = concat(v_match_sql,'}');
						set v_match_sql = concat(v_match_sql,']',char(39),') as json) as matched_json ');
						set v_match_sql = concat(v_match_sql,'from recon_tmp_tthemesource as a ');
						set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tthemecomparison as b ');
						set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

						set v_match_sql = concat(v_match_sql,v_theme_condition,' ');

            set v_match_sql = concat(v_match_sql,"where a.match_flag = 'N' and b.match_flag = 'N' ");

            /*
						if v_recontype_code <> 'N' then
							set v_match_sql = concat(v_match_sql,'and a.excp_value <> b.excp_value ');

						  set v_match_sql = concat(v_match_sql,'where true ');
						end if;
            */

						set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid ');
						set v_match_sql = concat(v_match_sql,'having count(*) = 1 ');

            -- run match sql one to one
						call pr_run_sql(v_match_sql,@msg,@result);

						-- select v_source_sql,v_comparison_sql,v_match_sql;
						-- leave me;

						truncate recon_tmp_tthemepseudorows;

						select max(matched_count) into v_count from recon_tmp_tthemematch;
						set v_count = ifnull(v_count,0);

						if v_count >= 2 then
							insert into recon_tmp_tthemepseudorows select row from pseudo_rows1 where row <= v_count;
						else
							insert into recon_tmp_tthemepseudorows select 0 union select 1;
						end if;

						insert into recon_tmp_tthemematchdtl (match_gid,parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult,src_comp_flag)
							select
                match_gid,
								tran_gid as parent_tran_gid,
								tranbrkp_gid as parent_tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tran_gid'))) AS tran_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].ko_value'))) AS ko_value,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tran_mult'))) AS tran_mult,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemematch.matched_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].src_comp_flag'))) AS src_comp_flag
							FROM recon_tmp_tthemematch
							JOIN recon_tmp_tthemepseudorows
							where group_flag = 'N'
							HAVING tran_gid IS NOT NULL;

						-- clear matched records
						truncate recon_tmp_tthemetrangid;

						insert into recon_tmp_tthemetrangid (match_gid,tran_gid)
							select distinct match_gid,tran_gid from recon_tmp_tthemematchdtl where tran_gid > 0 and tranbrkp_gid = 0;

						update recon_tmp_tthemesource as a
            inner join recon_tmp_tthemetrangid as b on a.tran_gid = b.tran_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						update recon_tmp_tthemecomparison as a
            inner join recon_tmp_tthemetrangid as b on a.tran_gid = b.tran_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						truncate recon_tmp_tthemetranbrkpgid;

						insert into recon_tmp_tthemetranbrkpgid (match_gid,tranbrkp_gid)
							select distinct match_gid,tranbrkp_gid from recon_tmp_tthemematchdtl where tranbrkp_gid > 0;

						update recon_tmp_tthemesource as a
            inner join recon_tmp_tthemetranbrkpgid as b on a.tranbrkp_gid = b.tranbrkp_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						update recon_tmp_tthemecomparison as a
            inner join recon_tmp_tthemetranbrkpgid as b on a.tranbrkp_gid = b.tranbrkp_gid
            set
              a.match_flag = 'M'
            where a.match_flag = 'N';

						truncate recon_tmp_tthemetrangid;
						truncate recon_tmp_tthemetranbrkpgid;
          end if;

          -- duplicate validation
          insert ignore into recon_tmp_tthemematchdup (tran_gid,tranbrkp_gid,rec_count)
            select tran_gid,tranbrkp_gid,count(*) from recon_tmp_tthemematchdtl
            where src_comp_flag = 'S'
            group by tran_gid,tranbrkp_gid
            having count(*) > 1;

          insert ignore into recon_tmp_tthemematchdup (tran_gid,tranbrkp_gid,rec_count)
            select tran_gid,tranbrkp_gid,count(*) from recon_tmp_tthemematchdtl
            where src_comp_flag = 'C'
            group by tran_gid,tranbrkp_gid
            having count(*) > 1;

          insert ignore into recon_tmp_tthemematchparentgid(parent_tran_gid,parent_tranbrkp_gid)
            select b.parent_tran_gid,b.parent_tranbrkp_gid from recon_tmp_tthemematchdup as a
            inner join recon_tmp_tthemematchdtl as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
            where src_comp_flag = 'S'
            group by b.parent_tran_gid,b.parent_tranbrkp_gid;

          insert ignore into recon_tmp_tthemematchparentgid(parent_tran_gid,parent_tranbrkp_gid)
            select b.parent_tran_gid,b.parent_tranbrkp_gid from recon_tmp_tthemematchdup as a
            inner join recon_tmp_tthemematchdtl as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
            where src_comp_flag = 'C'
            group by b.parent_tran_gid,b.parent_tranbrkp_gid;

          update recon_tmp_tthemematch as a
          inner join recon_tmp_tthemematchparentgid as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
          set a.dup_flag = 'Y';

          update recon_tmp_tthemematch set
            matched_value = abs(matched_value),
            ko_flag = 'Y'
          where dup_flag = 'N';

          -- delete duplicate records in recon_tmp_tthemematchdtl
          delete from recon_tmp_tthemematchdtl
          where (parent_tran_gid,parent_tranbrkp_gid)
          in
          (
            select
              parent_tran_gid,
              parent_tranbrkp_gid
            from recon_tmp_tthemematchparentgid
          );

					-- update matched records
					truncate recon_tmp_tthemetrangid;

					insert into recon_tmp_tthemetrangid (tran_gid,match_gid)
						select distinct tran_gid,match_gid from recon_tmp_tthemematchdtl where tran_gid > 0 and tranbrkp_gid = 0;

					update recon_tmp_tthemesource as a
          inner join recon_tmp_tthemetrangid as b on a.tran_gid = b.tran_gid
          set
						a.match_gid = b.match_gid,
            a.match_flag = 'Y'
					where a.match_flag = 'M'
          and a.match_gid = 0;

					update recon_tmp_tthemecomparison as a
          inner join recon_tmp_tthemetrangid as b on a.tran_gid = b.tran_gid
          set
						a.match_gid = b.match_gid,
            a.match_flag = 'Y'
					where a.match_flag = 'M'
          and a.match_gid = 0;

					truncate recon_tmp_tthemetranbrkpgid;

					insert into recon_tmp_tthemetranbrkpgid (tranbrkp_gid,match_gid)
						select distinct tranbrkp_gid,match_gid from recon_tmp_tthemematchdtl where tranbrkp_gid > 0;

					update recon_tmp_tthemesource as a
          inner join recon_tmp_tthemetranbrkpgid as b on a.tranbrkp_gid = b.tranbrkp_gid
          set
            a.match_gid = b.match_gid,
						a.match_flag = 'Y'
					where a.match_flag = 'M'
          and a.match_gid = 0;

					update recon_tmp_tthemecomparison as a
          inner join recon_tmp_tthemetranbrkpgid as b on a.tranbrkp_gid = b.tranbrkp_gid
          set
            a.match_gid = b.match_gid,
						a.match_flag = 'Y'
					where a.match_flag = 'M'
          and a.match_gid = 0;

					truncate recon_tmp_tthemetrangid;
					truncate recon_tmp_tthemetranbrkpgid;

          -- start agg part
          -- source agg block
          set v_source_agg_field = '';
          set v_source_aggfunction_field = '';

          agg_source_block:begin
            declare agg_source_done int default 0;
            declare agg_source_cursor cursor for
            select
              recon_field,
              themeagg_function,
              themeagg_field,
              themeagg_field_type
            from recon_mst_tthemeaggfieldhistory
            where theme_code = v_theme_code
            and recon_version = v_recon_version
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

          if v_source_agg_field = '' then
            set v_source_agg_field = ',col128';
          end if;

          if v_source_aggfunction_field = '' then
            set v_source_aggfunction_field = concat(",'1'");
          end if;

					-- move records in source agg table
					set v_sql = concat('insert into recon_tmp_tthemesourceagg (match_gid',v_source_agg_field,',');
					set v_sql = concat(v_sql,'rec_count,themeagg_json,src_comp_flag) ');
					set v_sql = concat(v_sql,'select match_gid',v_source_aggfunction_field,',');
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

					set v_sql = concat(v_sql,'from recon_tmp_tthemesource ');

					set v_sql = concat(v_sql,'where match_gid > 0 ');
					set v_sql = concat(v_sql,'group by match_gid');

					-- set v_sql = concat(v_sql,'group by ',substr(replace(v_source_groupby,'a.',''),2));

          call pr_run_sql(v_sql,@result,@msg);

          -- comparison agg block
          set v_comparison_agg_field = '';
          set v_comparison_aggfunction_field = '';

          agg_comparison_block:begin
            declare agg_comparison_done int default 0;
            declare agg_comparison_cursor cursor for
            select
              recon_field,
              themeagg_function,
              themeagg_field,
              themeagg_field_type
            from recon_mst_tthemeaggfieldhistory
            where theme_code = v_theme_code
            and recon_version = v_recon_version
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

          if v_comparison_agg_field = '' then
            set v_comparison_agg_field = ',col128';
          end if;

          if v_comparison_aggfunction_field = '' then
            set v_comparison_aggfunction_field = concat(",'1'");
          end if;

					-- move records in comparison agg table
					set v_sql = concat('insert into recon_tmp_tthemecomparisonagg (match_gid',v_comparison_agg_field,',');
					set v_sql = concat(v_sql,'rec_count,themeagg_json,src_comp_flag) ');
					set v_sql = concat(v_sql,'select match_gid',v_comparison_aggfunction_field,',');
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

					set v_sql = concat(v_sql,'from recon_tmp_tthemecomparison ');

					set v_sql = concat(v_sql,'where match_gid > 0 ');
					set v_sql = concat(v_sql,'group by match_gid');

					-- set v_sql = concat(v_sql,'group by ',substr(replace(v_comparison_groupby,'b.',''),2));

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
            from recon_mst_tthemeaggconditionhistory as a
            left join recon_mst_tthemeaggfieldhistory as b on a.themeagg_field = b.themeagg_field
              and a.theme_code = b.theme_code
              and a.recon_version = b.recon_version
              and a.themeagg_applied_on = b.themeaggfield_applied_on
              and b.active_status = 'Y'
              and b.delete_flag = 'N'
            where a.theme_code = v_theme_code
            and a.recon_version = v_recon_version
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
          set v_sql = concat('insert into recon_tmp_tthemetranagg(rec_count,themeagg_json) ');
          set v_sql = concat(v_sql,'select (a.rec_count+b.rec_count),JSON_MERGE_PRESERVE(a.themeagg_json,b.themeagg_json) ');
          set v_sql = concat(v_sql,'from recon_tmp_tthemesourceagg as a ');
          set v_sql = concat(v_sql,'inner join recon_tmp_tthemecomparisonagg as b on a.match_gid = b.match_gid ');
          set v_sql = concat(v_sql,'where 1 = 1 ',v_themeagg_condition);

          call pr_run_sql(v_sql,@result,@msg);

          -- insert in tranbrkp_gid table
          select max(rec_count) into v_count from recon_tmp_tthemetranagg;
          set v_count = ifnull(v_count,0);

          truncate recon_tmp_tthemepseudorows;

          if v_count >= 2 then
            insert into recon_tmp_tthemepseudorows select row from pseudo_rows1 where row <= v_count;
          else
            insert into recon_tmp_tthemepseudorows select 0 union select 1;
          end if;

          truncate recon_tmp_tthemetranwithbrkpgid;

					insert ignore into recon_tmp_tthemetranwithbrkpgid (tran_gid,tranbrkp_gid)
						select
							JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemetranagg.themeagg_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tran_gid'))) AS tran_gid,
							JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tthemetranagg.themeagg_json, CONCAT('$[', recon_tmp_tthemepseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid
						FROM recon_tmp_tthemetranagg
						JOIN recon_tmp_tthemepseudorows
						where rec_count > 0
						HAVING tran_gid IS NOT NULL;

          -- update theme
          -- tran table
					set v_sql = concat("
						update ",v_tran_table," as a set
							a.theme_code = concat(if(a.theme_code = '','",v_theme_name,"',
							concat(a.theme_code,',','",v_theme_name,"')))
						where a.tran_gid in (select b.tran_gid from recon_tmp_tthemetranwithbrkpgid as b
              where a.tran_gid = b.tran_gid and b.tranbrkp_gid = 0)");

					call pr_run_sql(v_sql,@msg,@result);

          -- tranbrkp table
					set v_sql = concat("
						update ",v_tranbrkp_table," as a set
							a.theme_code = concat(if(a.theme_code = '','",v_theme_name,"',
							concat(a.theme_code,',','",v_theme_name,"')))
						where (a.tran_gid,a.tranbrkp_gid) in (select b.tran_gid,b.tranbrkp_gid from recon_tmp_tthemetranwithbrkpgid as b
              where a.tran_gid = b.tran_gid
              and a.tranbrkp_gid = b.tranbrkp_gid
              and b.tranbrkp_gid > 0)");

					call pr_run_sql(v_sql,@msg,@result);

          -- delete in index table
          delete from recon_tmp_tthemeindex
          where table_name in ('recon_tmp_tthemesourceagg','recon_tmp_tthemecomparisonagg','recon_tmp_tthemetranagg');

          truncate recon_tmp_tthemesource;
          truncate recon_tmp_tthemecomparison;
          truncate recon_tmp_tthemesourcedup;
          truncate recon_tmp_tthemetrangid;
          truncate recon_tmp_tthemetranbrkpgid;

          truncate recon_tmp_tthemesourceagg;
          truncate recon_tmp_tthemecomparisonagg;
          truncate recon_tmp_tthemetranagg;

          drop temporary table if exists recon_tmp_tthemesource;
          drop temporary table if exists recon_tmp_tthemecomparison;

          drop temporary table if exists recon_tmp_tthemesourceagg;
          drop temporary table if exists recon_tmp_tthemecomparisonagg;
          drop temporary table if exists recon_tmp_tthemetranagg;
    end loop theme_loop;

    close theme_cursor;
  end theme_block;

  set out_result = v_count;

  set out_msg = 'Theme updated successfully !';

  drop temporary table if exists recon_tmp_tthemesource;
  drop temporary table if exists recon_tmp_tthemecomparison;
  drop temporary table if exists recon_tmp_tthemesourcedup;
  drop temporary table if exists recon_tmp_tthemepseudorows;
  drop temporary table if exists recon_tmp_tthemetrangid;
  drop temporary table if exists recon_tmp_tthemetranbrkpgid;
  drop temporary table if exists recon_tmp_tthemetranwithbrkpgid;
  drop temporary table if exists recon_tmp_tthemeindex;
  drop temporary table if exists recon_tmp_tthemesql;

  drop temporary table if exists recon_tmp_tthemematch;
  drop temporary table if exists recon_tmp_tthemematchdtl;
  drop temporary table if exists recon_tmp_tthemematchdtlgid;
  drop temporary table if exists recon_tmp_tthemematchdup;
  drop temporary table if exists recon_tmp_tthemematchparentgid;
  drop temporary table if exists recon_tmp_tthememanymatch;
end $$

DELIMITER ;