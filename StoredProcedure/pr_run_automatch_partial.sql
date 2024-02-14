DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_automatch_partial` $$
CREATE PROCEDURE `pr_run_automatch_partial`(
  in in_recon_code text,
  in in_rule_code text,
  in in_job_gid int,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_acc_mode varchar(32) default '';
  declare v_source_acc_mode varchar(32) default '';
  declare v_comparison_acc_mode varchar(32) default '';
  declare v_base_acc_mode varchar(32) default '';
  declare v_recontype_code varchar(16) default '';

  declare v_source_head_sql text default '';
  declare v_comparison_head_sql text default '';

  declare v_source_headbrkp_sql text default '';
  declare v_comparison_headbrkp_sql text default '';

  declare v_sql text default '';
  declare v_tmp_sql text default '';
  declare v_source_sql text default '';
  declare v_comparison_sql text default '';
  declare v_match_sql text default '';
  declare v_trangid_sql text default '';
  declare v_index_sql text default '';

  declare v_rule_code text default '';

  declare v_reversal_flag char(1) default '';
  declare v_group_flag varchar(32) default '';
  declare v_group_method_flag char(1) default '';
  declare v_manytomany_match_flag char(1) default '';
  declare v_field_group_flag char(1) default '';
  declare v_txt text default '';
  declare v_result int default 0;

  declare v_source_dataset_code varchar(32) default '';
  declare v_comparison_dataset_code varchar(32) default '';

  declare v_source_field varchar(128) default '';
  declare v_source_field_format text default '';
  declare v_extraction_criteria varchar(255) default '';
  declare v_extraction_filter int default 0;
  declare v_comparison_field varchar(128) default '';
  declare v_comparison_criteria varchar(255) default '';
  declare v_group_field text default '';
  declare v_group_condition text default '';

  declare v_source_condition text default '';
  declare v_comparison_condition text default '';
  declare v_build_condition text default '';

  declare v_basefilter_condition text default '';
  declare v_sourcebase_filter text default '';
  declare v_comparisonbase_filter text default '';
  declare v_comparison_filter text default '';

  declare v_rule_condition text default '';
  declare v_rule_notnull_condition text default '';
  declare v_fieldfilter_format text default '';
  declare v_comparisonfilter_format text default '';
  declare v_rule_groupby text default '';

  declare v_field_format text default '';
  declare v_field text default '';

  declare v_tran_gid int default 0;
  declare v_tranbrkp_gid int default 0;
  declare v_excp_value double(15,2) default 0;
  declare v_ko_value double(15,2) default 0;
  declare v_match_gid int default 0;

  declare v_txt_tran_gid text default '';

  declare v_source_tran_gid text default '';
  declare v_comparison_tran_gid text default '';
  declare v_count int default 0;

  declare v_system_matchoff char(1) default null;
  declare v_manual_matchoff char(1) default null;

  declare v_filter_applied_on char(1) default '';
  declare v_filter_field varchar(128) default '';
  declare v_filter_criteria text default '';
  declare v_add_filter int default 0;
  declare v_ident_criteria text default '';
  declare v_ident_value text default '';

  declare v_open_parentheses_flag text default '';
  declare v_close_parentheses_flag text default '';
  declare v_join_condition text default '';

  declare v_tran_fields text default '';
  declare v_tranbrkp_fields text default '';

  declare v_grp_field text default '';
  declare v_grp_field_condition text default '';

  declare v_source_field_org_type text default '';
  declare v_comparison_field_org_type text default '';

  declare v_database_name text default '';
  declare v_table_name text default '';
  declare v_index_name text default '';
  declare v_sys_index_name text default '';

  declare v_recon_name text default '';
  declare v_recon_value_flag text default '';
  declare v_recon_automatch_partial text default '';
  declare v_rule_name text default '';
  declare v_field_type text default '';

  declare v_threshold_plus_value double(15,2) default 1;
  declare v_threshold_minus_value double(15,2) default 1;

  declare v_diff_value double(15,2) default 0;
  declare v_matched_count int default 0;

  declare v_src_acc_mode char(1) default null;
  declare v_cmp_acc_mode char(1) default null;

  declare v_preview_gid int default 0;

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  if in_automatch_flag = 'Y' then
    set v_system_matchoff = 'Y';
  else
    set v_manual_matchoff = 'Y';
  end if;

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and recon_value_flag = 'Y'
    and recon_automatch_partial = 'Y'
    and period_from <= curdate()
    and (until_active_flag = 'Y'
          or period_to >= curdate())
    and active_status = 'Y'
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Invalid recon !';
    leave me;
  end if;

  select database() into v_database_name;

  drop temporary table if exists recon_tmp_tmatch;
  drop temporary table if exists recon_tmp_tmatchdtl;
  drop temporary table if exists recon_tmp_tmatchdtlgid;
  drop temporary table if exists recon_tmp_tmatchdup;
  drop temporary table if exists recon_tmp_tmatchparentgid;
  drop temporary table if exists recon_tmp_tmatchko;
  drop temporary table if exists recon_tmp_tmanymatch;
  drop temporary table if exists recon_tmp_tkodtl;
  drop temporary table if exists recon_tmp_tkodtlsumm;
  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;
  drop temporary table if exists recon_tmp_tgid;

  drop temporary table if exists recon_tmp_tmatchdiff;
  drop temporary table if exists recon_tmp_tmatchdiffdtl;

  drop temporary table if exists recon_tmp_tindex;
  drop temporary table if exists recon_tmp_tsql;

  CREATE TEMPORARY TABLE recon_tmp_tindex(
    table_name varchar(128) not null,
    index_name varchar(128) not null,
    sys_flag char(1) not null default 'N',
    PRIMARY KEY (table_name,index_name),
    key idx_sys_flag(sys_flag)
  ) ENGINE = MyISAM;

  insert into recon_tmp_tindex select 'recon_tmp_tsource','idx_tran_date','Y';
  insert into recon_tmp_tindex select 'recon_tmp_tcomparison','idx_tran_date','Y';

  CREATE temporary TABLE recon_tmp_tmatchdiff(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    matched_count int not null default 0,
    matched_value double(15,2) not null default 0,
    diff_value double(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    matched_json json NOT NULL,
    tran_acc_mode char(1) default null,
    group_flag char(1) not null default 'N',
    dup_flag char(1) not null default 'N',
    PRIMARY KEY (tran_gid,tranbrkp_gid),
    key idx_group_flag(group_flag),
    key idx_dup_flag(dup_flag)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchdiffdtl(
    matchdiffdtl_gid int unsigned NOT NULL AUTO_INCREMENT,
    parent_tran_gid int unsigned NOT NULL,
    parent_tranbrkp_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    ko_value decimal(15,2) not null default 0,
    src_comp_flag char(1) default null,
    tran_acc_mode char(1) default null,
    dup_flag char(1) not null default 'N',
    PRIMARY KEY (matchdiffdtl_gid),
    key idx_parent_tran_gid(parent_tran_gid,parent_tranbrkp_gid),
    key idx_tran_gid(tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  -- drop table if exists recon_tmp_tmatch;

  CREATE temporary TABLE recon_tmp_tmatch(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    matched_count int not null default 0,
    matched_value double(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    matched_json json NOT NULL,
    group_flag char(1) not null default 'N',
    dup_flag char(1) not null default 'N',
    ko_flag char(1) not null default 'N',
    PRIMARY KEY (tran_gid,tranbrkp_gid),
    key idx_group_flag(group_flag),
    key idx_dup_flag(dup_flag),
    key idx_ko_flag(ko_flag)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchdtl(
    matchdtl_gid int unsigned NOT NULL AUTO_INCREMENT,
    parent_tran_gid int unsigned NOT NULL,
    parent_tranbrkp_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    ko_value decimal(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    src_comp_flag char(1) default null,
    dup_flag char(1) not null default 'N',
    ko_flag char(1) not null default 'N',
    PRIMARY KEY (matchdtl_gid),
    key idx_parent_tran_gid(parent_tran_gid,parent_tranbrkp_gid),
    key idx_tran_gid(tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tmanymatch(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    source_value double(15,2) not null default 0,
    comparison_value double(15,2) not null default 0,
    matched_count int not null default 0,
    tran_mult tinyint not null default 0,
    matched_txt_json text NOT NULL,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tkodtl(
    kodtl_gid int unsigned NOT NULL AUTO_INCREMENT,
    ko_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    tran_mult tinyint not null default 0,
    ko_value decimal(15,2) not null default 0,
    PRIMARY KEY (kodtl_gid),
    key idx_ko_gid(ko_gid),
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tkodtlsumm(
    kodtlsumm_gid int unsigned NOT NULL AUTO_INCREMENT,
    max_ko_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL,
    excp_value decimal(15,2) not null default 0,
    ko_value decimal(15,2) not null default 0,
    rec_count int not null default 0,
    PRIMARY KEY (kodtlsumm_gid),
    key idx_tran_gid(tran_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchdtlgid(
    matchdtl_gid int unsigned NOT NULL,
    PRIMARY KEY (matchdtl_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchdup(
    matchdup_gid int unsigned NOT NULL AUTO_INCREMENT,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    rec_count int unsigned not null default 0,
    PRIMARY KEY (matchdup_gid),
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchparentgid(
    parent_tran_gid int unsigned NOT NULL,
    parent_tranbrkp_gid int unsigned not null default 0,
    rec_count int unsigned not null default 0,
    PRIMARY KEY (parent_tran_gid,parent_tranbrkp_gid),
    key idx_parent_tran_gid(parent_tran_gid),
    key idx_parent_tranbrkp_gid(parent_tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchko(
    tran_gid int unsigned NOT NULL,
    ko_value decimal(15,2) not null default 0,
    excp_value decimal(15,2) not null default 0,
    transfer_flag char(1) not null default 'N',
    ko_flag char(1) not null default 'N',
    ko_gid int unsigned not null default 0,
    ko_date date default null,
    PRIMARY KEY (tran_gid),
    key idx_transfer_flag(transfer_flag),
    key idx_ko_flag(ko_flag)
  ) ENGINE = MyISAM;

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
    PRIMARY KEY (tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tgid(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned NOT NULL,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tsql(
    sql_gid int(10) unsigned NOT NULL AUTO_INCREMENT,
    table_type char(1) default null,
    tran_acc_mode char(1) default null,
    sql_query text default null,
    PRIMARY KEY (sql_gid)
  ) ENGINE = MyISAM;

  if in_rule_code = '' then set in_rule_code = null; end if;

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

  select
    recon_name,recontype_code,recon_value_flag,recon_automatch_partial
  into
    v_recon_name,v_recontype_code,v_recon_value_flag,v_recon_automatch_partial
  from recon_mst_trecon
  where recon_code = in_recon_code
  and period_from <= curdate()
  and (until_active_flag = 'Y'
  or period_to >= curdate())
  and delete_flag = 'N';

  set v_recontype_code = ifnull(v_recontype_code,'');
  set v_recon_name = ifnull(v_recon_name,'');
  -- set v_recon_value_flag = ifnull(v_recon_value_flag,'Y');
  set v_recon_automatch_partial = ifnull(v_recon_automatch_partial,'N');

  if v_recontype_code <> 'N' then
    set v_recon_value_flag = 'Y';
  else
    set v_recon_value_flag = 'N';
  end if;

  applyrule_block:begin
    declare applyrule_done int default 0;
    declare applyrule_cursor cursor for
      select
		    a.rule_code,a.rule_name,
        a.source_dataset_code,a.source_acc_mode,
        a.comparison_dataset_code,a.comparison_acc_mode,
        a.group_flag,a.reversal_flag,
        a.group_method_flag,a.manytomany_match_flag
      from recon_mst_trule as a
      where a.recon_code = in_recon_code
      and a.rule_code = ifnull(in_rule_code,a.rule_code)
      and a.period_from <= curdate()
      and (a.until_active_flag = 'Y'
      or a.period_to >= curdate())
      and a.rule_apply_on <> 'S'
      and a.active_status = 'Y'
      and a.system_match_flag = ifnull(v_system_matchoff,a.system_match_flag)
      and a.manual_match_flag = ifnull(v_manual_matchoff,a.manual_match_flag)
      and a.delete_flag = 'N'
      order by a.rule_order;
    declare continue handler for not found set applyrule_done=1;

    open applyrule_cursor;

    applyrule_loop: loop
      fetch applyrule_cursor into v_rule_code,v_rule_name,
                  v_source_dataset_code,v_source_acc_mode,
                  v_comparison_dataset_code,v_comparison_acc_mode,
                  v_group_flag,v_reversal_flag,
                  v_group_method_flag,v_manytomany_match_flag;

      if applyrule_done = 1 then leave applyrule_loop; end if;

      if in_automatch_flag = 'Y' then
        set v_txt = concat('Auto Matching Recon - ',v_recon_name,' Rule - ',v_rule_name);
      else
        set v_txt = concat('Preview Recon - ',v_recon_name,' Rule - ',v_rule_name);
      end if;

      call pr_upd_job(in_job_gid,'P',v_txt,@msg,@result);

      set v_rule_code = ifnull(v_rule_code,'');

      set v_reversal_flag = ifnull(v_reversal_flag,'N');
      set v_group_method_flag = ifnull(v_group_method_flag,'C');

      set v_group_flag = ifnull(v_group_flag,'N');

      if v_group_flag = 'OTO' then
        set v_group_flag = 'N';
        set v_manytomany_match_flag = 'N';
      elseif v_group_flag = 'OTM' then
        set v_group_flag = 'Y';
        set v_manytomany_match_flag = 'N';
      elseif v_group_flag = 'MTM' then
        set v_group_flag = 'Y';
        set v_manytomany_match_flag = 'Y';
      end if;

      if v_recontype_code = 'B'
        or v_recontype_code = 'W'
        or v_recontype_code = 'I' then
        if v_comparison_acc_mode = 'B' then
          set v_group_method_flag = 'B';

          if v_source_acc_mode = 'C' then
            set v_comparison_acc_mode = 'D';
          else
            set v_comparison_acc_mode = 'C';
          end if;
        elseif v_source_acc_mode = v_comparison_acc_mode then
          set v_group_method_flag = 'M';
        elseif v_source_acc_mode = v_comparison_acc_mode then
          set v_group_method_flag = 'C';
        end if;
      end if;

      set v_source_head_sql = concat('insert into recon_tmp_tsource (',v_tran_fields,') ');

      if in_automatch_flag = 'Y' then
        set v_source_head_sql = concat(v_source_head_sql,' select ',v_tran_fields ,' from recon_trn_ttran ');
      else
        set v_source_head_sql = concat(v_source_head_sql,' select ',v_tran_fields ,' from recon_tmp_ttran ');
      end if;

      set v_source_head_sql = concat(v_source_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_source_head_sql = concat(v_source_head_sql,' and excp_value > 0 and mapped_value = 0 ');
      else
        set v_source_head_sql = concat(v_source_head_sql,' and ko_gid = 0 ');
      end if;

      set v_comparison_head_sql = concat('insert into recon_tmp_tcomparison (',v_tran_fields,') ');

      if in_automatch_flag = 'Y' then
        set v_comparison_head_sql = concat(v_comparison_head_sql,' select ',v_tran_fields ,' from recon_trn_ttran ');
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' select ',v_tran_fields ,' from recon_tmp_ttran ');
      end if;

      set v_comparison_head_sql = concat(v_comparison_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and excp_value > 0 and mapped_value = 0 ');
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and ko_gid = 0 ');
      end if;

      set v_source_headbrkp_sql = concat('insert into recon_tmp_tsource (',v_tranbrkp_fields,') ');

      if in_automatch_flag = 'Y' then
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' select ',v_tranbrkp_fields ,' from recon_trn_ttranbrkp ');
      else
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' select ',v_tranbrkp_fields ,' from recon_tmp_ttranbrkp ');
      end if;

      set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and excp_value > 0 and tran_gid > 0 ');
      else
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and 1 = 2 ');
      end if;

      set v_comparison_headbrkp_sql = concat('insert into recon_tmp_tcomparison (',v_tranbrkp_fields,') ');

      if in_automatch_flag = 'Y' then
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' select ',v_tranbrkp_fields ,' from recon_trn_ttranbrkp ');
      else
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' select ',v_tranbrkp_fields ,' from recon_tmp_ttranbrkp ');
      end if;

      set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and excp_value > 0 and tran_gid > 0 ');
      else
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and 1 = 2 ');
      end if;

          basefilter_block:begin
            declare basefilter_done int default 0;
            declare basefilter_cursor cursor for
            select
              filter_applied_on,filter_field,filter_criteria,add_filter,ident_criteria,ident_value,
              open_parentheses_flag,close_parentheses_flag,join_condition
            from recon_mst_truleselefilter
            where rule_code = v_rule_code
            and active_status = 'Y'
            and delete_flag = 'N'
            order by filter_applied_on,ruleselefilter_seqno;

            declare continue handler for not found set basefilter_done=1;

            open basefilter_cursor;

            set v_sourcebase_filter = ' and ';
            set v_comparisonbase_filter = ' and ';

            basefilter_loop: loop
              fetch basefilter_cursor into v_filter_applied_on,v_filter_field,
                                    v_filter_criteria,v_add_filter,
                                    v_ident_criteria,v_ident_value,
                                    v_open_parentheses_flag,v_close_parentheses_flag,
                                    v_join_condition;
              if basefilter_done = 1 then leave basefilter_loop; end if;

              set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
              set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
              set v_join_condition = ifnull(v_join_condition,'');

              if v_join_condition = '' then
                set v_join_condition = 'and';
              end if;

              set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
              set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

              set v_basefilter_condition = concat(v_open_parentheses_flag,
                                                  fn_get_basefilterformat(v_filter_field,v_filter_criteria,v_add_filter,v_ident_criteria,v_ident_value),
                                                  v_close_parentheses_flag,' ',
                                                  v_join_condition,' ');

              if v_filter_applied_on = 'S' then
                set v_sourcebase_filter = concat(v_sourcebase_filter,v_basefilter_condition);
              elseif v_filter_applied_on = 'C' then
                set v_comparisonbase_filter = concat(v_comparisonbase_filter,v_basefilter_condition);
              end if;
            end loop basefilter_loop;

            close basefilter_cursor;
          end basefilter_block;

          -- if v_sourcebase_filter = ' and ' then set v_sourcebase_filter = ''; end if;
          -- if v_comparisonbase_filter = ' and ' then set v_comparisonbase_filter = ''; end if;

          set v_sourcebase_filter = concat(v_sourcebase_filter,' 1 = 1 ');
          set v_comparisonbase_filter = concat(v_comparisonbase_filter,' 1 = 1 ');

          set v_rule_condition = ' and ';
          set v_rule_notnull_condition = ' and ';
          set v_rule_groupby = '';

          set v_source_condition = ' and ';
          set v_comparison_condition = ' and ';

          drop temporary table if exists recon_tmp_tsource;
          drop temporary table if exists recon_tmp_tcomparison;
          drop temporary table if exists recon_tmp_tsourcedup;

          /*
          drop table if exists recon_tmp_tsource;
          drop table if exists recon_tmp_tcomparison;
          */
          create temporary table recon_tmp_tsource select * from recon_tmp_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tsource add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tsource(excp_value);
          create index idx_tran_date on recon_tmp_tsource(tran_date);
          create index idx_tran_gid on recon_tmp_tsource(tran_gid);
          create index idx_tranbrkp_gid on recon_tmp_tsource(tranbrkp_gid);
          create index idx_dataset_code on recon_tmp_tsource(recon_code,dataset_code);
          alter table recon_tmp_tsource ENGINE = MyISAM;

          create temporary table recon_tmp_tcomparison select * from recon_tmp_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tcomparison add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tcomparison(excp_value);
          create index idx_tran_date on recon_tmp_tcomparison(tran_date);
          create index idx_tran_gid on recon_tmp_tcomparison(tran_gid);
          create index idx_tranbrkp_gid on recon_tmp_tcomparison(tranbrkp_gid);
          create index idx_dataset_code on recon_tmp_tcomparison(recon_code,dataset_code);
          alter table recon_tmp_tcomparison ENGINE = MyISAM;

          create temporary table recon_tmp_tsourcedup select * from recon_tmp_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tsourcedup add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tsourcedup(excp_value);
          create index idx_tran_date on recon_tmp_tsourcedup(tran_date);
          create index idx_dataset_code on recon_tmp_tsourcedup(recon_code,dataset_code);
          alter table recon_tmp_tsourcedup ENGINE = MyISAM;

          delete from recon_tmp_tindex where index_name <> 'idx_tran_date';
          truncate recon_tmp_tsql;

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
            and a.delete_flag = 'N';

            declare continue handler for not found set rule_done=1;

            open rule_cursor;

            rule_loop: loop
              fetch rule_cursor into v_source_field,v_extraction_criteria,v_extraction_filter,
                                     v_comparison_field,v_comparison_criteria,v_comparison_filter,
                                     v_open_parentheses_flag,v_close_parentheses_flag,v_join_condition;
              if rule_done = 1 then leave rule_loop; end if;

              set v_index_name = concat('idx_',v_source_field);

              if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_tsource'
                            and index_name = v_index_name) then

                set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tsource(',v_source_field,')');
                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_tsource',v_index_name;
              end if;

              set v_index_name = concat('idx_',v_comparison_field);

              if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_tcomparison'
                            and index_name = v_index_name) then

                set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tcomparison(',v_comparison_field,')');

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_tcomparison',v_index_name;
              end if;

              set v_source_field_org_type = fn_get_fieldorgtype(in_recon_code,v_source_field);
              set v_comparison_field_org_type = fn_get_fieldorgtype(in_recon_code,v_comparison_field);

              set v_source_field = ifnull(v_source_field,'');
              set v_extraction_criteria = ifnull(v_extraction_criteria,'');
              set v_extraction_filter = ifnull(v_extraction_filter,0);

              set v_comparison_field = ifnull(v_comparison_field,'');
              set v_comparison_criteria = ifnull(v_comparison_criteria,'');
              set v_comparison_filter = ifnull(v_comparison_filter,0);

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
              if (instr(v_extraction_criteria,'$FIELD$') > 0 or v_extraction_filter > 0)
                and v_open_parentheses_flag <> '('
                and v_join_condition <> 'OR'
                and v_close_parentheses_flag <> ')' then

                set v_field = replace(v_source_field,'a.','');
                set v_field_format = fn_get_fieldfilterformat(v_field,v_extraction_criteria,v_extraction_filter);

                set v_sql = '';
                set v_sql = concat(v_sql,'update recon_tmp_tsource set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format);

                insert into recon_tmp_tsql(table_type,tran_acc_mode,sql_query) values ('S',v_source_acc_mode,v_sql);

                set v_extraction_criteria = 'EXACT';
                set v_extraction_filter = 0;
              end if;

              -- comparison
              if (instr(v_comparison_criteria,'$FIELD$') > 0 or v_comparison_filter > 0)
                and v_open_parentheses_flag <> '('
                and v_join_condition <> 'OR'
                and v_close_parentheses_flag <> ')' then

                set v_field = replace(v_comparison_field,'b.','');
                set v_field_format = fn_get_fieldfilterformat(v_field,v_comparison_criteria,v_comparison_filter);

                set v_sql = '';
                set v_sql = concat(v_sql,'update recon_tmp_tcomparison set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');
                set v_sql = concat(v_sql,'where tran_acc_mode =',char(39), v_comparison_acc_mode,char(39), ' ');

                insert into recon_tmp_tsql(table_type,tran_acc_mode,sql_query) values ('C',v_comparison_acc_mode,v_sql);

                set v_sql = '';
                set v_sql = concat(v_sql,'update recon_tmp_tcomparison set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');
                set v_sql = concat(v_sql,'where tran_acc_mode =',char(39), v_source_acc_mode,char(39),' ');

                insert into recon_tmp_tsql(table_type,tran_acc_mode,sql_query) values ('C',v_source_acc_mode,v_sql);

                set v_comparison_criteria = 'EXACT';
                set v_comparison_filter = 0;
              end if;

              set v_source_field_format = fn_get_fieldfilterformat(v_source_field,v_extraction_criteria,v_extraction_filter);
              set v_build_condition = concat(v_open_parentheses_flag,
                                             fn_get_comparisoncondition(in_recon_code,v_source_field_format,v_comparison_field,v_comparison_criteria,v_comparison_filter),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition);

              set v_rule_condition = concat(v_rule_condition,v_build_condition,' ');

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

              set v_rule_notnull_condition = concat(v_rule_notnull_condition,v_build_condition);

              set v_rule_groupby = concat(v_rule_groupby,',',v_source_field);
            end loop rule_loop;

            close rule_cursor;
          end rule_block;

          truncate recon_tmp_tsource;
          truncate recon_tmp_tcomparison;

          if v_source_condition = ' and ' or v_comparison_condition = ' and ' then
            set v_source_condition = ' and 1 = 2 ';
            set v_comparison_condition  = ' and 1 = 2 ';
            set v_rule_condition = ' and 1 = 2 ';
            set v_rule_notnull_condition = ' and 1 = 2 ';
            set v_rule_groupby = ',tran_gid';
          else
            set v_source_condition = concat(v_source_condition, ' 1 = 1 ');
            set v_comparison_condition  = concat(v_comparison_condition,' 1 = 1 ');
            set v_rule_condition  = concat(v_rule_condition,' 1 = 1 ');
            set v_rule_notnull_condition  = concat(v_rule_notnull_condition,' 1 = 1 ');
          end if;

          set v_rule_condition = concat(v_rule_condition,v_rule_notnull_condition);

          set v_source_sql = v_source_head_sql;
          set v_source_sql = concat(v_source_sql,' and dataset_code = ',char(39),v_source_dataset_code,char(39));
          set v_source_sql = concat(v_source_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
          set v_source_sql = concat(v_source_sql,' and tran_date >= ',char(39),in_period_from,char(39));
          set v_source_sql = concat(v_source_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          set v_source_sql = concat(v_source_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_source_sql = concat(v_source_sql,' ',v_source_condition);
          set v_source_sql = concat(v_source_sql,' ',v_sourcebase_filter);

          /*
          if in_automatch_flag = 'N' then
            set v_source_sql = concat(v_source_sql,' and tran_gid not in (select tran_gid from recon_trn_tpreviewdtl');
            set v_source_sql = concat(v_source_sql,' where job_gid = ',char(39),in_job_gid,char(39),' and tranbrkp_gid = 0) ');
          end if;
          */

          call pr_run_sql(v_source_sql,@result,@msg);

          -- select v_source_sql;
          -- leave me;

          set v_source_sql = v_source_headbrkp_sql;
          set v_source_sql = concat(v_source_sql,' and dataset_code = ',char(39),v_source_dataset_code,char(39));
          set v_source_sql = concat(v_source_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
          set v_source_sql = concat(v_source_sql,' and tran_date >= ',char(39),in_period_from,char(39));
          set v_source_sql = concat(v_source_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          set v_source_sql = concat(v_source_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_source_sql = concat(v_source_sql,' ',v_source_condition);
          set v_source_sql = concat(v_source_sql,' ',v_sourcebase_filter);

          /*
          if in_automatch_flag = 'N' then
            set v_source_sql = concat(v_source_sql,' and tranbrkp_gid not in (select tranbrkp_gid from recon_trn_tpreviewdtl');
            set v_source_sql = concat(v_source_sql,' where job_gid = ',char(39),in_job_gid,char(39),' and tranbrkp_gid > 0) ');
          end if;
          */

          call pr_run_sql(v_source_sql,@result,@msg);

          -- select v_source_sql;
          -- leave me;

          set v_comparison_sql = v_comparison_head_sql;
          set v_comparison_sql = concat(v_comparison_sql,' and dataset_code = ',char(39),v_comparison_dataset_code,char(39));
          set v_comparison_sql = concat(v_comparison_sql,' and tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39));
          set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
          set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          set v_comparison_sql = concat(v_comparison_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_comparison_sql = concat(v_comparison_sql,' ',v_comparison_condition);
          set v_comparison_sql = concat(v_comparison_sql,' ',v_comparisonbase_filter);

          /*
          if in_automatch_flag = 'N' then
            set v_comparison_sql = concat(v_comparison_sql,' and tran_gid not in (select tran_gid from recon_trn_tpreviewdtl');
            set v_comparison_sql = concat(v_comparison_sql,' where job_gid = ',char(39),in_job_gid,char(39),' and tranbrkp_gid = 0) ');
          end if;
          */

          call pr_run_sql(v_comparison_sql,@result,@msg);

          -- select v_comparison_sql;

          set v_comparison_sql = v_comparison_headbrkp_sql;
          set v_comparison_sql = concat(v_comparison_sql,' and dataset_code = ',char(39),v_comparison_dataset_code,char(39));
          set v_comparison_sql = concat(v_comparison_sql,' and tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39));
          set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
          set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          set v_comparison_sql = concat(v_comparison_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_comparison_sql = concat(v_comparison_sql,' ',v_comparison_condition);
          set v_comparison_sql = concat(v_comparison_sql,' ',v_comparisonbase_filter);

          /*
          if in_automatch_flag = 'N' then
            set v_comparison_sql = concat(v_comparison_sql,' and tranbrkp_gid not in (select tranbrkp_gid from recon_trn_tpreviewdtl');
            set v_comparison_sql = concat(v_comparison_sql,' where job_gid = ',char(39),in_job_gid,char(39),' and tranbrkp_gid > 0) ');
          end if;
          */

          call pr_run_sql(v_comparison_sql,@result,@msg);

          -- sql block
          sql_block:begin
            declare sql_done int default 0;
            declare sql_cursor cursor for
            select sql_query from recon_tmp_tsql
               where table_type = 'S'
               or (table_type = 'C' and tran_acc_mode = v_comparison_acc_mode);
            declare continue handler for not found set sql_done=1;

            open sql_cursor;

            sql_loop: loop
              fetch sql_cursor into v_sql;
              if sql_done = 1 then leave sql_loop; end if;

              call pr_run_sql(v_sql,@result,@msg);
            end loop sql_loop;
            close sql_cursor;
          end sql_block;

          -- group flag cases
          if v_group_flag = 'Y' then
            -- get target addtional group field
            select
              group_concat(concat('b.',a.grp_field)),
              group_concat(
              concat('b.',
                     a.grp_field,
                     case
                       when b.field_org_type = 'DATE' then ' is not null '
                       when b.field_org_type = 'NUMBER' then ' > 0 '
                     else concat(' <> ',char(39),char(39),' ')
                     end
               )
              )
            into
              v_grp_field,v_grp_field_condition
            from recon_mst_trulegrpfield as a
            inner join recon_mst_tfieldstru as b on b.field_name = a.grp_field
              and b.delete_flag = 'N'
            where a.rule_code = v_rule_code
            and a.active_status = 'Y'
            and a.delete_flag = 'N';

            set v_grp_field = ifnull(v_grp_field,'');

            set v_grp_field_condition = ifnull(v_grp_field_condition,'');
            set v_grp_field_condition = ifnull(v_grp_field_condition,'');

            set v_grp_field_condition = replace(v_grp_field_condition,',','and');

            -- comparison contra and mirror
            if v_group_method_flag = 'B' then
              set v_comparison_sql = v_comparison_head_sql;
              set v_comparison_sql = concat(v_comparison_sql,' and dataset_code = ',char(39),v_comparison_dataset_code,char(39));
              set v_comparison_sql = concat(v_comparison_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
              set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
              set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
              set v_comparison_sql = concat(v_comparison_sql,' and delete_flag = ',char(39),'N',char(39));
              set v_comparison_sql = concat(v_comparison_sql,' ',v_comparison_condition);
              set v_comparison_sql = concat(v_comparison_sql,' ',v_comparisonbase_filter);

              /*
              if in_automatch_flag = 'N' then
                set v_comparison_sql = concat(v_comparison_sql,' and tran_gid not in (select tran_gid from recon_trn_tpreviewdtl');
                set v_comparison_sql = concat(v_comparison_sql,' where job_gid = ',char(39),in_job_gid,char(39),') ');
              end if;
              */

              call pr_run_sql(v_comparison_sql,@result,@msg);

              set v_comparison_sql = v_comparison_headbrkp_sql;
              set v_comparison_sql = concat(v_comparison_sql,' and dataset_code = ',char(39),v_comparison_dataset_code,char(39));
              set v_comparison_sql = concat(v_comparison_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
              set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
              set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
              set v_comparison_sql = concat(v_comparison_sql,' and delete_flag = ',char(39),'N',char(39));
              set v_comparison_sql = concat(v_comparison_sql,' ',v_comparison_condition);
              set v_comparison_sql = concat(v_comparison_sql,' ',v_comparisonbase_filter);

              /*
              if in_automatch_flag = 'N' then
                set v_comparison_sql = concat(v_comparison_sql,' and tranbrkp_gid not in (select tranbrkp_gid from recon_trn_tpreviewdtl');
                set v_comparison_sql = concat(v_comparison_sql,' where job_gid = ',char(39),in_job_gid,char(39),') ');
              end if;
              */

              call pr_run_sql(v_comparison_sql,@result,@msg);

              -- sql block
              sql_block2:begin
                declare sql_done2 int default 0;
                declare sql_cursor2 cursor for
                select sql_query from recon_tmp_tsql
                   where table_type = 'C' and tran_acc_mode = v_source_acc_mode;
                declare continue handler for not found set sql_done2=1;

                open sql_cursor2;

                sql_loop2: loop
                  fetch sql_cursor2 into v_sql;
                  if sql_done2 = 1 then leave sql_loop2; end if;

                  call pr_run_sql(v_sql,@result,@msg);
                end loop sql_loop2;

                close sql_cursor2;
              end sql_block2;
            end if;
          end if;

          if in_automatch_flag = 'N' then
            -- remove already mapped lines
            delete from recon_tmp_tsource where (tran_gid,tranbrkp_gid) in
            (
              select tran_gid,tranbrkp_gid from recon_trn_tpreviewdtl
              where job_gid = in_job_gid
              and delete_flag = 'N'
            );

            delete from recon_tmp_tcomparison where (tran_gid,tranbrkp_gid) in
            (
              select tran_gid,tranbrkp_gid from recon_trn_tpreviewdtl
              where job_gid = in_job_gid
              and delete_flag = 'N'
            );
          end if;

          -- remove duplicate
          set v_trangid_sql = 'insert into recon_tmp_ttrangid ';
          set v_trangid_sql = concat(v_trangid_sql,'select cast(group_concat(tran_gid) as unsigned) from recon_tmp_tsource as a ') ;
          set v_trangid_sql = concat(v_trangid_sql,' where dataset_code = ',char(39),v_source_dataset_code,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' and tran_date >= ',char(39),in_period_from,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' and tranbrkp_gid = 0 ');
          set v_trangid_sql = concat(v_trangid_sql,' and delete_flag = ',char(39),'N',char(39));

          if v_recontype_code <> 'N' then
            set v_trangid_sql = concat(v_trangid_sql,' group by tran_value,',substr(v_rule_groupby,2));
          else
            set v_trangid_sql = concat(v_trangid_sql,' group by ',substr(v_rule_groupby,2));
          end if;

          set v_trangid_sql = concat(v_trangid_sql,' having count(*) = 1 ');

          call pr_run_sql(v_trangid_sql,@msg,@result);

          -- select v_trangid_sql;
          -- leave me;

          set v_trangid_sql = 'insert into recon_tmp_ttranbrkpgid ';
          set v_trangid_sql = concat(v_trangid_sql,'select cast(group_concat(tranbrkp_gid) as unsigned) from recon_tmp_tsource as a ') ;
          set v_trangid_sql = concat(v_trangid_sql,' where dataset_code = ',char(39),v_source_dataset_code,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' and tran_date >= ',char(39),in_period_from,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' and tranbrkp_gid > 0 ');
          set v_trangid_sql = concat(v_trangid_sql,' and delete_flag = ',char(39),'N',char(39));

          if v_recontype_code <> 'N' then
            set v_trangid_sql = concat(v_trangid_sql,' group by tran_value,',substr(v_rule_groupby,2));
          else
            set v_trangid_sql = concat(v_trangid_sql,' group by ',substr(v_rule_groupby,2));
          end if;

          set v_trangid_sql = concat(v_trangid_sql,' having count(*) = 1 ');

          call pr_run_sql(v_trangid_sql,@msg,@result);

          insert into recon_tmp_tsourcedup select * from recon_tmp_tsource where tran_gid not in (select tran_gid from recon_tmp_ttrangid) and tranbrkp_gid = 0;
          insert into recon_tmp_tsourcedup select * from recon_tmp_tsource where tranbrkp_gid not in (select tranbrkp_gid from recon_tmp_ttranbrkpgid) and tranbrkp_gid > 0;

          delete from recon_tmp_tsource where tran_gid not in (select tran_gid from recon_tmp_ttrangid) and tranbrkp_gid = 0;
          delete from recon_tmp_tsource where tranbrkp_gid not in (select tranbrkp_gid from recon_tmp_ttranbrkpgid) and tranbrkp_gid > 0;

          truncate recon_tmp_ttrangid;
          truncate recon_tmp_ttranbrkpgid;


          -- find matchoff diff value
          -- plus value
          set v_match_sql = 'insert into recon_tmp_tmatchdiff (group_flag,tran_gid,tranbrkp_gid,matched_count,diff_value,matched_json) ';
          set v_match_sql = concat(v_match_sql,'select ',char(39),'N',char(39),',');
          set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,(a.excp_value-b.excp_value) as diff_value,');
          set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[');
          set v_match_sql = concat(v_match_sql,'{');
          set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(a.tranbrkp_gid as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(a.tran_mult as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');
          set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',a.tran_acc_mode,',char(39),'",');
          set v_match_sql = concat(v_match_sql,'"ko_value":', char(39),',cast(a.excp_value as nchar),',char(39));
          set v_match_sql = concat(v_match_sql,'},');
          set v_match_sql = concat(v_match_sql,'{');
          set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(b.tran_gid as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(b.tran_mult as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"src_comp_flag":"C",');
          set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',b.tran_acc_mode,',char(39),'",');
          set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(b.excp_value as nchar),',char(39));
          set v_match_sql = concat(v_match_sql,'}');
          set v_match_sql = concat(v_match_sql,']',char(39),') as json) as matched_json ');
          set v_match_sql = concat(v_match_sql,'from recon_tmp_tsource as a ');
          set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tcomparison as b ');
          set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

          set v_match_sql = concat(v_match_sql,'and (a.excp_value - b.excp_value) > 0 ');
          set v_match_sql = concat(v_match_sql,'and (a.excp_value - b.excp_value) <=  ',cast(v_threshold_plus_value as nchar),' ');

          set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

          set v_match_sql = concat(v_match_sql,'and a.tran_acc_mode = ',char(39),v_source_acc_mode,char(39),' ');
          set v_match_sql = concat(v_match_sql,'and b.tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39),' ');

          set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid ');
          -- set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid,a.excp_value,b.excp_value ');
          set v_match_sql = concat(v_match_sql,'having count(*) = 1 ');

          call pr_run_sql(v_match_sql,@msg,@result);

          -- minus value
          set v_match_sql = 'insert ignore into recon_tmp_tmatchdiff (group_flag,tran_gid,tranbrkp_gid,matched_count,diff_value,matched_json) ';
          set v_match_sql = concat(v_match_sql,'select ',char(39),'N',char(39),',');
          set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,(a.excp_value-b.excp_value) as diff_value,');
          set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[');
          set v_match_sql = concat(v_match_sql,'{');
          set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(a.tranbrkp_gid as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(a.tran_mult as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');
          set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',a.tran_acc_mode,',char(39),'",');
          set v_match_sql = concat(v_match_sql,'"ko_value":', char(39),',cast(a.excp_value as nchar),',char(39));
          set v_match_sql = concat(v_match_sql,'},');
          set v_match_sql = concat(v_match_sql,'{');
          set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(b.tran_gid as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(b.tran_mult as nchar),',char(39),',');
          set v_match_sql = concat(v_match_sql,'"src_comp_flag":"C",');
          set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',b.tran_acc_mode,',char(39),'",');
          set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(b.excp_value as nchar),',char(39));
          set v_match_sql = concat(v_match_sql,'}');
          set v_match_sql = concat(v_match_sql,']',char(39),') as json) as matched_json ');
          set v_match_sql = concat(v_match_sql,'from recon_tmp_tsource as a ');
          set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tcomparison as b ');
          set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

          set v_match_sql = concat(v_match_sql,'and (b.excp_value - a.excp_value) > 0 ');
          set v_match_sql = concat(v_match_sql,'and (b.excp_value - a.excp_value) <=  ',cast(v_threshold_minus_value as nchar),' ');

          set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

          set v_match_sql = concat(v_match_sql,'and a.tran_acc_mode = ',char(39),v_source_acc_mode,char(39),' ');
          set v_match_sql = concat(v_match_sql,'and b.tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39),' ');

          set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid ');
          -- set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid,a.excp_value,b.excp_value ');
          set v_match_sql = concat(v_match_sql,'having count(*) = 1 ');

          call pr_run_sql(v_match_sql,@msg,@result);

          truncate recon_tmp_tpseudorows;
          insert into recon_tmp_tpseudorows select 0 union select 1;

          insert into recon_tmp_tmatchdiffdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,src_comp_flag,tran_acc_mode)
            select
              tran_gid as parent_tran_gid,
              tranbrkp_gid as parent_tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].ko_value'))) AS ko_value,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].src_comp_flag'))) AS src_comp_flag,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_acc_mode'))) AS tran_acc_mode
            FROM recon_tmp_tmatchdiff
            JOIN recon_tmp_tpseudorows
            where group_flag = 'N'
            HAVING tran_gid IS NOT NULL;

          if v_group_flag = 'Y' then
            -- plus value
            set v_match_sql = 'insert ignore into recon_tmp_tmatchdiff (group_flag,tran_gid,tranbrkp_gid,matched_count,diff_value,tran_mult,matched_json) ';
            set v_match_sql = concat(v_match_sql,'select ',char(39),'Y',char(39),',');
            set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,(abs(a.excp_value*a.tran_mult) - abs(sum(b.excp_value*b.tran_mult))) as diff_value,a.tran_mult,');

            set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[');
            set v_match_sql = concat(v_match_sql,'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(a.tranbrkp_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(a.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');
            set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',a.tran_acc_mode,',char(39),'",');
            set v_match_sql = concat(v_match_sql,'"ko_value":', char(39),',cast(a.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'},',char(39),',');
            set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(b.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(b.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"src_comp_flag":"C",');
            set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',b.tran_acc_mode,',char(39),'",');
            set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(b.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'}',char(39),'),');
            set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json) as matched_json ');

            set v_match_sql = concat(v_match_sql,'from recon_tmp_tsource as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tcomparison as b ');
            set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

            set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

            if v_grp_field_condition <> '' then
              set v_match_sql = concat(v_match_sql,'and ',v_grp_field_condition);
            end if;

            set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_rule_groupby,' ');

            if v_grp_field <> '' then
              set v_match_sql = concat(v_match_sql,',',v_grp_field,' ');
            end if;

            set v_match_sql = concat(v_match_sql,'having count(*) > 1 ');

            set v_match_sql = concat(v_match_sql,'and (abs(a.excp_value*a.tran_mult) - abs(sum(b.excp_value*b.tran_mult))) > 0 ');
            set v_match_sql = concat(v_match_sql,'and (abs(a.excp_value*a.tran_mult) - abs(sum(b.excp_value*b.tran_mult))) <= ',cast(v_threshold_plus_value as nchar),' ');

            call pr_run_sql(v_match_sql,@msg,@result);

            insert ignore into recon_tmp_tgid select tran_gid,tranbrkp_gid from recon_tmp_tmatchdiff;

            -- minus value
            set v_match_sql = 'insert ignore into recon_tmp_tmatchdiff (group_flag,tran_gid,tranbrkp_gid,matched_count,diff_value,tran_mult,matched_json) ';
            set v_match_sql = concat(v_match_sql,'select ',char(39),'Y',char(39),',');
            set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,(abs(a.excp_value*a.tran_mult) - abs(sum(b.excp_value*b.tran_mult))) as diff_value,a.tran_mult,');

            set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[');
            set v_match_sql = concat(v_match_sql,'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(a.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(a.tranbrkp_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(a.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');
            set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',a.tran_acc_mode,',char(39),'",');
            set v_match_sql = concat(v_match_sql,'"ko_value":', char(39),',cast(a.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'},',char(39),',');
            set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
            set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(b.tran_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(b.tran_mult as nchar),',char(39),',');
            set v_match_sql = concat(v_match_sql,'"src_comp_flag":"C",');
            set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',b.tran_acc_mode,',char(39),'",');
            set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(b.excp_value as nchar),',char(39));
            set v_match_sql = concat(v_match_sql,'}',char(39),'),');
            set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json) as matched_json ');

            set v_match_sql = concat(v_match_sql,'from recon_tmp_tsource as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tcomparison as b ');
            set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

            set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

            if v_grp_field_condition <> '' then
              set v_match_sql = concat(v_match_sql,'and ',v_grp_field_condition);
            end if;

            set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_rule_groupby,' ');

            if v_grp_field <> '' then
              set v_match_sql = concat(v_match_sql,',',v_grp_field,' ');
            end if;

            set v_match_sql = concat(v_match_sql,'having count(*) > 1 ');

            set v_match_sql = concat(v_match_sql,'and (abs(sum(b.excp_value*b.tran_mult)) - abs(a.excp_value*a.tran_mult)) > 0 ');
            set v_match_sql = concat(v_match_sql,'and (abs(sum(b.excp_value*b.tran_mult)) - abs(a.excp_value*a.tran_mult)) <= ',cast(v_threshold_minus_value as nchar),' ');

            call pr_run_sql(v_match_sql,@msg,@result);

            select max(matched_count) into v_count from recon_tmp_tmatchdiff;
            set v_count = ifnull(v_count,0);

            truncate recon_tmp_tpseudorows;

            if v_count >= 2 then
              insert into recon_tmp_tpseudorows select row from pseudo_rows1 where row <= v_count;
            else
              insert into recon_tmp_tpseudorows select 0 union select 1;
            end if;

            insert into recon_tmp_tmatchdiffdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,src_comp_flag,tran_acc_mode)
              select
                tran_gid as parent_tran_gid,
                tranbrkp_gid as parent_tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].ko_value'))) AS ko_value,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].src_comp_flag'))) AS src_comp_flag,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatchdiff.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_acc_mode'))) AS tran_acc_mode
              FROM recon_tmp_tmatchdiff
              JOIN recon_tmp_tpseudorows
              where group_flag = 'Y'
              HAVING tran_gid IS NOT NULL;
          end if;

          truncate recon_tmp_tmatchdup;
          truncate recon_tmp_tmatchparentgid;

          -- duplicate validation
          insert into recon_tmp_tmatchdup (tran_gid,tranbrkp_gid,rec_count)
            select tran_gid,tranbrkp_gid,count(*) from recon_tmp_tmatchdiffdtl
            group by tran_gid,tranbrkp_gid
            having count(*) > 1;

          insert into recon_tmp_tmatchparentgid(parent_tran_gid,parent_tranbrkp_gid)
            select b.parent_tran_gid,b.parent_tranbrkp_gid from recon_tmp_tmatchdup as a
            inner join recon_tmp_tmatchdiffdtl as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
            group by b.parent_tran_gid,b.parent_tranbrkp_gid;

          update recon_tmp_tmatchdiff as a
          inner join recon_tmp_tmatchparentgid as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
          set a.dup_flag = 'Y';

          update recon_tmp_tmatchdiff as a
          inner join recon_tmp_tmatchdiffdtl as b on a.tran_gid = b.tran_gid
            and a.tranbrkp_gid = b.tranbrkp_gid
            and a.dup_flag = 'N'
          set a.tran_acc_mode = b.tran_acc_mode;

          -- source block
          source_block:begin
            declare source_done int default 0;
            declare source_cursor cursor for
            select tran_gid,tranbrkp_gid,tran_acc_mode,diff_value,matched_count from recon_tmp_tmatchdiff
               where dup_flag = 'N';
            declare continue handler for not found set source_done=1;

            open source_cursor;

            source_loop: loop
              fetch source_cursor into v_tran_gid,v_tranbrkp_gid,v_src_acc_mode,v_diff_value,v_matched_count;
              if source_done = 1 then leave source_loop; end if;

              if v_src_acc_mode = 'C' then
                set v_cmp_acc_mode = 'D';
              else
                set v_cmp_acc_mode = 'C';
              end if;

              if v_diff_value > 0 then
                update recon_tmp_tsource
                  set excp_value = excp_value - v_diff_value
                  where tran_gid = v_tran_gid
                  and tranbrkp_gid = v_tranbrkp_gid;
              else
                set v_diff_value = abs(v_diff_value);

                comparison_block:begin
                  declare comparison_done int default 0;
                  declare comparison_cursor cursor for
                  select tran_gid,tranbrkp_gid,ko_value from recon_tmp_tmatchdiffdtl
                    where parent_tran_gid = v_tran_gid
                    and parent_tranbrkp_gid = v_tranbrkp_gid
                    and src_comp_flag = 'C'
                    and tran_acc_mode = v_cmp_acc_mode
                    order by ko_value;
                  declare continue handler for not found set comparison_done=1;

                  open comparison_cursor;

                  comparison_loop: loop
                    fetch comparison_cursor into v_tran_gid,v_tranbrkp_gid,v_ko_value;
                    if comparison_done = 1 then leave comparison_loop; end if;

                    if v_ko_value >= v_diff_value then
                      update recon_tmp_tcomparison
                       set excp_value = excp_value - v_diff_value
                        where tran_gid = v_tran_gid
                        and tranbrkp_gid = v_tranbrkp_gid;
                      leave comparison_loop;
                    else
                      update recon_tmp_tcomparison
                       set excp_value = 0
                        where tran_gid = v_tran_gid
                        and tranbrkp_gid = v_tranbrkp_gid;

                      set v_diff_value = v_diff_value - v_ko_value;
                    end if;
                  end loop comparison_loop;

                  close comparison_cursor;
                end comparison_block;
              end if;
            end loop source_loop;
            close source_cursor;
          end source_block;

          truncate recon_tmp_tmatchdup;
          truncate recon_tmp_tmatchparentgid;

          truncate recon_tmp_tmatchdiff;
          truncate recon_tmp_tmatchdiffdtl;

          truncate recon_tmp_tpseudorows;

          -- vijay start

          -- get target addtional group field
          if v_group_flag = 'Y' then
            select group_concat(concat('b.',grp_field)) into v_grp_field from recon_mst_trulegrpfield
            where rule_code = v_rule_code
            and active_status = 'Y'
            and delete_flag = 'N';

            set v_grp_field = ifnull(v_grp_field,'');

            if v_grp_field <> '' then
              if v_rule_groupby <> '' then
                set v_rule_groupby = concat(v_rule_groupby,',',v_grp_field);
              else
                set v_rule_groupby = v_grp_field;
              end if;
            end if;
					end if;

					-- many to many match
					if v_manytomany_match_flag = 'Y' then
						-- and v_recontype_code <> 'N' and v_recontype_code <> 'V'
						set v_match_sql = 'insert into recon_tmp_tmanymatch (tran_gid,tranbrkp_gid,matched_count,';
						set v_match_sql = concat(v_match_sql,'tran_mult,source_value,comparison_value,matched_txt_json) ');
						set v_match_sql = concat(v_match_sql,'select ');
						set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,a.tran_mult,');
						set v_match_sql = concat(v_match_sql,'a.excp_value as source_value,sum(b.excp_value*b.tran_mult)*-1 as comparison_value,');

						set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
						set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(b.tran_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(b.tran_mult as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"src_comp_flag":"C",');
						set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(b.excp_value as nchar),',char(39));
						set v_match_sql = concat(v_match_sql,'}',char(39),' order by b.tran_gid,b.tranbrkp_gid) as matched_json ');

						set v_match_sql = concat(v_match_sql,'from recon_tmp_tsource as a ');
						set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tcomparison as b ');
						set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

						set v_match_sql = concat(v_match_sql,'where 1 = 1 ');
						set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

						set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_rule_groupby,' ');
						-- set v_match_sql = concat(v_match_sql,'having true  '); -- count(*) > 1
						-- set v_match_sql = concat(v_match_sql,'and a.excp_value*a.tran_mult <> sum(b.excp_value*b.tran_mult)*-1 ');

						call pr_run_sql(v_match_sql,@msg,@result);

						-- insert in match table
						set v_match_sql = 'insert into recon_tmp_tmatch (group_flag,tran_gid,tranbrkp_gid,matched_count,matched_value,tran_mult,matched_json) ';
						set v_match_sql = concat(v_match_sql,'select ',char(39),'M',char(39),',');
						set v_match_sql = concat(v_match_sql,'max(tran_gid),max(tranbrkp_gid),sum(matched_count)+count(*) as matched_count,');
						set v_match_sql = concat(v_match_sql,'comparison_value as matched_value,tran_mult,');
						set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[',char(39),',');
						set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
						set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(tran_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(tranbrkp_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(tran_mult as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');
						set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(source_value as nchar),',char(39));
						set v_match_sql = concat(v_match_sql,'}',char(39),'),');
						set v_match_sql = concat(v_match_sql,char(39),',',char(39),',');
						set v_match_sql = concat(v_match_sql,'matched_txt_json,');
						set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json) as matched_json ');

						set v_match_sql = concat(v_match_sql,'from recon_tmp_tmanymatch ');

						if v_recontype_code <> 'N' then
							set v_match_sql = concat(v_match_sql,'group by matched_txt_json,comparison_value,tran_mult ');
							set v_match_sql = concat(v_match_sql,'having sum(source_value*tran_mult) = comparison_value');
						else
							set v_match_sql = concat(v_match_sql,'group by matched_txt_json ');
						end if;

						-- set v_match_sql = concat(v_match_sql,'having count(*) > 1 ');
						-- set v_match_sql = concat(v_match_sql,'and sum(source_value*tran_mult) = comparison_value');

						call pr_run_sql(v_match_sql,@msg,@result);

						select max(matched_count) into v_count from recon_tmp_tmatch;
						set v_count = ifnull(v_count,0);

						truncate recon_tmp_tpseudorows;

						if v_count >= 2 then
							insert into recon_tmp_tpseudorows select row from pseudo_rows1 where row <= v_count;
						else
							insert into recon_tmp_tpseudorows select 0 union select 1;
						end if;

						insert into recon_tmp_tmatchdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult,src_comp_flag)
							select
								tran_gid as parent_tran_gid,
								tranbrkp_gid as parent_tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].ko_value'))) AS ko_value,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_mult'))) AS tran_mult,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].src_comp_flag'))) AS src_comp_flag
							FROM recon_tmp_tmatch
							JOIN recon_tmp_tpseudorows
							where group_flag = 'M'
							HAVING tran_gid IS NOT NULL;

						-- clear matched records
						truncate recon_tmp_ttrangid;

						insert into recon_tmp_ttrangid
							select distinct tran_gid from recon_tmp_tmatchdtl where tran_gid > 0 and tranbrkp_gid = 0;

						delete from recon_tmp_tsource where tran_gid in (select tran_gid from recon_tmp_ttrangid);
						delete from recon_tmp_tcomparison where tran_gid in (select tran_gid from recon_tmp_ttrangid);

						truncate recon_tmp_ttranbrkpgid;

						insert into recon_tmp_ttranbrkpgid
							select distinct tranbrkp_gid from recon_tmp_tmatchdtl where tranbrkp_gid > 0;

						delete from recon_tmp_tsource where tranbrkp_gid in (select tranbrkp_gid from recon_tmp_ttranbrkpgid);
						delete from recon_tmp_tcomparison where tranbrkp_gid in (select tranbrkp_gid from recon_tmp_ttranbrkpgid);

						truncate recon_tmp_ttrangid;
						truncate recon_tmp_ttranbrkpgid;
					end if;

					-- one to many match
					 if v_group_flag = 'Y' then
            set v_match_sql = 'insert into recon_tmp_tmatch (group_flag,tran_gid,tranbrkp_gid,matched_count,matched_value,tran_mult,matched_json) ';
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

            set v_match_sql = concat(v_match_sql,'from recon_tmp_tsource as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tcomparison as b ');
            set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

						set v_match_sql = concat(v_match_sql,'where 1 = 1 ');
            set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

            if v_recontype_code <> 'N' then
              set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_rule_groupby,' ');
            else
              set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid',v_rule_groupby,' ');
            end if;

            set v_match_sql = concat(v_match_sql,'having count(*) > 1 ');

            if v_recontype_code <> 'N' then
              set v_match_sql = concat(v_match_sql,'and a.excp_value*a.tran_mult = sum(b.excp_value*b.tran_mult)*-1 ');
            end if;

            call pr_run_sql(v_match_sql,@msg,@result);

            select max(matched_count) into v_count from recon_tmp_tmatch;
            set v_count = ifnull(v_count,0);

            truncate recon_tmp_tpseudorows;

            if v_count >= 2 then
              insert into recon_tmp_tpseudorows select row from pseudo_rows1 where row <= v_count;
            else
              insert into recon_tmp_tpseudorows select 0 union select 1;
            end if;

            insert into recon_tmp_tmatchdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult,src_comp_flag)
              select
                tran_gid as parent_tran_gid,
                tranbrkp_gid as parent_tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].ko_value'))) AS ko_value,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_mult'))) AS tran_mult,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].src_comp_flag'))) AS src_comp_flag
              FROM recon_tmp_tmatch
              JOIN recon_tmp_tpseudorows
              where group_flag = 'Y'
              HAVING tran_gid IS NOT NULL;

						-- clear matched records
						truncate recon_tmp_ttrangid;

						insert into recon_tmp_ttrangid
							select distinct tran_gid from recon_tmp_tmatchdtl where tran_gid > 0 and tranbrkp_gid = 0;

						delete from recon_tmp_tsource where tran_gid in (select tran_gid from recon_tmp_ttrangid);
						delete from recon_tmp_tcomparison where tran_gid in (select tran_gid from recon_tmp_ttrangid);

						truncate recon_tmp_ttranbrkpgid;

						insert into recon_tmp_ttranbrkpgid
							select distinct tranbrkp_gid from recon_tmp_tmatchdtl where tranbrkp_gid > 0;

						delete from recon_tmp_tsource where tranbrkp_gid in (select tranbrkp_gid from recon_tmp_ttranbrkpgid);
						delete from recon_tmp_tcomparison where tranbrkp_gid in (select tranbrkp_gid from recon_tmp_ttranbrkpgid);

						truncate recon_tmp_ttrangid;
						truncate recon_tmp_ttranbrkpgid;
					 end if;

					-- one to one match
          set v_match_sql = 'insert into recon_tmp_tmatch (group_flag,tran_gid,tranbrkp_gid,matched_count,matched_value,matched_json) ';
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
          set v_match_sql = concat(v_match_sql,'from recon_tmp_tsource as a ');
          set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tcomparison as b ');
          set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

          if v_recontype_code <> 'N' then
            set v_match_sql = concat(v_match_sql,'and a.excp_value = b.excp_value ');
          end if;

          set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

          set v_match_sql = concat(v_match_sql,'where a.tran_acc_mode = ',char(39),v_source_acc_mode,char(39),' ');
          set v_match_sql = concat(v_match_sql,'and b.tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39),' ');
          set v_match_sql = concat(v_match_sql,'and 1 = 1 ');

          set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid ');
          set v_match_sql = concat(v_match_sql,'having count(*) = 1 ');

          call pr_run_sql(v_match_sql,@msg,@result);

          -- pseudorows
          select max(matched_count) into v_count from recon_tmp_tmatch;
          set v_count = ifnull(v_count,0);

          truncate recon_tmp_tpseudorows;

          if v_count >= 2 then
            insert into recon_tmp_tpseudorows select row from pseudo_rows1 where row <= v_count;
          else
            insert into recon_tmp_tpseudorows select 0 union select 1;
          end if;

          -- select v_source_sql,v_comparison_sql,v_match_sql;
          -- leave me;

          insert into recon_tmp_tmatchdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult,src_comp_flag)
            select
              tran_gid as parent_tran_gid,
              tranbrkp_gid as parent_tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].ko_value'))) AS ko_value,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_mult'))) AS tran_mult,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_tmatch.matched_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].src_comp_flag'))) AS src_comp_flag
            FROM recon_tmp_tmatch
            JOIN recon_tmp_tpseudorows
            where group_flag = 'N'
            HAVING tran_gid IS NOT NULL;

          -- duplicate validation
          insert into recon_tmp_tmatchdup (tran_gid,tranbrkp_gid,rec_count)
            select tran_gid,tranbrkp_gid,count(*) from recon_tmp_tmatchdtl
            group by tran_gid,tranbrkp_gid
            having count(*) > 1;

          insert into recon_tmp_tmatchparentgid(parent_tran_gid,parent_tranbrkp_gid)
            select b.parent_tran_gid,b.parent_tranbrkp_gid from recon_tmp_tmatchdup as a
            inner join recon_tmp_tmatchdtl as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
            group by b.parent_tran_gid,b.parent_tranbrkp_gid;

          update recon_tmp_tmatch as a
          inner join recon_tmp_tmatchparentgid as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
          set a.dup_flag = 'Y';

          update recon_tmp_tmatch set
            matched_value = abs(matched_value),
            ko_flag = 'Y'
          where dup_flag = 'N';

          if in_automatch_flag = 'Y' then
            truncate recon_tmp_tmatchko;

            if v_recontype_code <> 'N' then
              insert into recon_tmp_tmatchko (tran_gid,ko_value,excp_value)
              select
                a.tran_gid,abs(sum(a.ko_value*a.tran_mult)),b.excp_value
              from recon_tmp_tmatch as m
              inner join recon_tmp_tmatchdtl as a on m.tran_gid = a.parent_tran_gid and m.tranbrkp_gid = a.parent_tranbrkp_gid
              inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid and b.excp_value > 0 and b.delete_flag = 'N'
              where m.dup_flag = 'N'
              and m.ko_flag = 'Y'
              group by a.tran_gid
              having b.excp_value >= abs(sum(a.ko_value*a.tran_mult));

              update recon_tmp_tmatchdtl as a
              inner join recon_tmp_tmatchko as b on a.tran_gid = b.tran_gid
              set a.ko_flag = 'Y';

              update recon_tmp_tmatch as a
              inner join recon_tmp_tmatchdtl as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
                and b.ko_flag = 'N'
              set a.ko_flag = 'N'
              where a.dup_flag = 'N';
            end if;

            insert into recon_trn_tko
            (
              job_gid,ko_date,ko_value,recon_code,rule_code,
              reversal_flag,manual_matchoff,kodtl_json,kodtl_post_flag,insert_date,insert_by
            )
            select
              in_job_gid,curdate(),matched_value,in_recon_code,v_rule_code,
              v_reversal_flag,'N',matched_json,'N',sysdate(),in_user_code
            from recon_tmp_tmatch
            where ko_flag = 'Y';

            insert into recon_tmp_tkodtl
            ( ko_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult)
            select
              ko_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tko.kodtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tko.kodtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tko.kodtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].ko_value'))) AS excp_value,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tko.kodtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_mult'))) AS tran_mult
            FROM recon_trn_tko
            JOIN recon_tmp_tpseudorows
            where job_gid = in_job_gid
            and kodtl_post_flag = 'N'
            HAVING tran_gid IS NOT NULL
            order by ko_gid;

            insert into recon_trn_tkodtl (ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult)
              select ko_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult from recon_tmp_tkodtl;

            insert into recon_tmp_tkodtlsumm (max_ko_gid,tran_gid,ko_value,rec_count)
              select max(ko_gid) as max_ko_gid,tran_gid,sum(ko_value*tran_mult) as ko_value,count(*) as rec_count from recon_tmp_tkodtl
              group by tran_gid;

            if v_recontype_code <> 'N' then
              update recon_trn_ttran as a
              inner join recon_tmp_tkodtlsumm as b on a.tran_gid = b.tran_gid
              set a.excp_value = a.excp_value - (b.ko_value * a.tran_mult),
                  a.ko_gid = b.max_ko_gid,
                  a.ko_date = curdate()
              where a.excp_value > 0
              and a.delete_flag = 'N';

              update recon_trn_ttranbrkp as a
              inner join recon_tmp_tkodtl as b on a.tranbrkp_gid = b.tranbrkp_gid
              set a.excp_value = a.excp_value - b.ko_value,
                  a.ko_gid = b.ko_gid,
                  a.ko_date = curdate()
              where a.excp_value > 0
              and a.delete_flag = 'N';
            else
              update recon_trn_ttran as a
              inner join recon_tmp_tkodtlsumm as b on a.tran_gid = b.tran_gid
              set a.ko_gid = b.max_ko_gid,
                  a.ko_date = curdate()
              where a.ko_gid = 0
              and a.delete_flag = 'N';

              update recon_trn_ttranbrkp as a
              inner join recon_tmp_tkodtl as b on a.tranbrkp_gid = b.tranbrkp_gid
              set a.ko_gid = b.ko_gid,
                  a.ko_date = curdate()
              where a.ko_gid = 0
              and a.delete_flag = 'N';
            end if;

            truncate recon_tmp_ttrangid;

            insert into recon_tmp_ttrangid
              select a.tran_gid from recon_tmp_tkodtlsumm as a
              inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid and b.excp_value = 0 and b.delete_flag = 'N';

            insert into recon_trn_ttranko
              select t.* from recon_tmp_ttrangid as g
              inner join recon_trn_ttran as t on g.tran_gid = t.tran_gid;

            delete from recon_trn_ttran where tran_gid in (select tran_gid from recon_tmp_ttrangid);

            truncate recon_tmp_ttranbrkpgid;

            insert into recon_tmp_ttranbrkpgid select tranbrkp_gid from recon_tmp_tkodtl where tranbrkp_gid > 0;

            insert into recon_trn_ttranbrkpko
              select b.* from recon_tmp_ttranbrkpgid as g
              inner join recon_trn_ttranbrkp as b on g.tranbrkp_gid = b.tranbrkp_gid;

            delete from recon_trn_ttranbrkp where tranbrkp_gid in (select tranbrkp_gid from recon_tmp_ttranbrkpgid);

            update recon_trn_tko set
              kodtl_post_flag = 'Y'
            where job_gid = in_job_gid
            and kodtl_post_flag = 'N'
            and delete_flag = 'N';
          else
            select max(preview_gid) into v_preview_gid from recon_trn_tpreview
            where job_gid = in_job_gid
            and delete_flag = 'N';

            set v_preview_gid = ifnull(v_preview_gid,0);
            set @preview_gid = v_preview_gid;

            insert into recon_trn_tpreview
            (
              preview_gid,job_gid,preview_date,preview_value,recon_code,rule_code,
              reversal_flag,previewdtl_json,previewdtl_post_flag,insert_date,insert_by
            )
            select
              @preview_gid:=@preview_gid+1,in_job_gid,sysdate(),matched_value,in_recon_code,
              v_rule_code,v_reversal_flag,matched_json,'N',sysdate(),in_user_code
            from recon_tmp_tmatch
            where dup_flag = 'N'
            and tranbrkp_gid = 0;

            insert into recon_trn_tpreviewdtl
            ( previewdtl_gid,preview_gid,job_gid,tran_gid,tranbrkp_gid,excp_value,tran_mult,reversal_flag,src_comp_flag)
            select
              recon_tmp_tpseudorows.row+1,
              preview_gid,
              job_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].ko_value'))) AS excp_value,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_mult'))) AS tran_mult,
              v_reversal_flag,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].src_comp_flag'))) AS src_comp_flag
            FROM recon_trn_tpreview
            JOIN recon_tmp_tpseudorows
            where job_gid = in_job_gid
            and previewdtl_post_flag = 'N'
            HAVING tran_gid IS NOT NULL;

            -- update in preview
            update recon_trn_tpreview
            set previewdtl_post_flag = 'Y'
            where job_gid = in_job_gid
            and previewdtl_post_flag = 'N'
            and delete_flag = 'N';

            if v_recontype_code <> 'N' then
							insert into recon_trn_tpreview
							(
								preview_gid,job_gid,preview_date,preview_value,recon_code,rule_code,
								reversal_flag,previewdtl_json,previewdtl_post_flag,insert_date,insert_by
							)
              select
                @preview_gid:=@preview_gid+1,in_job_gid,curdate(),matched_value,in_recon_code,v_rule_code,
                v_reversal_flag,matched_json,'N',sysdate(),in_user_code
              from recon_tmp_tmatch
              where 1 = 1
              and dup_flag = 'N'
              and tranbrkp_gid > 0;

							insert into recon_trn_tpreviewdtl
							( previewdtl_gid,preview_gid,job_gid,tran_gid,tranbrkp_gid,excp_value,tran_mult,reversal_flag,src_comp_flag)
							select
								recon_tmp_tpseudorows.row+1,
								preview_gid,
								job_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].ko_value'))) AS excp_value,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_mult'))) AS tran_mult,
								v_reversal_flag,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].src_comp_flag'))) AS src_comp_flag
							FROM recon_trn_tpreview
							JOIN recon_tmp_tpseudorows
							where job_gid = in_job_gid
							and previewdtl_post_flag = 'N'
							HAVING tran_gid IS NOT NULL;
            end if;

            update recon_trn_tpreview
            set previewdtl_post_flag = 'Y'
            where job_gid = in_job_gid
            and previewdtl_post_flag = 'N'
            and delete_flag = 'N';

            if v_recontype_code <> 'N' then
              update recon_tmp_ttran as a
              inner join recon_trn_tpreviewdtl as b on a.tran_gid = b.tran_gid
                and b.job_gid = in_job_gid
                and b.preview_gid > v_preview_gid
              set a.excp_value = a.excp_value - b.excp_value
              where a.excp_value > 0
              and a.delete_flag = 'N';

              update recon_tmp_ttranbrkp as a
							inner join recon_tmp_tmatchdtl as c on a.tranbrkp_gid = c.tranbrkp_gid and a.tran_gid = c.tran_gid
              inner join recon_trn_tpreviewdtl as b on c.tran_gid = b.tran_gid
                and b.job_gid = in_job_gid
                and b.preview_gid > v_preview_gid
              set a.excp_value = a.excp_value - c.ko_value
              where a.excp_value > 0
              and a.delete_flag = 'N';
            else
              update recon_tmp_ttran as a
              inner join recon_trn_tpreviewdtl as b on a.tran_gid = b.tran_gid
                and b.job_gid = in_job_gid
                and b.preview_gid > v_preview_gid
              set a.ko_gid = b.preview_gid
              where a.ko_gid = 0
              and a.delete_flag = 'N';

              update recon_tmp_ttranbrkp as a
              inner join recon_trn_tpreviewdtl as b on a.tranbrkp_gid = b.tranbrkp_gid
                and b.job_gid = in_job_gid
                and b.preview_gid > v_preview_gid
              set a.ko_gid = b.preview_gid
              where a.ko_gid = 0
              and a.delete_flag = 'N';
            end if;
          end if;

          -- vijay end

          leave me;

          truncate recon_tmp_tsource;
          truncate recon_tmp_tcomparison;
          truncate recon_tmp_tsourcedup;
          truncate recon_tmp_tmatch;
          truncate recon_tmp_tmatchdtl;
          truncate recon_tmp_tmatchdtlgid;
          truncate recon_tmp_tmatchdup;
          truncate recon_tmp_tmatchparentgid;
          truncate recon_tmp_tmatchko;
          truncate recon_tmp_tmanymatch;
          truncate recon_tmp_tkodtl;
          truncate recon_tmp_tkodtlsumm;
          truncate recon_tmp_ttrangid;
          truncate recon_tmp_ttranbrkpgid;
    end loop applyrule_loop;

    close applyrule_cursor;
  end applyrule_block;

  set out_result = v_count;

  if in_automatch_flag = 'Y' then
    set out_msg = 'Auto match ran successfully !';
  else
    set out_msg = 'Preview ran successfully !';
  end if;

  drop temporary table if exists recon_tmp_tsource;
  drop temporary table if exists recon_tmp_tcomparison;
  drop temporary table if exists recon_tmp_tsourcedup;
  drop temporary table if exists recon_tmp_tmatch;
  drop temporary table if exists recon_tmp_tmatchdtl;
  drop temporary table if exists recon_tmp_tmatchdtlgid;
  drop temporary table if exists recon_tmp_tmatchdup;
  drop temporary table if exists recon_tmp_tmatchparentgid;
  drop temporary table if exists recon_tmp_tmatchko;
  drop temporary table if exists recon_tmp_tmanymatch;
  drop temporary table if exists recon_tmp_tkodtl;
  drop temporary table if exists recon_tmp_tkodtlsumm;
  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;
  drop temporary table if exists recon_tmp_tindex;
  drop temporary table if exists recon_tmp_tsql;
  drop temporary table if exists recon_tmp_tgid;

  drop temporary table if exists recon_tmp_tmatchdiff;
  drop temporary table if exists recon_tmp_tmatchdiffdtl;
end $$

DELIMITER ;