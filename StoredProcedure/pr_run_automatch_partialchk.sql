DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_automatch_partialchk` $$
CREATE PROCEDURE `pr_run_automatch_partialchk`
(
  in in_recon_code text,
  in in_rule_code text,
  in in_group_flag text,
  in in_job_gid int,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_acc_mode varchar(32) default '';
  declare v_source_acc_mode varchar(32) default '';
  declare v_comparison_acc_mode varchar(32) default '';
  declare v_base_acc_mode varchar(32) default '';
  declare v_recontype_code varchar(32) default '';

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
  declare v_group_desc text default '';
  declare v_group_method_flag char(1) default '';
  declare v_manytomany_match_flag char(1) default '';
  declare v_field_group_flag char(1) default '';
  declare v_txt text default '';
  declare v_result int default 0;

  declare v_source_dataset_code varchar(32) default '';
  declare v_comparison_dataset_code varchar(32) default '';

  declare v_source_dataset_type varchar(32) default '';
  declare v_comparison_dataset_type varchar(32) default '';

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
  declare v_tran_mult tinyint default 0;
  declare v_excp_value double(15,2) default 0;
  declare v_ko_value double(15,2) default 0;
  declare v_match_gid int default 0;

  declare v_txt_tran_gid text default '';

  declare v_source_tran_gid text default '';
  declare v_comparison_tran_gid text default '';
  declare v_count int default 0;

  declare v_parent_tran_gid int default 0;
  declare v_parent_tranbrkp_gid int default 0;

  declare v_system_matchoff char(1) default null;
  declare v_manual_matchoff char(1) default null;

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
  declare v_grp_field_condition text default '';

  declare v_source_field_org_type text default '';
  declare v_comparison_field_org_type text default '';

  declare v_database_name text default '';
  declare v_table_name text default '';
  declare v_index_name text default '';
  declare v_sys_index_name text default '';

  declare v_recon_name text default '';
  declare v_recon_value_flag text default '';
  declare v_recon_date_flag text default '';
  declare v_recon_automatch_partial text default '';
  declare v_rule_name text default '';
  declare v_field_type text default '';

  declare v_threshold_code text default '';
  declare v_threshold_flag text default '';
  declare v_threshold_plus_value double(15,2) default 0;
  declare v_threshold_minus_value double(15,2) default 0;

  declare v_rule_automatch_partial text default '';
  declare v_rule_threshold_plus_value double(15,2) default 0;
  declare v_rule_threshold_minus_value double(15,2) default 0;

  declare v_matched_value double(15,2) default 0;
  declare v_matched_count int default 0;

  declare v_diff_value double(15,2) default 0;
  declare v_mapped_value double(15,2) default 0;

  declare v_src_acc_mode char(1) default null;
  declare v_cmp_acc_mode char(1) default null;
  declare v_tran_acc_mode char(1) default null;

  declare v_src_comp_flag char(1) default null;

  declare v_recorder_source text default '';
  declare v_recorder_comparison text default '';
  declare v_recorder text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';
	declare v_koroundoff_table text default '';

  declare v_preview_gid int default 0;

  declare v_concurrent_ko_flag text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

	-- set tran table
  /*
	set v_tran_table = concat(in_recon_code,'_tran');
	set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');

	set v_tranko_table = concat(in_recon_code,'_tranko');
	set v_tranbrkpko_table = concat(in_recon_code,'_tranbrkpko');

	set v_ko_table = concat(in_recon_code,'_ko');
	set v_kodtl_table = concat(in_recon_code,'_kodtl');
	set v_koroundoff_table = concat(in_recon_code,'_koroundoff');
  */

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(in_recon_code,'_tran');
	  set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');

	  set v_tranko_table = concat(in_recon_code,'_tranko');
	  set v_tranbrkpko_table = concat(in_recon_code,'_tranbrkpko');

	  set v_ko_table = concat(in_recon_code,'_ko');
	  set v_kodtl_table = concat(in_recon_code,'_kodtl');
	  set v_koroundoff_table = concat(in_recon_code,'_koroundoff');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

	  set v_tranko_table = 'recon_trn_ttranko';
	  set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';

	  set v_ko_table = 'recon_trn_tko';
	  set v_kodtl_table = 'recon_trn_tkodtl';
	  set v_koroundoff_table = 'recon_trn_tkoroundoff';
  end if;

  set v_group_flag = in_group_flag;

  if v_group_flag = 'MTM' then
    set v_group_desc = ' (partial - many to many)';
  elseif v_group_flag = 'OTM' then
    set v_group_desc = ' (partial - one to many)';
  elseif v_group_flag = 'OTO' then
    set v_group_desc = ' (partial - one to one)';
  end if;

  if in_automatch_flag = 'Y' then
    set v_system_matchoff = 'Y';
  else
    set v_manual_matchoff = 'Y';
  end if;

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
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

  drop temporary table if exists recon_tmp_t4match;
  drop temporary table if exists recon_tmp_t4matchdtl;
  drop temporary table if exists recon_tmp_t4matchdtlgid;
  drop temporary table if exists recon_tmp_t4matchdup;
  drop temporary table if exists recon_tmp_t4matchparentgid;
  drop temporary table if exists recon_tmp_t4matchko;
  drop temporary table if exists recon_tmp_t4matchdiff;
  drop temporary table if exists recon_tmp_t4matchdiffdtl;
  drop temporary table if exists recon_tmp_t4manymatch;
  drop temporary table if exists recon_tmp_t4kodtl;
  drop temporary table if exists recon_tmp_t4kodtlsumm;
  drop temporary table if exists recon_tmp_t4pseudorows;
  drop temporary table if exists recon_tmp_t4trangid;
  drop temporary table if exists recon_tmp_t4trangid1;
  drop temporary table if exists recon_tmp_t4trangid2;
  drop temporary table if exists recon_tmp_t4tranbrkpgid;
  drop temporary table if exists recon_tmp_t4tranwithbrkpgid;
  drop temporary table if exists recon_tmp_t4gid;
  drop temporary table if exists recon_tmp_t4tranroundoff;

  drop temporary table if exists recon_tmp_t4thresholddiff;
  drop temporary table if exists recon_tmp_t4thresholddiffdtl;

  drop temporary table if exists recon_tmp_t4index;
  drop temporary table if exists recon_tmp_t4sql;

  CREATE TEMPORARY TABLE recon_tmp_t4index(
    table_name varchar(128) not null,
    index_name varchar(128) not null,
    sys_flag char(1) not null default 'N',
    PRIMARY KEY (table_name,index_name),
    key idx_sys_flag(sys_flag)
  ) ENGINE = MyISAM;

  insert into recon_tmp_t4index select 'recon_tmp_t4source','idx_tran_date','Y';
  insert into recon_tmp_t4index select 'recon_tmp_t4comparison','idx_tran_date','Y';

  drop table if exists recon_tmp_t4thresholddiff;
  drop table if exists recon_tmp_t4thresholddiffdtl;

  CREATE /*temporary*/ TABLE recon_tmp_t4thresholddiff(
    matchdiff_gid int unsigned NOT NULL AUTO_INCREMENT,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    matched_count int not null default 0,
    matched_value double(15,2) not null default 0,
    diff_value double(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    matched_json json NOT NULL,
    tran_acc_mode char(1) default null,
    group_flag char(1) not null default 'N',
    many_to_many_flag char(1) not null default 'N',
    dup_flag char(1) not null default 'N',
    PRIMARY KEY (matchdiff_gid),
    unique key idx_tran_gid(tran_gid,tranbrkp_gid),
    key idx_group_flag(group_flag),
    key idx_dup_flag(dup_flag)
  ) ENGINE = MyISAM;

  create /*temporary*/ table recon_tmp_t4thresholddiffdtl(
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

  drop table if exists recon_tmp_t4match;

  CREATE /*temporary*/ TABLE recon_tmp_t4match(
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
  );

  create temporary table recon_tmp_t4matchdtl(
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
  );

  drop table if exists recon_tmp_t4manymatch;

  CREATE /*temporary*/ TABLE recon_tmp_t4manymatch(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    source_value double(15,2) not null default 0,
    comparison_value double(15,2) not null default 0,
    matched_count int not null default 0,
    tran_mult tinyint not null default 0,
    tran_acc_mode char(1),
    matched_txt_json json NOT NULL,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_t4kodtl(
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

  create temporary table recon_tmp_t4kodtlsumm(
    kodtlsumm_gid int unsigned NOT NULL AUTO_INCREMENT,
    max_ko_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL,
    excp_value decimal(15,2) not null default 0,
    ko_value decimal(15,2) not null default 0,
    roundoff_value decimal(15,2) not null default 0,
    rec_count int not null default 0,
    PRIMARY KEY (kodtlsumm_gid),
    key idx_tran_gid(tran_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_t4matchdtlgid(
    matchdtl_gid int unsigned NOT NULL,
    PRIMARY KEY (matchdtl_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_t4matchdup(
    matchdup_gid int unsigned NOT NULL AUTO_INCREMENT,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    rec_count int unsigned not null default 0,
    PRIMARY KEY (matchdup_gid),
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_t4matchparentgid(
    parent_tran_gid int unsigned NOT NULL,
    parent_tranbrkp_gid int unsigned not null default 0,
    rec_count int unsigned not null default 0,
    PRIMARY KEY (parent_tran_gid,parent_tranbrkp_gid),
    key idx_parent_tran_gid(parent_tran_gid),
    key idx_parent_tranbrkp_gid(parent_tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_t4matchko(
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

  create temporary table recon_tmp_t4matchdiff(
    tran_gid int unsigned NOT NULL,
    tran_mult tinyint not null default 0,
    tran_value decimal(15,2) not null default 0,
    excp_value decimal(15,2) not null default 0,
    mapped_value decimal(15,2) not null default 0,
    diff_value decimal(15,2) not null default 0,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_t4matchdiffdtl(
    matchdiffdtl_gid int unsigned NOT NULL,
    parent_tran_gid int unsigned NOT NULL default 0,
    parent_tranbrkp_gid int unsigned NOT NULL default 0,
    tran_gid int unsigned NOT NULL default 0,
    tranbrkp_gid int unsigned not null default 0,
    ko_value decimal(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    src_comp_flag char(1) default null,
    dup_flag char(1) not null default 'N',
    ko_flag char(1) not null default 'N',
    PRIMARY KEY (matchdiffdtl_gid),
    key idx_parent_tran_gid(parent_tran_gid),
    key idx_parent_gid(parent_tran_gid,parent_tranbrkp_gid),
    key idx_tran_gid(tran_gid),
    key idx_gid(tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t4pseudorows(
    row int unsigned NOT NULL,
    PRIMARY KEY (row)
  ) ENGINE = MyISAM;

  insert into recon_tmp_t4pseudorows select 0 union select 1;

  CREATE temporary TABLE recon_tmp_t4trangid(
    tran_gid int unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t4trangid1(
    tran_gid int unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t4trangid2(
    tran_gid int unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t4tranbrkpgid(
    tranbrkp_gid int unsigned NOT NULL,
    excp_value double(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    tran_gid int not null default 0,
    PRIMARY KEY (tranbrkp_gid),
    key idx_tran_gid(tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t4tranwithbrkpgid(
    tran_gid int unsigned not null,
    tranbrkp_gid int unsigned NOT NULL,
    rec_count int not null default 0,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t4gid(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned NOT NULL default 0,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t4tranroundoff(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned NOT NULL,
    roundoff_value double(15,2) NOT NULL default 0,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_t4sql(
    sql_gid int(10) unsigned NOT NULL AUTO_INCREMENT,
    table_type char(1) default null,
    tran_acc_mode char(1) default null,
    sql_query text default null,
    PRIMARY KEY (sql_gid)
  ) ENGINE = MyISAM;

  if in_rule_code = '' then set in_rule_code = null; end if;

  -- get record order
  -- source & comparison
  set v_recorder_source = fn_get_rulerecorder(in_rule_code,'S','a.');
  set v_recorder_comparison = fn_get_rulerecorder(in_rule_code,'C','b.');
  set v_recorder = '';

  if v_recorder_source <> '' then
    set v_recorder = v_recorder_source;
  end if;

  if v_recorder_comparison <> '' then
    if v_recorder = '' then
      set v_recorder = v_recorder_comparison;
    else
      set v_recorder = concat(v_recorder,',',v_recorder_comparison);
    end if;
  end if;

  if v_recorder <> '' then
    set v_recorder = concat('order by ',v_recorder);
  end if;

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

  -- get recon value
  select
    recon_name,recontype_code,recon_date_flag,
    recon_value_flag,recon_automatch_partial,threshold_code,
    abs(threshold_plus_value),abs(threshold_minus_value)
  into
    v_recon_name,v_recontype_code,v_recon_date_flag,
    v_recon_value_flag,v_recon_automatch_partial,
    v_threshold_code,
    v_threshold_plus_value,
    v_threshold_minus_value
  from recon_mst_trecon
  where recon_code = in_recon_code
  and period_from <= curdate()
  and (until_active_flag = 'Y'
  or period_to >= curdate())
  and delete_flag = 'N';

  set v_recontype_code = ifnull(v_recontype_code,'');
  set v_recon_name = ifnull(v_recon_name,'');
  set v_threshold_code = ifnull(v_threshold_code,'');
  -- set v_recon_value_flag = ifnull(v_recon_value_flag,'Y');
  set v_recon_automatch_partial = ifnull(v_recon_automatch_partial,'N');

  if v_recon_automatch_partial = 'Y' then
    set v_threshold_plus_value = ifnull(v_threshold_plus_value,0);
    set v_threshold_minus_value = ifnull(v_threshold_minus_value,0);

    if v_threshold_code = 'P' then
      set v_threshold_minus_value = 0;
    elseif v_threshold_code = 'M' then
      set v_threshold_plus_value = 0;
    end if;
  else
    if exists(select rule_code from recon_mst_trule
      where recon_code = in_recon_code
      and rule_code = ifnull(in_rule_code,rule_code)
      and period_from <= curdate()
      and (until_active_flag = 'Y'
      or period_to >= curdate())
      and rule_apply_on <> 'S'
      and active_status = 'Y'
      and rule_automatch_partial = 'Y'
      and delete_flag = 'N') then

      set v_threshold_plus_value = 0;
      set v_threshold_minus_value = 0;
    else
      leave me;
    end if;
  end if;

  if v_recontype_code <> 'N' then
    set v_recon_value_flag = 'Y';
  else
    set v_recon_value_flag = 'N';
  end if;

 /*
  if v_recon_automatch_partial = 'N' then
    leave me;
  end if;
  */

  applyrule_block:begin
    declare applyrule_done int default 0;
    declare applyrule_cursor cursor for
      select
		    a.rule_code,a.rule_name,
        a.source_dataset_code,a.source_acc_mode,
        a.comparison_dataset_code,a.comparison_acc_mode,
        a.reversal_flag,
        a.group_method_flag,
        a.manytomany_match_flag,
        a.rule_automatch_partial,
        a.threshold_flag,
        a.threshold_code,
        abs(a.threshold_plus_value) as threshold_plus_value,
        abs(a.threshold_minus_value) as threshold_minus_value
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
      -- and a.rule_automatch_partial = if(v_recon_automatch_partial='N','Y',a.rule_automatch_partial)
      and a.delete_flag = 'N'
      order by a.rule_order;
    declare continue handler for not found set applyrule_done=1;

    open applyrule_cursor;

    applyrule_loop: loop
      fetch applyrule_cursor into v_rule_code,v_rule_name,
                  v_source_dataset_code,v_source_acc_mode,
                  v_comparison_dataset_code,v_comparison_acc_mode,
                  v_reversal_flag,
                  v_group_method_flag,
                  v_manytomany_match_flag,
                  v_rule_automatch_partial,
                  v_threshold_flag,
                  v_threshold_code,
                  v_rule_threshold_plus_value,
                  v_rule_threshold_minus_value;

      if applyrule_done = 1 then leave applyrule_loop; end if;

      -- check threshold at rule level
      set v_rule_automatch_partial = ifnull(v_rule_automatch_partial,'N');
      set v_threshold_flag = ifnull(v_threshold_flag,'N');
      set v_threshold_code = ifnull(v_threshold_code,'');

      if v_rule_automatch_partial = 'Y' then
        if v_threshold_flag = 'Y' then
          set v_rule_threshold_plus_value = ifnull(v_rule_threshold_plus_value,0);
          set v_rule_threshold_minus_value = ifnull(v_rule_threshold_minus_value,0);

          if v_threshold_code = 'P' then
            set v_rule_threshold_minus_value = 0;
          elseif v_threshold_code = 'M' then
            set v_rule_threshold_plus_value = 0;
          end if;
        else
          set v_rule_threshold_plus_value = 0;
          set v_rule_threshold_minus_value = 0;
        end if;
      else
        set v_rule_threshold_plus_value = v_threshold_plus_value;
        set v_rule_threshold_minus_value = v_threshold_minus_value;
      end if;

      if v_rule_threshold_plus_value = 0 and v_rule_threshold_minus_value = 0 then
        iterate applyrule_loop;
      end if;

      -- update job
      set v_txt = concat('Applying Rule - ',v_rule_name,v_group_desc);
      call pr_upd_job(in_job_gid,'P',v_txt,@msg,@result);

      set v_rule_code = ifnull(v_rule_code,'');

      set v_reversal_flag = ifnull(v_reversal_flag,'N');

      -- mirror reveral
      if v_recontype_code = 'I' and v_source_dataset_code = v_comparison_dataset_code then
        set v_reversal_flag = 'Y';
      end if;

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
        -- elseif v_source_acc_mode = v_comparison_acc_mode then
        --  set v_group_method_flag = 'M';
        -- elseif v_source_acc_mode = v_comparison_acc_mode then
        --  set v_group_method_flag = 'C';
        end if;
      elseif v_recontype_code = 'V' then
        set v_source_acc_mode = 'V';
        set v_comparison_acc_mode = 'V';
      end if;

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

      -- source head for tran table
      set v_source_head_sql = concat('insert into recon_tmp_t4source (',v_tran_fields,') ');

      if in_automatch_flag = 'Y' then
        set v_source_head_sql = concat(v_source_head_sql,' select ',v_tran_fields ,' from ',v_tran_table,' ');
      else
        set v_source_head_sql = concat(v_source_head_sql,' select ',v_tran_fields ,' from recon_tmp_ttran ');
      end if;

      set v_source_head_sql = concat(v_source_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_source_dataset_type <> 'S' then
        set v_source_head_sql = concat(v_source_head_sql,' and dataset_code = ',char(39),v_source_dataset_code,char(39));
      else
        set v_source_head_sql = concat(v_source_head_sql,' and tranbrkp_dataset_code = ',char(39),v_source_dataset_code,char(39));
      end if;

      if v_recontype_code <> 'N' then
        set v_source_head_sql = concat(v_source_head_sql,' and excp_value <> 0 and mapped_value = 0 and roundoff_value = 0 ');
        set v_source_head_sql = concat(v_source_head_sql,' and (excp_value - roundoff_value * tran_mult) <> 0 ');
      else
        set v_source_head_sql = concat(v_source_head_sql,' and ko_gid = 0 ');
      end if;

      set v_source_head_sql = concat(v_source_head_sql,' and auto_match_flag = ''Y'' ');

      -- comparison head for tran table
      set v_comparison_head_sql = concat('insert into recon_tmp_t4comparison (',v_tran_fields,') ');

      if in_automatch_flag = 'Y' then
        set v_comparison_head_sql = concat(v_comparison_head_sql,' select ',v_tran_fields ,' from ',v_tran_table,' ');
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' select ',v_tran_fields ,' from recon_tmp_ttran ');
      end if;

      set v_comparison_head_sql = concat(v_comparison_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_comparison_dataset_type <> 'S' then
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and dataset_code = ',char(39),v_comparison_dataset_code,char(39));
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and tranbrkp_dataset_code = ',char(39),v_comparison_dataset_code,char(39));
      end if;

      if v_recontype_code <> 'N' then
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and excp_value <> 0 and mapped_value = 0 and roundoff_value = 0 ');
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and (excp_value - roundoff_value * tran_mult) <> 0 ');
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and ko_gid = 0 ');
      end if;

      set v_comparison_head_sql = concat(v_comparison_head_sql,' and auto_match_flag = ''Y'' ');

      -- source head for tranbrkp table
      set v_source_headbrkp_sql = concat('insert into recon_tmp_t4source (',v_tranbrkp_fields,') ');

      if in_automatch_flag = 'Y' then
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' select ',v_tranbrkp_fields ,' from ',v_tranbrkp_table,' ');
      else
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' select ',v_tranbrkp_fields ,' from recon_tmp_ttranbrkp ');
      end if;

      set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_source_dataset_type <> 'S' then
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and dataset_code = ',char(39),v_source_dataset_code,char(39));
      else
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and tranbrkp_dataset_code = ',char(39),v_source_dataset_code,char(39));
      end if;

      if v_recontype_code <> 'N' then
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and excp_value <> 0 and tran_gid > 0 ');
      else
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and 1 = 2 ');
      end if;

      set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and auto_match_flag = ''Y'' ');

      -- comparison head for tranbrkp table
      set v_comparison_headbrkp_sql = concat('insert into recon_tmp_t4comparison (',v_tranbrkp_fields,') ');

      if in_automatch_flag = 'Y' then
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' select ',v_tranbrkp_fields ,' from ',v_tranbrkp_table,' ');
      else
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' select ',v_tranbrkp_fields ,' from recon_tmp_ttranbrkp ');
      end if;

      set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_comparison_dataset_type <> 'S' then
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and dataset_code = ',char(39),v_comparison_dataset_code,char(39));
      else
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and tranbrkp_dataset_code = ',char(39),v_comparison_dataset_code,char(39));
      end if;

      if v_recontype_code <> 'N' then
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and excp_value <> 0 and tran_gid > 0 ');
      else
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and 1 = 2 ');
      end if;

      set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and auto_match_flag = ''Y'' ');

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
                                    v_filter_criteria,v_add_filter,
                                    v_ident_criteria,v_ident_value_flag,v_ident_value,
                                    v_open_parentheses_flag,v_close_parentheses_flag,
                                    v_join_condition;
              if basefilter_done = 1 then leave basefilter_loop; end if;

              set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
              set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
              set v_join_condition = ifnull(v_join_condition,'');

              if v_join_condition = '' then
                set v_join_condition = 'and';
              end if;

              set v_ident_value_flag = ifnull(v_ident_value_flag,'Y');
              set v_ident_value = ifnull(v_ident_value,'');

              set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
              set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

              set v_basefilter_condition = concat(v_open_parentheses_flag,
                                                  fn_get_basefilterformat(v_filter_field,v_filter_criteria,v_add_filter,v_ident_criteria,v_ident_value_flag,v_ident_value),
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

          set v_sourcebase_filter = concat(v_sourcebase_filter,' 1 = 1) ');
          set v_comparisonbase_filter = concat(v_comparisonbase_filter,' 1 = 1) ');

          set v_rule_condition = ' and a.excp_value <> 0 and b.excp_value <> 0 and ';
          set v_rule_notnull_condition = ' and ';
          set v_rule_groupby = '';

          set v_source_condition = ' and ';
          set v_comparison_condition = ' and ';

          drop temporary table if exists recon_tmp_t4source;
          drop temporary table if exists recon_tmp_t4comparison;
          drop temporary table if exists recon_tmp_t4sourcedup;

          drop table if exists recon_tmp_t4source;
          drop table if exists recon_tmp_t4comparison;

          -- create source
          create /*temporary*/ table recon_tmp_t4source select * from recon_trn_ttranwithbrkp where 1 = 2;

          alter table recon_tmp_t4source add primary key(tran_gid,tranbrkp_gid);
          alter table recon_tmp_t4source add match_flag char(1) not null default 'N';

          create index idx_excp_value on recon_tmp_t4source(excp_value);
          create index idx_tran_date on recon_tmp_t4source(tran_date);
          create index idx_tran_gid on recon_tmp_t4source(tran_gid);
          create index idx_tranbrkp_gid on recon_tmp_t4source(tranbrkp_gid);
          create index idx_dataset_code on recon_tmp_t4source(recon_code,dataset_code);
          create index idx_match_flag on recon_tmp_t4source(match_flag);

          alter table recon_tmp_t4source ENGINE = MyISAM;

          -- create comparison
          create /*temporary*/ table recon_tmp_t4comparison select * from recon_trn_ttranwithbrkp where 1 = 2;

          alter table recon_tmp_t4comparison add primary key(tran_gid,tranbrkp_gid);
          alter table recon_tmp_t4comparison add match_flag char(1) not null default 'N';

          create index idx_excp_value on recon_tmp_t4comparison(excp_value);
          create index idx_tran_date on recon_tmp_t4comparison(tran_date);
          create index idx_tran_gid on recon_tmp_t4comparison(tran_gid);
          create index idx_tranbrkp_gid on recon_tmp_t4comparison(tranbrkp_gid);
          create index idx_dataset_code on recon_tmp_t4comparison(recon_code,dataset_code);
          create index idx_match_flag on recon_tmp_t4comparison(match_flag);

          alter table recon_tmp_t4comparison ENGINE = MyISAM;

          create temporary table recon_tmp_t4sourcedup select * from recon_trn_ttranwithbrkp where 1 = 2;

          alter table recon_tmp_t4sourcedup add primary key(tran_gid,tranbrkp_gid);
          alter table recon_tmp_t4sourcedup add match_flag char(1) not null default 'N';

          create index idx_excp_value on recon_tmp_t4sourcedup(excp_value);
          create index idx_tran_date on recon_tmp_t4sourcedup(tran_date);
          create index idx_dataset_code on recon_tmp_t4sourcedup(recon_code,dataset_code);
          alter table recon_tmp_t4sourcedup ENGINE = MyISAM;

          delete from recon_tmp_t4index where index_name <> 'idx_tran_date';
          truncate recon_tmp_t4sql;

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

              if not exists(select index_name from recon_tmp_t4index
                            WHERE table_name = 'recon_tmp_t4source'
                            and index_name = v_index_name) then

                if substr(v_source_field,1,3) = 'col' then
                  set v_sql = concat('alter table recon_tmp_t4source modify column ',v_source_field,' varchar(255) default null');
                  call pr_run_sql(v_sql,@msg,@result);

                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_t4source(',v_source_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_t4source(',v_source_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_t4index(table_name,index_name) select 'recon_tmp_t4source',v_index_name;
              end if;

              set v_index_name = concat('idx_',v_comparison_field);

              if not exists(select index_name from recon_tmp_t4index
                            WHERE table_name = 'recon_tmp_t4comparison'
                            and index_name = v_index_name) then

                if substr(v_comparison_field,1,3) = 'col' then
                  set v_sql = concat('alter table recon_tmp_t4comparison modify column ',v_source_field,' varchar(255) default null');
                  call pr_run_sql(v_sql,@msg,@result);

                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_t4comparison(',v_comparison_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_t4comparison(',v_comparison_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_t4index(table_name,index_name) select 'recon_tmp_t4comparison',v_index_name;
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
                set v_sql = concat(v_sql,'update recon_tmp_t4source set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format);

                insert into recon_tmp_t4sql(table_type,tran_acc_mode,sql_query) values ('S',v_source_acc_mode,v_sql);

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
                set v_sql = concat(v_sql,'update recon_tmp_t4comparison set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');
                set v_sql = concat(v_sql,'where tran_acc_mode =',char(39), v_comparison_acc_mode,char(39), ' ');

                insert into recon_tmp_t4sql(table_type,tran_acc_mode,sql_query) values ('C',v_comparison_acc_mode,v_sql);

                set v_sql = '';
                set v_sql = concat(v_sql,'update recon_tmp_t4comparison set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');
                set v_sql = concat(v_sql,'where tran_acc_mode =',char(39), v_source_acc_mode,char(39),' ');

                insert into recon_tmp_t4sql(table_type,tran_acc_mode,sql_query) values ('C',v_source_acc_mode,v_sql);

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
                set v_build_condition = concat(v_build_condition,' ',v_source_field ,' <> '''' ');
              else
                set v_build_condition = concat(v_build_condition,' ',v_source_field ,' is not null ');
              end if;

              set v_build_condition = concat(v_build_condition,' and ');

              if v_comparison_field_org_type = 'TEXT' then
                set v_build_condition = concat(v_build_condition,' ',v_comparison_field ,' <> '''' ');
              else
                set v_build_condition = concat(v_build_condition,' ',v_comparison_field ,' is not null ');
              end if;

              set v_build_condition = concat(v_build_condition,')');

              set v_build_condition = concat(v_build_condition,' ',v_close_parentheses_flag,' ',v_join_condition);

              set v_rule_notnull_condition = concat(v_rule_notnull_condition,v_build_condition);

              set v_rule_groupby = concat(v_rule_groupby,',',v_source_field);
            end loop rule_loop;

            close rule_cursor;
          end rule_block;

          truncate recon_tmp_t4source;
          truncate recon_tmp_t4comparison;

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

          -- source from tran table
          set v_source_sql = v_source_head_sql;

          if v_recontype_code <> 'N' then
            set v_source_sql = concat(v_source_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_source_sql = concat(v_source_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_source_sql = concat(v_source_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

          set v_source_sql = concat(v_source_sql,' and delete_flag = ',char(39),'N',char(39));
          set v_source_sql = concat(v_source_sql,' ',v_source_condition);
          set v_source_sql = concat(v_source_sql,' ',v_sourcebase_filter);

          /*
          if in_automatch_flag = 'N' then
            set v_source_sql = concat(v_source_sql,' and tran_gid not in (select tran_gid from recon_trn_tpreviewdtl');
            set v_source_sql = concat(v_source_sql,' where job_gid = ',char(39),in_job_gid,char(39),' and tranbrkp_gid = 0) ');
          end if;
          */

          select v_source_sql;
          call pr_run_sql(v_source_sql,@result,@msg);

          update recon_tmp_t4source set excp_value = (excp_value - roundoff_value * tran_mult);

          -- select v_source_sql;
          -- leave me;

          -- source from tranbrkp table
          set v_source_sql = v_source_headbrkp_sql;

          if v_recontype_code <> 'N' then
            set v_source_sql = concat(v_source_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_source_sql = concat(v_source_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_source_sql = concat(v_source_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

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

          -- comparison from tran table
          set v_comparison_sql = v_comparison_head_sql;

          if v_recontype_code <> 'N' then
            set v_comparison_sql = concat(v_comparison_sql,' and tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

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

          update recon_tmp_t4comparison set excp_value = (excp_value - roundoff_value * tran_mult);

          -- select v_comparison_sql;

          -- comparison from tranbrkp table
          set v_comparison_sql = v_comparison_headbrkp_sql;

          if v_recontype_code <> 'N' then
            set v_comparison_sql = concat(v_comparison_sql,' and tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

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
            select sql_query from recon_tmp_t4sql
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
            if v_group_method_flag = 'B' and v_source_dataset_code <> v_comparison_dataset_code then
              -- comparison from tran table
              set v_comparison_sql = v_comparison_head_sql;

              if v_recontype_code <> 'N' then
                set v_comparison_sql = concat(v_comparison_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
              end if;

              if v_recon_date_flag = 'Y' then
                set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
                set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
              end if;

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

              update recon_tmp_t4comparison set excp_value = (excp_value - roundoff_value * tran_mult)
              where tran_acc_mode = v_source_acc_mode;

              -- comparison from tranbrkp table
              set v_comparison_sql = v_comparison_headbrkp_sql;

              if v_recontype_code <> 'N' then
                set v_comparison_sql = concat(v_comparison_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
              end if;

              if v_recon_date_flag = 'Y' then
                set v_comparison_sql = concat(v_comparison_sql,' and tran_date >= ',char(39),in_period_from,char(39));
                set v_comparison_sql = concat(v_comparison_sql,' and tran_date <= ',char(39),in_period_to,char(39));
              end if;

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
                select sql_query from recon_tmp_t4sql
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
            delete from recon_tmp_t4source where (tran_gid,tranbrkp_gid) in
            (
              select tran_gid,tranbrkp_gid from recon_trn_tpreviewdtl
              where job_gid = in_job_gid
              and delete_flag = 'N'
            );

            delete from recon_tmp_t4comparison where (tran_gid,tranbrkp_gid) in
            (
              select tran_gid,tranbrkp_gid from recon_trn_tpreviewdtl
              where job_gid = in_job_gid
              and delete_flag = 'N'
            );
          end if;

          -- remove duplicate
          set v_trangid_sql = 'insert into recon_tmp_t4trangid ';
          set v_trangid_sql = concat(v_trangid_sql,'select cast(group_concat(tran_gid) as unsigned) from recon_tmp_t4source as a ') ;

          -- set v_trangid_sql = concat(v_trangid_sql,' where dataset_code = ',char(39),v_source_dataset_code,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' where 1 = 1 ');

          if v_recontype_code <> 'N' then
            set v_trangid_sql = concat(v_trangid_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_trangid_sql = concat(v_trangid_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_trangid_sql = concat(v_trangid_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

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

          set v_trangid_sql = 'insert into recon_tmp_t4tranbrkpgid (tranbrkp_gid) ';
          set v_trangid_sql = concat(v_trangid_sql,'select cast(group_concat(tranbrkp_gid) as unsigned) from recon_tmp_t4source as a ') ;
          -- set v_trangid_sql = concat(v_trangid_sql,' where dataset_code = ',char(39),v_source_dataset_code,char(39));
          set v_trangid_sql = concat(v_trangid_sql,' where 1 = 1 ');

          if v_recontype_code <> 'N' then
            set v_trangid_sql = concat(v_trangid_sql,' and tran_acc_mode = ',char(39),v_source_acc_mode,char(39));
          end if;

          if v_recon_date_flag = 'Y' then
            set v_trangid_sql = concat(v_trangid_sql,' and tran_date >= ',char(39),in_period_from,char(39));
            set v_trangid_sql = concat(v_trangid_sql,' and tran_date <= ',char(39),in_period_to,char(39));
          end if;

          set v_trangid_sql = concat(v_trangid_sql,' and tranbrkp_gid > 0 ');
          set v_trangid_sql = concat(v_trangid_sql,' and delete_flag = ',char(39),'N',char(39));

          if v_recontype_code <> 'N' then
            set v_trangid_sql = concat(v_trangid_sql,' group by tran_value,',substr(v_rule_groupby,2));
          else
            set v_trangid_sql = concat(v_trangid_sql,' group by ',substr(v_rule_groupby,2));
          end if;

          set v_trangid_sql = concat(v_trangid_sql,' having count(*) = 1 ');

          call pr_run_sql(v_trangid_sql,@msg,@result);

          insert into recon_tmp_t4sourcedup select * from recon_tmp_t4source where tran_gid not in (select tran_gid from recon_tmp_t4trangid) and tranbrkp_gid = 0;
          insert into recon_tmp_t4sourcedup select * from recon_tmp_t4source where tranbrkp_gid not in (select tranbrkp_gid from recon_tmp_t4tranbrkpgid) and tranbrkp_gid > 0;

          delete from recon_tmp_t4source where tran_gid not in (select tran_gid from recon_tmp_t4trangid) and tranbrkp_gid = 0;
          delete from recon_tmp_t4source where tranbrkp_gid not in (select tranbrkp_gid from recon_tmp_t4tranbrkpgid) and tranbrkp_gid > 0;

          truncate recon_tmp_t4trangid;
          truncate recon_tmp_t4tranbrkpgid;

          -- find matchoff diff value
          -- plus value
          set v_match_sql = 'insert into recon_tmp_t4thresholddiff (group_flag,tran_gid,tranbrkp_gid,tran_mult,matched_count,diff_value,matched_json) ';
          set v_match_sql = concat(v_match_sql,'select ',char(39),'N',char(39),',');
          set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,a.tran_mult,count(*) as matched_count,(a.excp_value-b.excp_value) as diff_value,');
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
          set v_match_sql = concat(v_match_sql,'from recon_tmp_t4source as a ');
          set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t4comparison as b ');
          set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

          set v_match_sql = concat(v_match_sql,'and (a.excp_value - b.excp_value) > 0 ');
          set v_match_sql = concat(v_match_sql,'and (a.excp_value - b.excp_value) <=  ',cast(v_rule_threshold_plus_value as nchar),' ');

          set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

          if v_recontype_code <> 'N' then
            set v_match_sql = concat(v_match_sql,'and a.tran_acc_mode = ',char(39),v_source_acc_mode,char(39),' ');
            set v_match_sql = concat(v_match_sql,'and b.tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39),' ');
          end if;

          set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid ');
          -- set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid,a.excp_value,b.excp_value ');
          set v_match_sql = concat(v_match_sql,'having count(*) = 1 ');

          call pr_run_sql(v_match_sql,@msg,@result);

          -- minus value
          set v_match_sql = 'insert ignore into recon_tmp_t4thresholddiff (group_flag,tran_gid,tranbrkp_gid,tran_mult,matched_count,diff_value,matched_json) ';
          set v_match_sql = concat(v_match_sql,'select ',char(39),'N',char(39),',');
          set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,a.tran_mult,count(*) as matched_count,(a.excp_value-b.excp_value) as diff_value,');
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
          set v_match_sql = concat(v_match_sql,'from recon_tmp_t4source as a ');
          set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t4comparison as b ');
          set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

          set v_match_sql = concat(v_match_sql,'and (b.excp_value - a.excp_value) > 0 ');
          set v_match_sql = concat(v_match_sql,'and (b.excp_value - a.excp_value) <=  ',cast(v_rule_threshold_minus_value as nchar),' ');

          set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

          if v_recontype_code <> 'N' then
            set v_match_sql = concat(v_match_sql,'and a.tran_acc_mode = ',char(39),v_source_acc_mode,char(39),' ');
            set v_match_sql = concat(v_match_sql,'and b.tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39),' ');
          end if;

          set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid ');
          -- set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid,a.excp_value,b.excp_value ');
          set v_match_sql = concat(v_match_sql,'having count(*) = 1 ');

          call pr_run_sql(v_match_sql,@msg,@result);

          truncate recon_tmp_t4pseudorows;
          insert into recon_tmp_t4pseudorows select 0 union select 1;

          insert into recon_tmp_t4thresholddiffdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,src_comp_flag,tran_acc_mode)
            select
              tran_gid as parent_tran_gid,
              tranbrkp_gid as parent_tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_gid'))) AS tran_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].ko_value'))) AS ko_value,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].src_comp_flag'))) AS src_comp_flag,
              JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_acc_mode'))) AS tran_acc_mode
            FROM recon_tmp_t4thresholddiff
            JOIN recon_tmp_t4pseudorows
            where group_flag = 'N'
            HAVING tran_gid IS NOT NULL;

          if v_group_flag = 'Y' then
            -- plus value
            set v_match_sql = 'insert ignore into recon_tmp_t4thresholddiff (group_flag,tran_gid,tranbrkp_gid,matched_count,diff_value,tran_mult,matched_json) ';
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

            set v_match_sql = concat(v_match_sql,'from recon_tmp_t4source as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t4comparison as b ');
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
            set v_match_sql = concat(v_match_sql,'and (abs(a.excp_value*a.tran_mult) - abs(sum(b.excp_value*b.tran_mult))) <= ',cast(v_rule_threshold_plus_value  as nchar),' ');

            call pr_run_sql(v_match_sql,@msg,@result);

            insert ignore into recon_tmp_t4gid select tran_gid,tranbrkp_gid from recon_tmp_t4thresholddiff;

            -- minus value
            set v_match_sql = 'insert ignore into recon_tmp_t4thresholddiff (group_flag,tran_gid,tranbrkp_gid,matched_count,diff_value,tran_mult,matched_json) ';
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

            set v_match_sql = concat(v_match_sql,'from recon_tmp_t4source as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t4comparison as b ');
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
            set v_match_sql = concat(v_match_sql,'and (abs(sum(b.excp_value*b.tran_mult)) - abs(a.excp_value*a.tran_mult)) <= ',cast(v_rule_threshold_minus_value as nchar),' ');

            call pr_run_sql(v_match_sql,@msg,@result);

            select max(matched_count) into v_count from recon_tmp_t4thresholddiff;
            set v_count = ifnull(v_count,0);

            truncate recon_tmp_t4pseudorows;

            if v_count >= 2 then
              insert into recon_tmp_t4pseudorows select row from pseudo_rows1 where row <= v_count;
            else
              insert into recon_tmp_t4pseudorows select 0 union select 1;
            end if;

            insert into recon_tmp_t4thresholddiffdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,src_comp_flag,tran_acc_mode)
              select
                tran_gid as parent_tran_gid,
                tranbrkp_gid as parent_tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_gid'))) AS tran_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].ko_value'))) AS ko_value,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].src_comp_flag'))) AS src_comp_flag,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_acc_mode'))) AS tran_acc_mode
              FROM recon_tmp_t4thresholddiff
              JOIN recon_tmp_t4pseudorows
              where group_flag = 'Y'
              HAVING tran_gid IS NOT NULL;
          end if;

          -- update match flag in source table
          update recon_tmp_t4source as a
          inner join recon_tmp_t4thresholddiffdtl as b on a.tran_gid = b.tran_gid
            and a.tranbrkp_gid = b.tranbrkp_gid
          set a.match_flag = 'Y';

          -- update match flag in comparison table
          update recon_tmp_t4comparison as a
          inner join recon_tmp_t4thresholddiffdtl as b on a.tran_gid = b.tran_gid
            and a.tranbrkp_gid = b.tranbrkp_gid
          set a.match_flag = 'Y';

					-- many to many match
					if v_manytomany_match_flag = 'Y' then
						set v_match_sql = 'insert ignore into recon_tmp_t4manymatch (tran_gid,tranbrkp_gid,matched_count,';
						set v_match_sql = concat(v_match_sql,'tran_mult,tran_acc_mode,source_value,comparison_value,matched_txt_json) ');
						set v_match_sql = concat(v_match_sql,'select ');
						set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,a.tran_mult,a.tran_acc_mode,');
						set v_match_sql = concat(v_match_sql,'a.excp_value as source_value,sum(b.excp_value*b.tran_mult) as comparison_value,');

						set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[',char(39),',');
						set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
						set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(b.tran_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(b.tranbrkp_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(b.tran_mult as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"src_comp_flag":"C",');
            set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',b.tran_acc_mode,',char(39),'",');
						set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(b.excp_value as nchar),',char(39));
						set v_match_sql = concat(v_match_sql,'}',char(39),' order by b.tran_gid,b.tranbrkp_gid),');
						set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json) as matched_json ');

						set v_match_sql = concat(v_match_sql,'from recon_tmp_t4source as a ');
						set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t4comparison as b ');
						set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

						set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

            set v_match_sql = concat(v_match_sql,"where a.match_flag = 'N'
              and b.match_flag = 'N'");

						set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_rule_groupby,' ');

            -- add record order by
            if v_recorder <> '' then
              set v_match_sql = concat(v_match_sql,v_recorder);
            end if;

						call pr_run_sql(v_match_sql,@msg,@result);

            -- plus value
						-- insert in match table
						set v_match_sql = 'insert into recon_tmp_t4thresholddiff (group_flag,tran_gid,tranbrkp_gid,matched_count,diff_value,tran_mult,matched_json) ';
						set v_match_sql = concat(v_match_sql,'select ',char(39),'M',char(39),',');
						set v_match_sql = concat(v_match_sql,'max(tran_gid),max(tranbrkp_gid),sum(matched_count)+count(*) as matched_count,');
						set v_match_sql = concat(v_match_sql,'(abs(sum(source_value*tran_mult)) - abs(comparison_value)),tran_mult,');
						set v_match_sql = concat(v_match_sql,'JSON_MERGE_PRESERVE(');
						set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[',char(39),',');
						set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
						set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(tran_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(tranbrkp_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(tran_mult as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');
            set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',tran_acc_mode,',char(39),'",');
						set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(source_value as nchar),',char(39));
						set v_match_sql = concat(v_match_sql,'}',char(39),'),');
						set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json),matched_txt_json) as matched_json ');

						set v_match_sql = concat(v_match_sql,'from recon_tmp_t4manymatch ');
            set v_match_sql = concat(v_match_sql,'group by matched_txt_json,comparison_value,tran_mult ');
            set v_match_sql = concat(v_match_sql,'having (abs(sum(source_value*tran_mult))-abs(comparison_value)) > 0 ');
            set v_match_sql = concat(v_match_sql,'and (abs(sum(source_value*tran_mult))-abs(comparison_value)) <= ',cast(v_rule_threshold_plus_value as nchar),' ');

            select v_match_sql;
						call pr_run_sql(v_match_sql,@msg,@result);

            -- minus value
						-- insert in match table
						set v_match_sql = 'insert into recon_tmp_t4thresholddiff (group_flag,tran_gid,tranbrkp_gid,matched_count,diff_value,tran_mult,matched_json) ';
						set v_match_sql = concat(v_match_sql,'select ',char(39),'M',char(39),',');
						set v_match_sql = concat(v_match_sql,'max(tran_gid),max(tranbrkp_gid),sum(matched_count)+count(*) as matched_count,');
						set v_match_sql = concat(v_match_sql,'(abs(sum(source_value*tran_mult)) - abs(comparison_value)),tran_mult,');
						set v_match_sql = concat(v_match_sql,'JSON_MERGE_PRESERVE(');
						set v_match_sql = concat(v_match_sql,'cast(concat(',char(39),'[',char(39),',');
						set v_match_sql = concat(v_match_sql,'group_concat(',char(39),'{');
						set v_match_sql = concat(v_match_sql,'"tran_gid":',char(39),',cast(tran_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tranbrkp_gid":',char(39),',cast(tranbrkp_gid as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"tran_mult":',char(39),',cast(tran_mult as nchar),',char(39),',');
						set v_match_sql = concat(v_match_sql,'"src_comp_flag":"S",');
            set v_match_sql = concat(v_match_sql,'"tran_acc_mode":"',char(39),',tran_acc_mode,',char(39),'",');
						set v_match_sql = concat(v_match_sql,'"ko_value":',char(39),',cast(source_value as nchar),',char(39));
						set v_match_sql = concat(v_match_sql,'}',char(39),'),');
						set v_match_sql = concat(v_match_sql,char(39), ']',char(39),') as json),matched_txt_json) as matched_json ');

						set v_match_sql = concat(v_match_sql,'from recon_tmp_t4manymatch ');
            set v_match_sql = concat(v_match_sql,'group by matched_txt_json,comparison_value,tran_mult ');
            set v_match_sql = concat(v_match_sql,'having (abs(comparison_value)-abs(sum(source_value*tran_mult))) > 0 ');
            set v_match_sql = concat(v_match_sql,'and (abs(comparison_value)-abs(sum(source_value*tran_mult))) <= ',cast(v_rule_threshold_minus_value as nchar),' ');

            select v_match_sql;
						call pr_run_sql(v_match_sql,@msg,@result);

						select max(matched_count) into v_count from recon_tmp_t4thresholddiff;
						set v_count = ifnull(v_count,0);

						truncate recon_tmp_t4pseudorows;

						if v_count >= 2 then
							insert into recon_tmp_t4pseudorows select row from pseudo_rows1 where row <= v_count;
						else
							insert into recon_tmp_t4pseudorows select 0 union select 1;
						end if;

            insert into recon_tmp_t4thresholddiffdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,src_comp_flag,tran_acc_mode)
              select
                tran_gid as parent_tran_gid,
                tranbrkp_gid as parent_tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_gid'))) AS tran_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].ko_value'))) AS ko_value,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].src_comp_flag'))) AS src_comp_flag,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4thresholddiff.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_acc_mode'))) AS tran_acc_mode
              FROM recon_tmp_t4thresholddiff
              JOIN recon_tmp_t4pseudorows
              where group_flag = 'M'
              HAVING tran_gid IS NOT NULL;
					end if;

          -- Find Duplicates
          truncate recon_tmp_t4matchdup;
          truncate recon_tmp_t4matchparentgid;

          -- duplicate validation
          insert into recon_tmp_t4matchdup (tran_gid,tranbrkp_gid,rec_count)
            select tran_gid,tranbrkp_gid,count(*) from recon_tmp_t4thresholddiffdtl
            group by tran_gid,tranbrkp_gid
            having count(*) > 1;

          insert into recon_tmp_t4matchparentgid(parent_tran_gid,parent_tranbrkp_gid)
            select b.parent_tran_gid,b.parent_tranbrkp_gid from recon_tmp_t4matchdup as a
            inner join recon_tmp_t4thresholddiffdtl as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
            group by b.parent_tran_gid,b.parent_tranbrkp_gid;

          update recon_tmp_t4thresholddiff as a
          inner join recon_tmp_t4matchparentgid as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
          set a.dup_flag = 'Y';

          update recon_tmp_t4thresholddiff as a
          inner join recon_tmp_t4thresholddiffdtl as b on a.tran_gid = b.tran_gid
            and a.tranbrkp_gid = b.tranbrkp_gid
            and a.dup_flag = 'N'
          set a.tran_acc_mode = b.tran_acc_mode;

          -- source block
          source_block:begin
            declare source_done int default 0;
            declare source_cursor cursor for
            select tran_gid,tranbrkp_gid,tran_acc_mode,diff_value,matched_count from recon_tmp_t4thresholddiff
               where dup_flag = 'N';
            declare continue handler for not found set source_done=1;

            open source_cursor;

            source_loop: loop
              fetch source_cursor into v_tran_gid,v_tranbrkp_gid,v_src_acc_mode,v_diff_value,v_matched_count;
              if source_done = 1 then leave source_loop; end if;

              if v_recontype_code = 'B'
                or v_recontype_code = 'W'
                or (v_recontype_code = 'I' and v_source_dataset_code = v_comparison_dataset_code) then
                if v_src_acc_mode = 'C' then
                  set v_cmp_acc_mode = 'D';
                else
                  set v_cmp_acc_mode = 'C';
                end if;
              else
                set v_cmp_acc_mode = v_src_acc_mode;
              end if;

              if v_diff_value > 0 then
                set v_src_comp_flag = 'S';
                set v_tran_acc_mode = v_src_acc_mode;
              else
                set v_src_comp_flag = 'C';
                set v_tran_acc_mode = v_cmp_acc_mode;
              end if;

              -- adjust difference value
							thresholddiff_block:begin
								declare thresholddiff_done int default 0;
								declare thresholddiff_cursor cursor for
								select tran_gid,tranbrkp_gid,ko_value from recon_tmp_t4thresholddiffdtl
									where parent_tran_gid = v_tran_gid
									and parent_tranbrkp_gid = v_tranbrkp_gid
									and src_comp_flag = v_src_comp_flag
									and tran_acc_mode = v_tran_acc_mode
									order by ko_value;
								declare continue handler for not found set thresholddiff_done=1;

								open thresholddiff_cursor;

								thresholddiff_loop: loop
									fetch thresholddiff_cursor into v_tran_gid,v_tranbrkp_gid,v_ko_value;
									if thresholddiff_done = 1 then leave thresholddiff_loop; end if;

									if v_ko_value >= v_diff_value then
                    if v_src_comp_flag = 'S' then
										  update recon_tmp_t4source set
											  excp_value = excp_value - v_diff_value * tran_mult
										  where tran_gid = v_tran_gid
										  and tranbrkp_gid = v_tranbrkp_gid;
                    else
										  update recon_tmp_t4comparison set
											  excp_value = excp_value - v_diff_value * tran_mult
										  where tran_gid = v_tran_gid
										  and tranbrkp_gid = v_tranbrkp_gid;
                    end if;

										-- insert roundoff value
										insert into recon_tmp_t4tranroundoff (tran_gid,tranbrkp_gid,roundoff_value)
											select v_tran_gid,v_tranbrkp_gid,v_diff_value;

										leave thresholddiff_loop;
									else
                    if v_src_comp_flag = 'S' then
										  update recon_tmp_t4source set
											  excp_value = 0
										  where tran_gid = v_tran_gid
										  and tranbrkp_gid = v_tranbrkp_gid;
                    else
										  update recon_tmp_t4comparison set
											  excp_value = 0
										  where tran_gid = v_tran_gid
										  and tranbrkp_gid = v_tranbrkp_gid;
                    end if;

										set v_diff_value = v_diff_value - v_ko_value;
									end if;
								end loop thresholddiff_loop;

								close thresholddiff_cursor;
							end thresholddiff_block;
            end loop source_loop;
            close source_cursor;
          end source_block;

          truncate recon_tmp_t4manymatch;

          truncate recon_tmp_t4matchdup;
          truncate recon_tmp_t4matchparentgid;

          truncate recon_tmp_t4thresholddiff;
          truncate recon_tmp_t4thresholddiffdtl;

          truncate recon_tmp_t4pseudorows;

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
						set v_match_sql = 'insert ignore into recon_tmp_t4manymatch (tran_gid,tranbrkp_gid,matched_count,';
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

						set v_match_sql = concat(v_match_sql,'from recon_tmp_t4source as a ');
						set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t4comparison as b ');
						set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

						set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

						set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_rule_groupby,' ');

            -- add record order by
            if v_recorder <> '' then
              set v_match_sql = concat(v_match_sql,v_recorder);
            end if;

            -- select v_match_sql;
						call pr_run_sql(v_match_sql,@msg,@result);

						-- insert in match table
						set v_match_sql = 'insert into recon_tmp_t4match (group_flag,tran_gid,tranbrkp_gid,matched_count,matched_value,tran_mult,matched_json) ';
						set v_match_sql = concat(v_match_sql,'select ',char(39),'M',char(39),',');
						set v_match_sql = concat(v_match_sql,'max(tran_gid),max(tranbrkp_gid),sum(matched_count)+count(*) as matched_count,');
						set v_match_sql = concat(v_match_sql,'comparison_value as matched_value,tran_mult,');

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

						set v_match_sql = concat(v_match_sql,'from recon_tmp_t4manymatch ');

						if v_recontype_code <> 'N' then
							set v_match_sql = concat(v_match_sql,'group by matched_txt_json,comparison_value,tran_mult ');

              if (v_recontype_code <> 'I' and v_recontype_code <> 'V') or v_reversal_flag = 'Y' then
                -- contra
							  set v_match_sql = concat(v_match_sql,'having sum(source_value*tran_mult) = (comparison_value*-1) ');
              else
                -- mirror
							  set v_match_sql = concat(v_match_sql,'having sum(source_value*tran_mult) = comparison_value ');
              end if;
						else
							set v_match_sql = concat(v_match_sql,'group by matched_txt_json ');
						end if;

            -- select v_match_sql;
            select v_match_sql;
						call pr_run_sql(v_match_sql,@msg,@result);

						select max(matched_count) into v_count from recon_tmp_t4match;
						set v_count = ifnull(v_count,0);

						truncate recon_tmp_t4pseudorows;

						if v_count >= 2 then
							insert into recon_tmp_t4pseudorows select row from pseudo_rows1 where row <= v_count;
						else
							insert into recon_tmp_t4pseudorows select 0 union select 1;
						end if;

						insert into recon_tmp_t4matchdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult,src_comp_flag)
							select
								tran_gid as parent_tran_gid,
								tranbrkp_gid as parent_tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_gid'))) AS tran_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].ko_value'))) AS ko_value,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_mult'))) AS tran_mult,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].src_comp_flag'))) AS src_comp_flag
							FROM recon_tmp_t4match
							JOIN recon_tmp_t4pseudorows
							where group_flag = 'M'
							HAVING tran_gid IS NOT NULL;

						-- clear matched records
						truncate recon_tmp_t4trangid;

						insert into recon_tmp_t4trangid
							select distinct tran_gid from recon_tmp_t4matchdtl where tran_gid > 0 and tranbrkp_gid = 0;

						delete a.* from recon_tmp_t4source as a
            where a.tran_gid in (select b.tran_gid from recon_tmp_t4trangid as b where a.tran_gid = b.tran_gid);

						delete a.* from recon_tmp_t4comparison as a
            where a.tran_gid in (select b.tran_gid from recon_tmp_t4trangid as b where a.tran_gid = b.tran_gid);

						truncate recon_tmp_t4tranbrkpgid;

						insert into recon_tmp_t4tranbrkpgid (tranbrkp_gid)
							select distinct tranbrkp_gid from recon_tmp_t4matchdtl where tranbrkp_gid > 0;

						delete a.* from recon_tmp_t4source as a
            where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_t4tranbrkpgid as b
              where a.tranbrkp_gid = b.tranbrkp_gid);

						delete a.* from recon_tmp_t4comparison as a
            where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_t4tranbrkpgid as b
              where a.tranbrkp_gid = b.tranbrkp_gid);

						truncate recon_tmp_t4trangid;
						truncate recon_tmp_t4tranbrkpgid;
					end if;

          -- leave me;

					-- one to many match
					 if v_group_flag = 'Y' and v_manytomany_match_flag = 'N' then
            set v_match_sql = 'insert into recon_tmp_t4match (group_flag,tran_gid,tranbrkp_gid,matched_count,matched_value,tran_mult,matched_json) ';
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

            set v_match_sql = concat(v_match_sql,'from recon_tmp_t4source as a ');
            set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t4comparison as b ');
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
              if (v_recontype_code <> 'I' and v_recontype_code <> 'V') or v_reversal_flag = 'Y' then
                -- contra
                set v_match_sql = concat(v_match_sql,'and a.excp_value*a.tran_mult = sum(b.excp_value*b.tran_mult)*-1 ');
              else
                -- mirror
                set v_match_sql = concat(v_match_sql,'and a.excp_value*a.tran_mult = sum(b.excp_value*b.tran_mult) ');
              end if;
            end if;

            -- add record order by
            if v_recorder <> '' then
              set v_match_sql = concat(v_match_sql,v_recorder);
            end if;

            -- run match query one to many
            call pr_run_sql(v_match_sql,@msg,@result);

            select max(matched_count) into v_count from recon_tmp_t4match;
            set v_count = ifnull(v_count,0);

            truncate recon_tmp_t4pseudorows;

            if v_count >= 2 then
              insert into recon_tmp_t4pseudorows select row from pseudo_rows1 where row <= v_count;
            else
              insert into recon_tmp_t4pseudorows select 0 union select 1;
            end if;

            insert into recon_tmp_t4matchdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult,src_comp_flag)
              select
                tran_gid as parent_tran_gid,
                tranbrkp_gid as parent_tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_gid'))) AS tran_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].ko_value'))) AS ko_value,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_mult'))) AS tran_mult,
                JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].src_comp_flag'))) AS src_comp_flag
              FROM recon_tmp_t4match
              JOIN recon_tmp_t4pseudorows
              where group_flag = 'Y'
              HAVING tran_gid IS NOT NULL;

						-- clear matched records
						truncate recon_tmp_t4trangid;

						insert into recon_tmp_t4trangid
							select distinct tran_gid from recon_tmp_t4matchdtl where tran_gid > 0 and tranbrkp_gid = 0;

						delete a.* from recon_tmp_t4source as a
            where a.tran_gid in (select b.tran_gid from recon_tmp_t4trangid as b where a.tran_gid = b.tran_gid);

						delete a.* from recon_tmp_t4comparison as a
            where a.tran_gid in (select b.tran_gid from recon_tmp_t4trangid as b where a.tran_gid = b.tran_gid);

						truncate recon_tmp_t4tranbrkpgid;

						insert into recon_tmp_t4tranbrkpgid (tranbrkp_gid)
							select distinct tranbrkp_gid from recon_tmp_t4matchdtl where tranbrkp_gid > 0;

						delete a.* from recon_tmp_t4source as a
            where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_t4tranbrkpgid as b
              where a.tranbrkp_gid = b.tranbrkp_gid);

						delete a.* from recon_tmp_t4comparison as a
            where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_t4tranbrkpgid as b
              where a.tranbrkp_gid = b.tranbrkp_gid);

						truncate recon_tmp_t4trangid;
						truncate recon_tmp_t4tranbrkpgid;
					 end if;

					-- one to one match
          if v_manytomany_match_flag = 'N' then
						set v_match_sql = 'insert into recon_tmp_t4match (group_flag,tran_gid,tranbrkp_gid,tran_mult,matched_count,matched_value,matched_json) ';
						set v_match_sql = concat(v_match_sql,'select ',char(39),'N',char(39),',');
						set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,a.tran_mult,count(*) as matched_count,');

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
						set v_match_sql = concat(v_match_sql,'from recon_tmp_t4source as a ');
						set v_match_sql = concat(v_match_sql,'inner join recon_tmp_t4comparison as b ');
						set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

						if v_recontype_code <> 'N' then
							set v_match_sql = concat(v_match_sql,'and a.excp_value = b.excp_value ');
						end if;

						set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

            if v_recontype_code <> 'N' then
						  set v_match_sql = concat(v_match_sql,'where a.tran_acc_mode = ',char(39),v_source_acc_mode,char(39),' ');
						  set v_match_sql = concat(v_match_sql,'and b.tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39),' ');
						  -- set v_match_sql = concat(v_match_sql,'and 1 = 1 ');
            end if;

						set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid ');
						set v_match_sql = concat(v_match_sql,'having count(*) = 1 ');

            -- add record order by
            if v_recorder <> '' then
              set v_match_sql = concat(v_match_sql,v_recorder);
            end if;

            -- run match query one to one
						call pr_run_sql(v_match_sql,@msg,@result);

						-- pseudorows
						select max(matched_count) into v_count from recon_tmp_t4match;
						set v_count = ifnull(v_count,0);

						truncate recon_tmp_t4pseudorows;

						if v_count >= 2 then
							insert into recon_tmp_t4pseudorows select row from pseudo_rows1 where row <= v_count;
						else
							insert into recon_tmp_t4pseudorows select 0 union select 1;
						end if;

						-- select v_source_sql,v_comparison_sql,v_match_sql;
						-- leave me;

						insert into recon_tmp_t4matchdtl (parent_tran_gid,parent_tranbrkp_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult,src_comp_flag)
							select
								tran_gid as parent_tran_gid,
								tranbrkp_gid as parent_tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_gid'))) AS tran_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].ko_value'))) AS ko_value,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_mult'))) AS tran_mult,
								JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_t4match.matched_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].src_comp_flag'))) AS src_comp_flag
							FROM recon_tmp_t4match
							JOIN recon_tmp_t4pseudorows
							where group_flag = 'N'
							HAVING tran_gid IS NOT NULL;
          end if;

          -- duplicate validation
          insert into recon_tmp_t4matchdup (tran_gid,tranbrkp_gid,rec_count)
            select tran_gid,tranbrkp_gid,count(*) from recon_tmp_t4matchdtl
            group by tran_gid,tranbrkp_gid
            having count(*) > 1;

          insert into recon_tmp_t4matchparentgid(parent_tran_gid,parent_tranbrkp_gid)
            select b.parent_tran_gid,b.parent_tranbrkp_gid from recon_tmp_t4matchdup as a
            inner join recon_tmp_t4matchdtl as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
            group by b.parent_tran_gid,b.parent_tranbrkp_gid;

          update recon_tmp_t4match as a
          inner join recon_tmp_t4matchparentgid as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
          set a.dup_flag = 'Y';

          update recon_tmp_t4match set
            matched_value = abs(matched_value),
            ko_flag = 'Y'
          where dup_flag = 'N';

          if in_automatch_flag = 'Y' then
            truncate recon_tmp_t4matchko;

            if v_recontype_code <> 'N' then
              -- match diff
							set v_sql = concat("
              insert into recon_tmp_t4matchdiff(tran_gid,tran_value,excp_value,mapped_value,tran_mult,diff_value)
              select
                a.tran_gid,b.tran_value,b.excp_value,b.mapped_value,b.tran_mult,
                b.excp_value - sum(a.ko_value*a.tran_mult)*b.tran_mult
              from recon_tmp_t4match as m
              inner join recon_tmp_t4matchdtl as a on m.tran_gid = a.parent_tran_gid and m.tranbrkp_gid = a.parent_tranbrkp_gid
              inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
                and b.excp_value > 0
                and b.delete_flag = 'N'
              where m.dup_flag = 'N'
              and m.ko_flag = 'Y'
              group by a.tran_gid,b.tran_value,b.excp_value,b.tran_mult
              having b.excp_value < sum(a.ko_value*a.tran_mult)*b.tran_mult");

							call pr_run_sql(v_sql,@msg,@result);

              if exists(select * from recon_tmp_t4matchdiff) then
								-- diff block
								diff_block:begin
									declare diff_done int default 0;
									declare diff_cursor cursor for
									select tran_gid,tran_mult,mapped_value,diff_value from recon_tmp_t4matchdiff;
									declare continue handler for not found set diff_done=1;

									open diff_cursor;

									diff_loop: loop
										fetch diff_cursor into v_tran_gid,v_tran_mult,v_mapped_value,v_diff_value;
										if diff_done = 1 then leave diff_loop; end if;

                    if v_mapped_value > 0 then
                      select
                        count(*),
                        sum(a.excp_value*a.tran_mult)
                      into
                        v_count,
                        v_excp_value
                      from
                      (
                        select tranbrkp_gid,excp_value,tran_mult from recon_tmp_t4source
                        where tran_gid = v_tran_gid
                        and tranbrkp_gid > 0

                        union

                        select tranbrkp_gid,excp_value,tran_mult from recon_tmp_t4comparison
                        where tran_gid = v_tran_gid
                        and tranbrkp_gid > 0
                      ) as a;

                      set v_count = ifnull(v_count,0);
                      set v_excp_value = ifnull(v_excp_value,0);

                      -- if not able to locate the diff with in source and comparison
                      --   then directly check it with tranbrkp
                      if v_count = 0 or v_excp_value <> v_diff_value then
                        set v_sql = concat("
													select
														count(*),
														sum(a.excp_value*a.tran_mult)
													into
														@v_count,
														@v_excp_value
													from
													(
														select b.tranbrkp_gid,b.excp_value,b.tran_mult
														from ",v_tranbrkp_table," as b
														where b.tran_gid = ",cast(v_tran_gid as nchar),"
														and b.tranbrkp_gid not in
														(
															select
																a.tranbrkp_gid
															from recon_tmp_t4matchdtl as a
															inner join recon_tmp_t4match as m on m.tran_gid = a.parent_tran_gid and m.tranbrkp_gid = a.parent_tranbrkp_gid
															where a.tran_gid = ",cast(v_tran_gid as nchar),"
															and m.dup_flag = 'N'
															and m.ko_flag = 'Y'
														)
													) as a");

							          call pr_run_sql(v_sql,@msg,@result);

												set v_count = ifnull(@v_count,0);
												set v_excp_value = ifnull(@v_excp_value,0);
                      end if;

                      if v_count = 0 or v_excp_value <> v_diff_value then
                      -- or (v_excp_value < v_diff_value and v_diff_value <> 0) then
												select
													a.parent_tran_gid,
													a.parent_tranbrkp_gid,
													b.matched_count,
													b.matched_value
												into
													v_parent_tran_gid,
													v_parent_tranbrkp_gid,
													v_matched_count,
													v_matched_value
												from recon_tmp_t4matchdtl as a
												inner join recon_tmp_t4match as b on a.parent_tran_gid = b.tran_gid and a.parent_tranbrkp_gid = b.tranbrkp_gid
												where a.tran_gid = v_tran_gid
												and b.ko_flag = 'Y'
												group by a.parent_tran_gid,a.parent_tranbrkp_gid,b.matched_count,b.matched_value
												having sum(a.ko_value*a.tran_mult)*v_tran_mult > v_diff_value
												order by sum(a.ko_value*a.tran_mult)*v_tran_mult,count(*)
												limit 0,1;

												update recon_tmp_t4match set
													ko_flag = 'D',
													dup_flag = 'D'
												where tran_gid = v_parent_tran_gid
												and tranbrkp_gid = v_parent_tranbrkp_gid;

												insert ignore into recon_tmp_t4matchdiffdtl
													select * from recon_tmp_t4matchdtl
													where parent_tran_gid = v_parent_tran_gid
													and parent_tranbrkp_gid = v_parent_tranbrkp_gid;
                      end if;
                    end if;

									end loop diff_loop;
									close diff_cursor;
								end diff_block;

                -- more than one record marked as duplicate
                if exists(select * from recon_tmp_t4match
                  where ko_flag = 'D') then
                  truncate recon_tmp_t4trangid;

                  insert into recon_tmp_t4trangid(tran_gid)
                    select tran_gid from recon_tmp_t4matchdiffdtl
                    group by tran_gid
                    having count(*) > 1;

                  insert into recon_tmp_t4tranwithbrkpgid(tran_gid,tranbrkp_gid,rec_count)
                    select a.parent_tran_gid,a.parent_tranbrkp_gid,count(*) from recon_tmp_t4matchdiffdtl as a
                    inner join recon_tmp_t4trangid as b on a.tran_gid = b.tran_gid
                    group by a.parent_tran_gid,a.parent_tranbrkp_gid;

                  update recon_tmp_t4match as a
                  inner join recon_tmp_t4tranwithbrkpgid as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
                  set
                    a.dup_flag = 'N',
                    a.ko_flag = 'Y'
                  where b.rec_count = 1
                  and a.ko_flag = 'D';
                end if;
              end if;

              update recon_tmp_t4match as a
              inner join recon_tmp_t4matchdtl as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
                and b.tranbrkp_gid > 0
                and b.ko_flag = 'N'
              set b.ko_flag = 'Y'
              where a.ko_flag = 'Y' and a.dup_flag = 'N';

              -- knockoff validation
							set v_sql = concat("
								insert into recon_tmp_t4matchko (tran_gid,ko_value,excp_value)
								select
									a.tran_gid,sum(a.ko_value*a.tran_mult)*b.tran_mult,b.excp_value
								from recon_tmp_t4match as m
								inner join recon_tmp_t4matchdtl as a on m.tran_gid = a.parent_tran_gid and m.tranbrkp_gid = a.parent_tranbrkp_gid
								inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
									and b.excp_value <> 0
									and b.mapped_value = 0
									and b.delete_flag = 'N'
								where m.dup_flag = 'N'
								and m.ko_flag = 'Y'
								group by a.tran_gid,b.tran_mult
								having b.excp_value >= sum(a.ko_value*a.tran_mult)*b.tran_mult");

							call pr_run_sql(v_sql,@msg,@result);

              update recon_tmp_t4matchdtl as a
              inner join recon_tmp_t4matchko as b on a.tran_gid = b.tran_gid
              set a.ko_flag = 'Y';

              update recon_tmp_t4match as a
              inner join recon_tmp_t4matchdtl as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
                and b.ko_flag = 'N'
              set a.ko_flag = 'N'
              where a.dup_flag = 'N';
            end if;

            leave me;

            -- insert into knockoff
						set v_sql = concat("
							insert into ",v_ko_table,"
							(
								job_gid,ko_date,ko_value,recon_code,rule_code,
								reversal_flag,manual_matchoff,kodtl_json,kodtl_post_flag,insert_date,insert_by
							)
							select
								",cast(in_job_gid as nchar),",
								curdate(),
								matched_value,
								'",in_recon_code,"',
								'",v_rule_code,"',
								'",v_reversal_flag,"',
								'N',matched_json,'N',sysdate(),'",in_user_code,"'
							from recon_tmp_t4match
							where ko_flag = 'Y'");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("
							insert into recon_tmp_t4kodtl
							( ko_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult)
							select
								ko_gid,
								JSON_UNQUOTE(JSON_EXTRACT(",v_ko_table,".kodtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_gid'))) AS tran_gid,
								JSON_UNQUOTE(JSON_EXTRACT(",v_ko_table,".kodtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(",v_ko_table,".kodtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].ko_value'))) AS excp_value,
								JSON_UNQUOTE(JSON_EXTRACT(",v_ko_table,".kodtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_mult'))) AS tran_mult
							FROM ",v_ko_table,"
							JOIN recon_tmp_t4pseudorows
							where job_gid = ",cast(in_job_gid as nchar),"
							and kodtl_post_flag = 'N'
							HAVING tran_gid IS NOT NULL
							order by ko_gid");

						call pr_run_sql(v_sql,@msg,@result);

            -- insert in kodtl table
						set v_sql = concat("
							insert into ",v_kodtl_table," (ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult)
								select ko_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult from recon_tmp_t4kodtl");

						call pr_run_sql(v_sql,@msg,@result);

            if v_recontype_code <> 'N' then
              -- insert in ko roundoff table
							set v_sql = concat("
								insert into ",v_koroundoff_table," (ko_gid,tran_gid,tranbrkp_gid,roundoff_value)
									select
										b.ko_gid,b.tran_gid,b.tranbrkp_gid,a.roundoff_value
									from recon_tmp_t4tranroundoff as a
									inner join recon_tmp_t4kodtl as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid");

							call pr_run_sql(v_sql,@msg,@result);

              insert into recon_tmp_t4kodtlsumm (max_ko_gid,tran_gid,ko_value,rec_count)
              select
                max(ko_gid) as max_ko_gid,tran_gid,sum(ko_value*tran_mult) as ko_value,count(*) as rec_count
              from recon_tmp_t4kodtl
              group by tran_gid;

              -- roundoff tranbrkp_gid = 0 cases
              update recon_tmp_t4kodtlsumm as a
              inner join recon_tmp_t4tranroundoff as b on a.tran_gid = b.tran_gid
                and b.tranbrkp_gid = 0
              set a.roundoff_value = a.roundoff_value + b.roundoff_value;

              -- roundoff tranbrkp_gid > 0 cases
              update recon_tmp_t4kodtlsumm as a
              inner join
              (
                select tran_gid,sum(roundoff_value) as roundoff_value from recon_tmp_t4tranroundoff
                where tranbrkp_gid > 0
                group by tran_gid
              ) as b on a.tran_gid = b.tran_gid
              set a.roundoff_value = a.roundoff_value + b.roundoff_value;

              -- update in tran table
							set v_sql = concat("
								update ",v_tran_table," as a
								inner join recon_tmp_t4kodtlsumm as b on a.tran_gid = b.tran_gid
								set a.excp_value = a.excp_value - (b.ko_value * a.tran_mult),
										a.roundoff_value = a.roundoff_value + b.roundoff_value,
										a.ko_gid = b.max_ko_gid,
										a.ko_date = curdate(),
										a.theme_code = ''
								where ((a.excp_value <> 0 and a.mapped_value = 0) or a.mapped_value > 0)
								and a.delete_flag = 'N'");

							call pr_run_sql(v_sql,@msg,@result);

              /*
              -- zero roundoff value
							set v_sql = concat("
								update ",v_tran_table," as a
								inner join recon_tmp_t4kodtlsumm as b on a.tran_gid = b.tran_gid
								set a.excp_value = a.excp_value - a.roundoff_value
								where a.excp_value = a.roundoff_value
                and a.roundoff_value <> 0
								and a.delete_flag = 'N'");

							call pr_run_sql(v_sql,@msg,@result);
              */

              -- update in tranbrkp table
							set v_sql = concat("
								update ",v_tranbrkp_table," as a
								inner join recon_tmp_t4kodtl as b on a.tranbrkp_gid = b.tranbrkp_gid
								set a.excp_value = a.excp_value - b.ko_value,
										a.ko_gid = b.ko_gid,
										a.ko_date = curdate(),
										a.theme_code = ''
								where a.excp_value <> 0
								and a.delete_flag = 'N'");

							call pr_run_sql(v_sql,@msg,@result);

              -- update in tranbrkp table
							set v_sql = concat("
								update ",v_tranbrkp_table," as a
								inner join recon_tmp_t4tranroundoff as r on a.tranbrkp_gid = r.tranbrkp_gid
								inner join recon_tmp_t4kodtl as b on a.tranbrkp_gid = b.tranbrkp_gid
								set a.roundoff_value = r.roundoff_value
								where a.delete_flag = 'N'");

							call pr_run_sql(v_sql,@msg,@result);
            else
							set v_sql = concat("
								update ",v_tran_table," as a
								inner join recon_tmp_t4kodtlsumm as b on a.tran_gid = b.tran_gid
								set a.ko_gid = b.max_ko_gid,
										a.ko_date = curdate(),
										a.theme_code = ''
								where a.ko_gid = 0
								and a.delete_flag = 'N'");

							call pr_run_sql(v_sql,@msg,@result);

							set v_sql = concat("
								update ",v_tranbrkp_table," as a
								inner join recon_tmp_t4kodtl as b on a.tranbrkp_gid = b.tranbrkp_gid
								set a.ko_gid = b.ko_gid,
										a.ko_date = curdate(),
										a.theme_code = ''
								where a.ko_gid = 0
								and a.delete_flag = 'N'");

							call pr_run_sql(v_sql,@msg,@result);
            end if;

            -- move tran table
            truncate recon_tmp_t4trangid;

						set v_sql = concat("
							insert into recon_tmp_t4trangid
								select a.tran_gid from recon_tmp_t4kodtlsumm as a
								inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
									and b.excp_value = 0
									and b.mapped_value = 0
									and b.delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("
							insert into ",v_tranko_table,"
								select t.* from recon_tmp_t4trangid as g
								inner join ",v_tran_table," as t on g.tran_gid = t.tran_gid");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("delete a.* from ",v_tran_table," as a
							where a.tran_gid in (select b.tran_gid from recon_tmp_t4trangid as b where a.tran_gid = b.tran_gid)");

						call pr_run_sql(v_sql,@msg,@result);

            -- move tranbrkp table
            truncate recon_tmp_t4tranbrkpgid;

            insert into recon_tmp_t4tranbrkpgid (tranbrkp_gid) select tranbrkp_gid from recon_tmp_t4kodtl where tranbrkp_gid > 0;

						set v_sql = concat("
							insert into ",v_tranbrkpko_table,"
								select b.* from recon_tmp_t4tranbrkpgid as g
								inner join ",v_tranbrkp_table," as b on g.tranbrkp_gid = b.tranbrkp_gid");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("delete a.* from ",v_tranbrkp_table," as a
							where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_t4tranbrkpgid as b where a.tranbrkp_gid = b.tranbrkp_gid)");

						call pr_run_sql(v_sql,@msg,@result);

            -- move tran mapped_value > 0
            /*
            truncate recon_tmp_t4trangid;

						set v_sql = concat("
							insert into recon_tmp_t4trangid
								select distinct a.tran_gid from recon_tmp_t4tranbrkpgid as a
								left join ",v_tranbrkp_table," as b on a.tran_gid = b.tran_gid and b.delete_flag = 'N'
								where b.tran_gid is null");

						call pr_run_sql(v_sql,@msg,@result);
            */

            truncate recon_tmp_t4trangid1;
            truncate recon_tmp_t4trangid2;

            -- set tran_gid1
						set v_sql = concat("
							insert into recon_tmp_t4trangid1
								select distinct tran_gid from recon_tmp_t4tranbrkpgid");

						call pr_run_sql(v_sql,@msg,@result);

            -- set tran_gid2
						set v_sql = concat("
							insert into recon_tmp_t4trangid2
								select distinct tran_gid from ",v_tranbrkp_table,"
                where recon_code = '",in_recon_code,"'
                and tran_gid > 0
                and delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);

            -- move tran mapped_value > 0
						set v_sql = concat("
							insert into recon_tmp_t4trangid
								select distinct a.tran_gid from recon_tmp_t4trangid1 as a
								left join recon_tmp_t4trangid2 as b on a.tran_gid = b.tran_gid
								where b.tran_gid is null");

						call pr_run_sql(v_sql,@msg,@result);

            -- keep excp value non-zero cases
            truncate recon_tmp_t4gid;

						set v_sql = concat("
							insert into recon_tmp_t4gid(tran_gid) select tran_gid from ",v_tran_table," as a
								where a.tran_gid in (select b.tran_gid from recon_tmp_t4trangid as b where a.tran_gid = b.tran_gid)
								and a.excp_value <> 0
								and a.delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);

            delete a.* from recon_tmp_t4trangid as a
            where a.tran_gid in (select b.tran_gid from recon_tmp_t4gid as b where a.tran_gid = b.tran_gid);

						set v_sql = concat("
							insert into ",v_tranko_table,"
								select t.* from recon_tmp_t4trangid as g
								inner join ",v_tran_table," as t on g.tran_gid = t.tran_gid");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("delete a.* from ",v_tran_table," as a
							where a.tran_gid in (select b.tran_gid from recon_tmp_t4trangid as b where a.tran_gid = b.tran_gid)");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("
							update ",v_ko_table," set
								kodtl_post_flag = 'Y'
							where job_gid = ",cast(in_job_gid as nchar),"
							and kodtl_post_flag = 'N'
							and delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);
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
            from recon_tmp_t4match
            where dup_flag = 'N'
            and tranbrkp_gid = 0;

            insert into recon_trn_tpreviewdtl
            ( previewdtl_gid,preview_gid,job_gid,tran_gid,tranbrkp_gid,excp_value,tran_mult,reversal_flag,src_comp_flag)
            select
              recon_tmp_t4pseudorows.row+1,
              preview_gid,
              job_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_gid'))) AS tran_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].ko_value'))) AS excp_value,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_mult'))) AS tran_mult,
              v_reversal_flag,
              JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].src_comp_flag'))) AS src_comp_flag
            FROM recon_trn_tpreview
            JOIN recon_tmp_t4pseudorows
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
              from recon_tmp_t4match
              where 1 = 1
              and dup_flag = 'N'
              and tranbrkp_gid > 0;

							insert into recon_trn_tpreviewdtl
							( previewdtl_gid,preview_gid,job_gid,tran_gid,tranbrkp_gid,excp_value,tran_mult,reversal_flag,src_comp_flag)
							select
								recon_tmp_t4pseudorows.row+1,
								preview_gid,
								job_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_gid'))) AS tran_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].ko_value'))) AS excp_value,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].tran_mult'))) AS tran_mult,
								v_reversal_flag,
								JSON_UNQUOTE(JSON_EXTRACT(recon_trn_tpreview.previewdtl_json, CONCAT('$[', recon_tmp_t4pseudorows.row, '].src_comp_flag'))) AS src_comp_flag
							FROM recon_trn_tpreview
							JOIN recon_tmp_t4pseudorows
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
              where a.excp_value <> 0
              and a.delete_flag = 'N';

              update recon_tmp_ttranbrkp as a
							inner join recon_tmp_t4matchdtl as c on a.tranbrkp_gid = c.tranbrkp_gid and a.tran_gid = c.tran_gid
              inner join recon_trn_tpreviewdtl as b on c.tran_gid = b.tran_gid
                and b.job_gid = in_job_gid
                and b.preview_gid > v_preview_gid
              set a.excp_value = a.excp_value - c.ko_value
              where a.excp_value <> 0
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

          truncate recon_tmp_t4source;
          truncate recon_tmp_t4comparison;
          truncate recon_tmp_t4sourcedup;
          truncate recon_tmp_t4match;
          truncate recon_tmp_t4matchdtl;
          truncate recon_tmp_t4matchdtlgid;
          truncate recon_tmp_t4matchdup;
          truncate recon_tmp_t4matchparentgid;
          truncate recon_tmp_t4matchko;
          truncate recon_tmp_t4manymatch;
          truncate recon_tmp_t4kodtl;
          truncate recon_tmp_t4kodtlsumm;
          truncate recon_tmp_t4trangid;
          truncate recon_tmp_t4tranbrkpgid;
    end loop applyrule_loop;

    close applyrule_cursor;
  end applyrule_block;

  set out_result = v_count;

  if in_automatch_flag = 'Y' then
    set out_msg = 'Auto match ran successfully !';
  else
    set out_msg = 'Preview ran successfully !';
  end if;

  drop temporary table if exists recon_tmp_t4source;
  drop temporary table if exists recon_tmp_t4comparison;
  drop temporary table if exists recon_tmp_t4sourcedup;
  drop temporary table if exists recon_tmp_t4match;
  drop temporary table if exists recon_tmp_t4matchdtl;
  drop temporary table if exists recon_tmp_t4matchdtlgid;
  drop temporary table if exists recon_tmp_t4matchdup;
  drop temporary table if exists recon_tmp_t4matchparentgid;
  drop temporary table if exists recon_tmp_t4matchko;
  drop temporary table if exists recon_tmp_t4matchdiff;
  drop temporary table if exists recon_tmp_t4matchdiffdtl;
  drop temporary table if exists recon_tmp_t4manymatch;
  drop temporary table if exists recon_tmp_t4kodtl;
  drop temporary table if exists recon_tmp_t4kodtlsumm;
  drop temporary table if exists recon_tmp_t4pseudorows;
  drop temporary table if exists recon_tmp_t4trangid;
  drop temporary table if exists recon_tmp_t4trangid1;
  drop temporary table if exists recon_tmp_t4trangid2;
  drop temporary table if exists recon_tmp_t4tranbrkpgid;
  drop temporary table if exists recon_tmp_t4tranwithbrkpgid;
  drop temporary table if exists recon_tmp_t4index;
  drop temporary table if exists recon_tmp_t4sql;
  drop temporary table if exists recon_tmp_t4gid;
  drop temporary table if exists recon_tmp_t4tranroundoff;

  drop temporary table if exists recon_tmp_t4thresholddiff;
  drop temporary table if exists recon_tmp_t4thresholddiffdtl;
end $$

DELIMITER ;