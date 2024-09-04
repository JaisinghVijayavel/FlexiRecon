DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_automatch` $$
CREATE PROCEDURE `pr_run_automatch`(
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

  declare v_source_tranbrkp_code varchar(32) default '';
  declare v_comparison_tranbrkp_code varchar(32) default '';

  declare v_source_dataset_type varchar(32) default '';
  declare v_comparison_dataset_type varchar(32) default '';

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

  declare v_system_matchoff char(1) default null;
  declare v_manual_matchoff char(1) default null;

  declare v_filter_applied_on char(1) default '';
  declare v_filter_field text default '';
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

  declare v_recorder_source text default '';
  declare v_recorder_comparison text default '';
  declare v_recorder text default '';
	
	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';

  declare v_preview_gid int default 0;

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  /*
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    set @text = concat(@text,' ',err_msg);

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    ROLLBACK;

    select @text;

    call pr_upd_job(v_job_gid,'F',@full_error,@msg,@result);

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;
  */

	-- set tran table
  /*
	set v_tran_table = concat(in_recon_code,'_tran');
	set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');

	set v_tranko_table = concat(in_recon_code,'_tranko');
	set v_tranbrkpko_table = concat(in_recon_code,'_tranbrkpko');

	set v_ko_table = concat(in_recon_code,'_ko');
	set v_kodtl_table = concat(in_recon_code,'_kodtl');
  */

	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

	set v_tranko_table = 'recon_trn_ttranko';
	set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';

	set v_ko_table = 'recon_trn_tko';
	set v_kodtl_table = 'recon_trn_tkodtl';

  set v_group_flag = in_group_flag;

  if v_group_flag = 'MTM' then
    set v_group_desc = ' (many to many)';
  elseif v_group_flag = 'OTM' then
    set v_group_desc = ' (one to many)';
  elseif v_group_flag = 'OTO' then
    set v_group_desc = ' (one to one)';
  end if;

  if in_automatch_flag = 'Y' then
    set v_system_matchoff = 'Y';
  else
    set v_manual_matchoff = 'Y';
  end if;

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
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
  drop temporary table if exists recon_tmp_tmatchkotran;
  drop temporary table if exists recon_tmp_tmatchkotranbrkp;
  drop temporary table if exists recon_tmp_tmatchdiff;
  drop temporary table if exists recon_tmp_tmatchdiffdtl;
  drop temporary table if exists recon_tmp_tmanymatch;
  drop temporary table if exists recon_tmp_tkodtl;
  drop temporary table if exists recon_tmp_tkodtlsumm;
  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttrangid1;
  drop temporary table if exists recon_tmp_ttrangid2;
  drop temporary table if exists recon_tmp_ttranbrkpgid;
  drop temporary table if exists recon_tmp_ttranwithbrkpgid;

  drop temporary table if exists recon_tmp_tgid;
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

  /*
  drop table if exists recon_tmp_tmatch;
  drop table if exists recon_tmp_tmatchdtl;
  drop table if exists recon_tmp_tmatchko;
  drop table if exists recon_tmp_tmatchdiff;
  drop table if exists recon_tmp_tmatchdiffdtl;
  */

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
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid),
    key idx_group_flag(group_flag),
    key idx_dup_flag(dup_flag),
    key idx_ko_flag(ko_flag)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchdtl(
    matchdtl_gid int unsigned NOT NULL AUTO_INCREMENT,
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

  create temporary table recon_tmp_tmatchko(
    tran_gid int unsigned NOT NULL,
    ko_value decimal(15,2) not null default 0,
    excp_value decimal(15,2) not null default 0,
    ko_flag char(1) not null default 'N',
    ko_gid int unsigned not null default 0,
    ko_date date default null,
    PRIMARY KEY (tran_gid),
    key idx_ko_flag(ko_flag)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchdiff(
    tran_gid int unsigned NOT NULL,
    tran_mult tinyint not null default 0,
    tran_value decimal(15,2) not null default 0,
    excp_value decimal(15,2) not null default 0,
    mapped_value decimal(15,2) not null default 0,
    diff_value decimal(15,2) not null default 0,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchdiffdtl(
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

  create temporary table recon_tmp_tmatchkotran(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned NOT NULL default 0,
    parent_tran_gid int unsigned NOT NULL default 0,
    ko_value decimal(15,2) not null default 0,
    ko_gid int unsigned not null default 0,
    PRIMARY KEY (tran_gid),
    key idx_parent_tran_gid(parent_tran_gid),
    key idx_ko_gid(ko_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmatchkotranbrkp(
    tranbrkp_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL default 0,
    ko_value decimal(15,2) not null default 0,
    ko_gid int unsigned not null default 0,
    PRIMARY KEY (tranbrkp_gid),
    key idx_tran_gid(tran_gid),
    key idx_ko_gid(ko_gid)
  ) ENGINE = MyISAM;

  /*
  drop table if exists recon_tmp_tmanymatch;
  */

  CREATE temporary TABLE recon_tmp_tmanymatch(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    source_value double(15,2) not null default 0,
    comparison_value double(15,2) not null default 0,
    matched_count int not null default 0,
    tran_mult tinyint not null default 0,
    matched_txt_json json NOT NULL,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tkodtl(
    kotmpdtl_gid int unsigned NOT NULL AUTO_INCREMENT,
    ko_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    tran_mult tinyint not null default 0,
    ko_value decimal(15,2) not null default 0,
    PRIMARY KEY (kotmpdtl_gid),
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

  CREATE temporary TABLE recon_tmp_tpseudorows(
    row int unsigned NOT NULL,
    PRIMARY KEY (row)
  ) ENGINE = MyISAM;

  insert into recon_tmp_tpseudorows select 0 union select 1;

  CREATE temporary TABLE recon_tmp_tgid(
    gid int unsigned NOT NULL,
    PRIMARY KEY (gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttrangid(
    tran_gid int unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttrangid1(
    tran_gid int unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttrangid2(
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
    recon_name,recontype_code,recon_value_flag,recon_date_flag,recon_automatch_partial
  into
    v_recon_name,v_recontype_code,v_recon_value_flag,v_recon_date_flag,v_recon_automatch_partial
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
        a.reversal_flag,
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
                  v_reversal_flag,
                  v_group_method_flag,v_manytomany_match_flag;

      if applyrule_done = 1 then leave applyrule_loop; end if;

      -- update the job
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
        elseif v_source_acc_mode <> v_comparison_acc_mode then
          set v_group_method_flag = 'C';
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

      -- source head for tran table
      set v_source_head_sql = concat('insert into recon_tmp_tsource (',v_tran_fields,') ');

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
        set v_source_head_sql = concat(v_source_head_sql,' and excp_value <> 0 and mapped_value = 0 ');
      else
        set v_source_head_sql = concat(v_source_head_sql,' and ko_gid = 0 ');
      end if;

      -- comparison head for tran table
      set v_comparison_head_sql = concat('insert into recon_tmp_tcomparison (',v_tran_fields,') ');

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
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and excp_value <> 0 and mapped_value = 0 ');
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and ko_gid = 0 ');
      end if;

      -- source head for tranbrkp table
      set v_source_headbrkp_sql = concat('insert into recon_tmp_tsource (',v_tranbrkp_fields,') ');

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
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and ko_gid = 0 ');
      end if;

      -- comparison head for tranbrkp table
      set v_comparison_headbrkp_sql = concat('insert into recon_tmp_tcomparison (',v_tranbrkp_fields,') ');

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
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and ko_gid = 0 ');
      end if;

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

            set v_sourcebase_filter = ' and ';
            set v_comparisonbase_filter = ' and ';

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
              set v_ident_value_flag = ifnull(v_ident_value_flag,'Y');
              set v_ident_value = ifnull(v_ident_value,'');

              if v_join_condition = '' then
                set v_join_condition = 'and';
              end if;

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

          set v_sourcebase_filter = concat(v_sourcebase_filter,' 1 = 1 ');
          set v_comparisonbase_filter = concat(v_comparisonbase_filter,' 1 = 1 ');

          -- if v_sourcebase_filter = ' and ' then set v_sourcebase_filter = ''; end if;
          -- if v_comparisonbase_filter = ' and ' then set v_comparisonbase_filter = ''; end if;

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

              if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_tsource'
                            and index_name = v_index_name) then

                if substr(v_source_field,1,3) = 'col' then
                  set v_sql = concat('alter table recon_tmp_tsource modify column ',v_source_field,' varchar(255) default null');
                  call pr_run_sql(v_sql,@msg,@result);

                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tsource(',v_source_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tsource(',v_source_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_tsource',v_index_name;
              end if;

              set v_index_name = concat('idx_',v_comparison_field);

              if not exists(select index_name from recon_tmp_tindex
                            WHERE table_name = 'recon_tmp_tcomparison'
                            and index_name = v_index_name) then

                if substr(v_comparison_field,1,3) = 'col' then
                  set v_sql = concat('alter table recon_tmp_tcomparison modify column ',v_source_field,' varchar(255) default null');
                  call pr_run_sql(v_sql,@msg,@result);

                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tcomparison(',v_comparison_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tcomparison(',v_comparison_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tindex(table_name,index_name) select 'recon_tmp_tcomparison',v_index_name;
              end if;

              set v_source_field_org_type = fn_get_fieldorgtype(in_recon_code,v_source_field);
              set v_comparison_field_org_type = fn_get_fieldorgtype(in_recon_code,v_comparison_field);

              set v_extraction_criteria = ifnull(v_extraction_criteria,'');
              set v_extraction_filter = ifnull(v_extraction_filter,0);
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

                insert into recon_tmp_tsql(table_type,acc_mode,sql_query) values ('S',v_source_acc_mode,v_sql);

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

                if v_recontype_code <> 'N' then
                  set v_sql = concat(v_sql,'where tran_acc_mode =',char(39), v_comparison_acc_mode,char(39), ' ');
                end if;

                insert into recon_tmp_tsql(table_type,acc_mode,sql_query) values ('C',v_comparison_acc_mode,v_sql);

								if v_manytomany_match_flag = 'Y' and v_recontype_code <> 'N' and v_recontype_code <> 'V' then
									set v_sql = '';
									set v_sql = concat(v_sql,'update recon_tmp_tcomparison set ');
									set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');
                  set v_sql = concat(v_sql,'where tran_acc_mode =',char(39), v_source_acc_mode,char(39),' ');

									insert into recon_tmp_tsql(table_type,acc_mode,sql_query) values ('C',v_source_acc_mode,v_sql);
								end if;

                set v_comparison_criteria = 'EXACT';
                set v_comparison_filter = 0;
              end if;

              set v_source_field_format = fn_get_fieldfilterformat(v_source_field,v_extraction_criteria,v_extraction_filter);
              set v_build_condition = concat(v_open_parentheses_flag,
                                             fn_get_comparisoncondition(in_recon_code,v_source_field_format,v_comparison_field,v_comparison_criteria,v_comparison_filter),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition);

              set v_rule_condition = concat(v_rule_condition,' ',v_build_condition,' ');

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
            set v_rule_notnull_condition = ' and 1 =2 ';
            set v_rule_groupby = ',tran_gid';
          else
            set v_source_condition = concat(v_source_condition, ' 1 = 1 ');
            set v_comparison_condition  = concat(v_comparison_condition,' 1 = 1 ');
            set v_rule_condition  = concat(v_rule_condition,' 1 = 1 ');
          end if;

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

          call pr_run_sql(v_source_sql,@result,@msg);

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

          call pr_run_sql(v_source_sql,@result,@msg);

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

          call pr_run_sql(v_comparison_sql,@result,@msg);

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

          call pr_run_sql(v_comparison_sql,@result,@msg);

					if v_group_method_flag = 'B' and v_recontype_code <> 'N' and v_recontype_code <> 'V' then
            -- and v_source_dataset_code <> v_comparison_dataset_code

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

						call pr_run_sql(v_comparison_sql,@result,@msg);

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

						call pr_run_sql(v_comparison_sql,@result,@msg);
					end if;

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

          alter table recon_tmp_tcomparison ENGINE = MyISAM;
          alter table recon_tmp_tsource ENGINE = MyISAM;

					-- many to many match
					if v_manytomany_match_flag = 'Y' then
						set v_match_sql = 'insert ignore into recon_tmp_tmanymatch (tran_gid,tranbrkp_gid,matched_count,';
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

						set v_match_sql = concat(v_match_sql,'from recon_tmp_tsource as a ');
						set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tcomparison as b ');
						set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

						set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

						set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_rule_groupby,' ');

            -- add record order by
            if v_recorder <> '' then
              set v_match_sql = concat(v_match_sql,v_recorder);
            end if;

						call pr_run_sql(v_match_sql,@msg,@result);

						-- insert in match table
						set v_match_sql = 'insert into recon_tmp_tmatch (group_flag,tran_gid,tranbrkp_gid,matched_count,matched_value,tran_mult,matched_json) ';
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

						set v_match_sql = concat(v_match_sql,'from recon_tmp_tmanymatch ');

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

						call pr_run_sql(v_match_sql,@msg,@result);

            /*
						-- and v_recontype_code <> 'N' and v_recontype_code <> 'V'
						set v_match_sql = 'insert into recon_tmp_tmanymatch (tran_gid,tranbrkp_gid,matched_count,';
						set v_match_sql = concat(v_match_sql,'tran_mult,source_value,comparison_value,matched_txt_json) ');
						set v_match_sql = concat(v_match_sql,'select ');
						set v_match_sql = concat(v_match_sql,'a.tran_gid,a.tranbrkp_gid,count(*) as matched_count,a.tran_mult,');
						set v_match_sql = concat(v_match_sql,'a.excp_value as source_value,sum(b.excp_value*b.tran_mult) as comparison_value,');

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

						-- set v_match_sql = concat(v_match_sql,'where 1 = 1 ');
						set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

						set v_match_sql = concat(v_match_sql,'group by a.excp_value,a.tran_gid,a.tranbrkp_gid',v_rule_groupby,' ');
						-- set v_match_sql = concat(v_match_sql,'having true  '); -- count(*) > 1
						-- set v_match_sql = concat(v_match_sql,'and a.excp_value*a.tran_mult <> sum(b.excp_value*b.tran_mult)*-1 ');

            -- select v_match_sql;
            -- leave me;

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

              if v_recontype_code <> 'I' then
                -- contra
							  set v_match_sql = concat(v_match_sql,'having sum(source_value*tran_mult) = (comparison_value*-1) ');
              else
                -- mirror
							  set v_match_sql = concat(v_match_sql,'having sum(source_value*tran_mult) = comparison_value ');
              end if;
						else
							set v_match_sql = concat(v_match_sql,'group by matched_txt_json ');
						end if;

						-- set v_match_sql = concat(v_match_sql,'having count(*) > 1 ');
						-- set v_match_sql = concat(v_match_sql,'and sum(source_value*tran_mult) = comparison_value');

						call pr_run_sql(v_match_sql,@msg,@result);
            */

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

						delete a.* from recon_tmp_tsource as a
              where a.tran_gid in (select b.tran_gid from recon_tmp_ttrangid as b where a.tran_gid = b.tran_gid);

						delete a.* from recon_tmp_tcomparison as a
              where a.tran_gid in (select b.tran_gid from recon_tmp_ttrangid as b where a.tran_gid = b.tran_gid);

						truncate recon_tmp_ttranbrkpgid;

						insert into recon_tmp_ttranbrkpgid (tranbrkp_gid)
							select distinct tranbrkp_gid from recon_tmp_tmatchdtl where tranbrkp_gid > 0;

						delete a.* from recon_tmp_tsource as a
              where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_ttranbrkpgid as b
                                       where a.tranbrkp_gid = b.tranbrkp_gid);

						delete a.* from recon_tmp_tcomparison as a
              where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_ttranbrkpgid as b
                                       where a.tranbrkp_gid = b.tranbrkp_gid);

						truncate recon_tmp_ttrangid;
						truncate recon_tmp_ttranbrkpgid;
					end if;

					-- one to many match
					 if v_group_flag = 'Y' and v_manytomany_match_flag = 'N' then
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

            -- run match sql one to many
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

						delete a.* from recon_tmp_tsource as a
              where a.tran_gid in (select b.tran_gid from recon_tmp_ttrangid as b where a.tran_gid = b.tran_gid);

						delete a.* from recon_tmp_tcomparison as a
              where a.tran_gid in (select b.tran_gid from recon_tmp_ttrangid as b where a.tran_gid = b.tran_gid);

						truncate recon_tmp_ttranbrkpgid;

						insert into recon_tmp_ttranbrkpgid (tranbrkp_gid)
							select distinct tranbrkp_gid from recon_tmp_tmatchdtl where tranbrkp_gid > 0;

						delete a.* from recon_tmp_tsource as a
              where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_ttranbrkpgid as b where a.tranbrkp_gid = b.tranbrkp_gid);

						delete a.* from recon_tmp_tcomparison as a
              where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_ttranbrkpgid as b where a.tranbrkp_gid = b.tranbrkp_gid);

						truncate recon_tmp_ttrangid;
						truncate recon_tmp_ttranbrkpgid;
					 end if;

					-- one to one match
          if v_manytomany_match_flag = 'N' then
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

						set v_match_sql = concat(v_match_sql,v_rule_condition,' ');

						if v_recontype_code <> 'N' then
							set v_match_sql = concat(v_match_sql,'and a.excp_value = b.excp_value ');

						  set v_match_sql = concat(v_match_sql,'where a.tran_acc_mode = ',char(39),v_source_acc_mode,char(39),' ');
						  set v_match_sql = concat(v_match_sql,'and b.tran_acc_mode = ',char(39),v_comparison_acc_mode,char(39),' ');
						end if;

						set v_match_sql = concat(v_match_sql,'group by a.tran_gid,a.tranbrkp_gid ');
						set v_match_sql = concat(v_match_sql,'having count(*) = 1 ');

            -- add record order by
            if v_recorder <> '' then
              set v_match_sql = concat(v_match_sql,v_recorder);
            end if;

            -- run match sql one to one
						call pr_run_sql(v_match_sql,@msg,@result);

						truncate recon_tmp_tpseudorows;

						select max(matched_count) into v_count from recon_tmp_tmatch;
						set v_count = ifnull(v_count,0);

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
							where group_flag = 'N'
							HAVING tran_gid IS NOT NULL;
          end if;

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
              -- match diff
							set v_sql = concat("
								insert into recon_tmp_tmatchdiff(tran_gid,tran_value,excp_value,mapped_value,tran_mult,diff_value)
								select
									a.tran_gid,b.tran_value,b.excp_value,b.mapped_value,b.tran_mult,
									b.excp_value - sum(a.ko_value*a.tran_mult)*b.tran_mult
								from recon_tmp_tmatch as m
								inner join recon_tmp_tmatchdtl as a on m.tran_gid = a.parent_tran_gid and m.tranbrkp_gid = a.parent_tranbrkp_gid
								inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
									and b.excp_value > 0
									and b.delete_flag = 'N'
								where m.dup_flag = 'N'
								and m.ko_flag = 'Y'
								group by a.tran_gid,b.tran_value,b.excp_value,b.tran_mult
								having b.excp_value < sum(a.ko_value*a.tran_mult)*b.tran_mult");
							
							call pr_run_sql(v_sql,@msg,@result);

              if exists(select * from recon_tmp_tmatchdiff) then
								-- diff block
								diff_block:begin
									declare diff_done int default 0;
									declare diff_cursor cursor for
									select tran_gid,tran_mult,mapped_value,diff_value from recon_tmp_tmatchdiff;
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
                        select tranbrkp_gid,excp_value,tran_mult from recon_tmp_tsource
                        where tran_gid = v_tran_gid
                        and tranbrkp_gid > 0

                        union

                        select tranbrkp_gid,excp_value,tran_mult from recon_tmp_tcomparison
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
															from recon_tmp_tmatchdtl as a
															inner join recon_tmp_tmatch as m on m.tran_gid = a.parent_tran_gid and m.tranbrkp_gid = a.parent_tranbrkp_gid
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
												from recon_tmp_tmatchdtl as a
												inner join recon_tmp_tmatch as b on a.parent_tran_gid = b.tran_gid and a.parent_tranbrkp_gid = b.tranbrkp_gid
												where a.tran_gid = v_tran_gid
												and b.ko_flag = 'Y'
												group by a.parent_tran_gid,a.parent_tranbrkp_gid,b.matched_count,b.matched_value
												having sum(a.ko_value*a.tran_mult)*v_tran_mult > v_diff_value
												order by sum(a.ko_value*a.tran_mult)*v_tran_mult,count(*)
												limit 0,1;

												update recon_tmp_tmatch set
													ko_flag = 'D',
													dup_flag = 'D'
												where tran_gid = v_parent_tran_gid
												and tranbrkp_gid = v_parent_tranbrkp_gid;

												insert ignore into recon_tmp_tmatchdiffdtl
													select * from recon_tmp_tmatchdtl
													where parent_tran_gid = v_parent_tran_gid
													and parent_tranbrkp_gid = v_parent_tranbrkp_gid;
                      end if;
                    end if;

									end loop diff_loop;
									close diff_cursor;
								end diff_block;

                -- more than one record marked as duplicate
                if exists(select * from recon_tmp_tmatch
                  where ko_flag = 'D') then
                  truncate recon_tmp_ttrangid;

                  insert into recon_tmp_ttrangid(tran_gid)
                    select tran_gid from recon_tmp_tmatchdiffdtl
                    group by tran_gid
                    having count(*) > 1;

                  insert into recon_tmp_ttranwithbrkpgid(tran_gid,tranbrkp_gid,rec_count)
                    select a.parent_tran_gid,a.parent_tranbrkp_gid,count(*) from recon_tmp_tmatchdiffdtl as a
                    inner join recon_tmp_ttrangid as b on a.tran_gid = b.tran_gid
                    group by a.parent_tran_gid,a.parent_tranbrkp_gid;

                  update recon_tmp_tmatch as a
                  inner join recon_tmp_ttranwithbrkpgid as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
                  set
                    a.dup_flag = 'N',
                    a.ko_flag = 'Y'
                  where b.rec_count = 1
                  and a.ko_flag = 'D';
                end if;
              end if;

              update recon_tmp_tmatch as a
              inner join recon_tmp_tmatchdtl as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
                and b.tranbrkp_gid > 0
                and b.ko_flag = 'N'
              set b.ko_flag = 'Y'
              where a.ko_flag = 'Y' and a.dup_flag = 'N';

              -- knockoff validation
							set v_sql = concat("
								insert into recon_tmp_tmatchko (tran_gid,ko_value,excp_value)
								select
									a.tran_gid,sum(a.ko_value*a.tran_mult)*b.tran_mult,b.excp_value
								from recon_tmp_tmatch as m
								inner join recon_tmp_tmatchdtl as a on m.tran_gid = a.parent_tran_gid and m.tranbrkp_gid = a.parent_tranbrkp_gid
								inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
									and b.excp_value <> 0
									and b.mapped_value = 0
									and b.delete_flag = 'N'
								where m.dup_flag = 'N'
								and m.ko_flag = 'Y'
								group by a.tran_gid,b.tran_mult
								having b.excp_value >= sum(a.ko_value*a.tran_mult)*b.tran_mult");
							
							call pr_run_sql(v_sql,@msg,@result);

              update recon_tmp_tmatchdtl as a
              inner join recon_tmp_tmatchko as b on a.tran_gid = b.tran_gid
              set a.ko_flag = 'Y';

              update recon_tmp_tmatch as a
              inner join recon_tmp_tmatchdtl as b on a.tran_gid = b.parent_tran_gid and a.tranbrkp_gid = b.parent_tranbrkp_gid
                and b.ko_flag = 'N'
              set a.ko_flag = 'N'
              where a.dup_flag = 'N';
            end if;

						set v_sql = concat("
							insert into ",v_ko_table,"
							(
								job_gid,ko_date,ko_value,recon_code,rule_code,
								reversal_flag,manual_matchoff,kodtl_json,kodtl_post_flag,insert_date,insert_by
							)
							select
								",cast(in_job_gid as nchar),",curdate(),matched_value,
								'",in_recon_code,"',
								'",v_rule_code,"',
								'",v_reversal_flag,"',
								'N',matched_json,'N',sysdate(),'",in_user_code,"'
							from recon_tmp_tmatch
							where ko_flag = 'Y'");
							
						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("
							insert into recon_tmp_tkodtl
							( ko_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult)
							select
								ko_gid,
								JSON_UNQUOTE(JSON_EXTRACT(",v_ko_table,".kodtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_gid'))) AS tran_gid,
								JSON_UNQUOTE(JSON_EXTRACT(",v_ko_table,".kodtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tranbrkp_gid'))) AS tranbrkp_gid,
								JSON_UNQUOTE(JSON_EXTRACT(",v_ko_table,".kodtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].ko_value'))) AS excp_value,
								JSON_UNQUOTE(JSON_EXTRACT(",v_ko_table,".kodtl_json, CONCAT('$[', recon_tmp_tpseudorows.row, '].tran_mult'))) AS tran_mult
							FROM ",v_ko_table,"
							JOIN recon_tmp_tpseudorows
							where job_gid = ",cast(in_job_gid as nchar),"
							and kodtl_post_flag = 'N'
							HAVING tran_gid IS NOT NULL
							order by ko_gid");
							
						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("
            insert into ",v_kodtl_table," (ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult)
              select ko_gid,tran_gid,tranbrkp_gid,ko_value,tran_mult from recon_tmp_tkodtl");
							
						call pr_run_sql(v_sql,@msg,@result);

            insert into recon_tmp_tkodtlsumm (max_ko_gid,tran_gid,ko_value,rec_count)
              select max(ko_gid) as max_ko_gid,tran_gid,sum(ko_value*tran_mult) as ko_value,count(*) as rec_count from recon_tmp_tkodtl
              group by tran_gid;

            if v_recontype_code <> 'N' then
							set v_sql = concat("
								update ",v_tran_table," as a
								inner join recon_tmp_tkodtlsumm as b on a.tran_gid = b.tran_gid
								set a.excp_value = a.excp_value - (b.ko_value * a.tran_mult),
										a.ko_gid = b.max_ko_gid,
										a.ko_date = curdate(),
										a.theme_code = ''
								where ((a.excp_value <> 0 and a.mapped_value = 0) or a.mapped_value > 0)
								and a.delete_flag = 'N'");
							
							call pr_run_sql(v_sql,@msg,@result);

							set v_sql = concat("
								update ",v_tranbrkp_table," as a
								inner join recon_tmp_tkodtl as b on a.tranbrkp_gid = b.tranbrkp_gid
								set a.excp_value = a.excp_value - b.ko_value,
										a.ko_gid = b.ko_gid,
										a.ko_date = curdate(),
										a.theme_code = ''
								where a.excp_value <> 0
								and a.delete_flag = 'N'");
								
							call pr_run_sql(v_sql,@msg,@result);
            else
							set v_sql = concat("
								update ",v_tran_table," as a
								inner join recon_tmp_tkodtlsumm as b on a.tran_gid = b.tran_gid
								set a.ko_gid = b.max_ko_gid,
										a.ko_date = curdate(),
										a.theme_code = ''
								where a.ko_gid = 0
								and a.delete_flag = 'N'");
								
							call pr_run_sql(v_sql,@msg,@result);

							set v_sql = concat("
              update ",v_tranbrkp_table," as a
              inner join recon_tmp_tkodtl as b on a.tranbrkp_gid = b.tranbrkp_gid
              set a.ko_gid = b.ko_gid,
                  a.ko_date = curdate(),
                  a.theme_code = ''
              where a.ko_gid = 0
              and a.delete_flag = 'N'");
							
							call pr_run_sql(v_sql,@msg,@result);
            end if;

            -- move tran mapped_value = 0
            truncate recon_tmp_ttrangid;

						set v_sql = concat("
							insert into recon_tmp_ttrangid
              select a.tran_gid from recon_tmp_tkodtlsumm as a
              inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
                and b.excp_value = 0
                and b.mapped_value = 0
                and b.delete_flag = 'N'");
						
						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("
							insert into ",v_tranko_table,"
								select t.* from recon_tmp_ttrangid as g
								inner join ",v_tran_table," as t on g.tran_gid = t.tran_gid");
								
						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("delete a.* from ",v_tran_table," as a
							where a.tran_gid in (select b.tran_gid from recon_tmp_ttrangid as b where a.tran_gid = b.tran_gid)");

						call pr_run_sql(v_sql,@msg,@result);

            -- move tranbrkp
            truncate recon_tmp_ttranbrkpgid;

            insert into recon_tmp_ttranbrkpgid (tranbrkp_gid,tran_gid) select tranbrkp_gid,tran_gid from recon_tmp_tkodtl where tranbrkp_gid > 0;

						set v_sql = concat("
            insert into ",v_tranbrkpko_table,"
              select b.* from recon_tmp_ttranbrkpgid as g
              inner join ",v_tranbrkp_table," as b on g.tranbrkp_gid = b.tranbrkp_gid");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("delete a.* from ",v_tranbrkp_table," as a
							where a.tranbrkp_gid in (select b.tranbrkp_gid from recon_tmp_ttranbrkpgid as b where a.tranbrkp_gid = b.tranbrkp_gid)");

						call pr_run_sql(v_sql,@msg,@result);

            -- move tran mapped_value > 0

            /*
            truncate recon_tmp_ttrangid;

						set v_sql = concat("
							insert into recon_tmp_ttrangid
								select distinct a.tran_gid from recon_tmp_ttranbrkpgid as a
								left join ",v_tranbrkp_table," as b on a.tran_gid = b.tran_gid and b.delete_flag = 'N'
								where b.tran_gid is null");

						call pr_run_sql(v_sql,@msg,@result);
            */

            truncate recon_tmp_ttrangid1;
            truncate recon_tmp_ttrangid2;

            -- set tran_gid1
						set v_sql = concat("
							insert into recon_tmp_ttrangid1
								select distinct tran_gid from recon_tmp_ttranbrkpgid");

						call pr_run_sql(v_sql,@msg,@result);

            -- set tran_gid2
						set v_sql = concat("
							insert into recon_tmp_ttrangid2
								select distinct tran_gid from ",v_tranbrkp_table,"
                where recon_code = '",in_recon_code,"'
                and tran_gid > 0
                and delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);

            -- move tran mapped_value > 0
						set v_sql = concat("
							insert into recon_tmp_ttrangid
								select distinct a.tran_gid from recon_tmp_ttrangid1 as a
								left join recon_tmp_ttrangid2 as b on a.tran_gid = b.tran_gid
								where b.tran_gid is null");

						call pr_run_sql(v_sql,@msg,@result);


            -- keep excp value non-zero cases
            truncate recon_tmp_tgid;

						set v_sql = concat("
							insert into recon_tmp_tgid select tran_gid from ",v_tran_table," as a
								where a.tran_gid in (select b.tran_gid from recon_tmp_ttrangid as b
                  where a.tran_gid = b.tran_gid)
								  and excp_value <> 0
								  and delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);

            delete a.* from recon_tmp_ttrangid as a
              where a.tran_gid in (select b.gid from recon_tmp_tgid as b where a.tran_gid = b.gid);

						set v_sql = concat("
							insert into ",v_tranko_table,"
								select t.* from recon_tmp_ttrangid as g
								inner join ",v_tran_table," as t on g.tran_gid = t.tran_gid");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("delete a.* from ",v_tran_table," as a
							where a.tran_gid in (select b.tran_gid from recon_tmp_ttrangid as b where a.tran_gid = b.tran_gid)");

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
              where a.excp_value <> 0
              and a.delete_flag = 'N';

              update recon_tmp_ttranbrkp as a
							inner join recon_tmp_tmatchdtl as c on a.tranbrkp_gid = c.tranbrkp_gid and a.tran_gid = c.tran_gid
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
  drop temporary table if exists recon_tmp_tmatchkotran;
  drop temporary table if exists recon_tmp_tmatchkotranbrkp;
  drop temporary table if exists recon_tmp_tmatchdiff;
  drop temporary table if exists recon_tmp_tmatchdiffdtl;
  drop temporary table if exists recon_tmp_tmanymatch;
  drop temporary table if exists recon_tmp_tkodtl;
  drop temporary table if exists recon_tmp_tkodtlsumm;
  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttrangid1;
  drop temporary table if exists recon_tmp_ttrangid2;
  drop temporary table if exists recon_tmp_ttranbrkpgid;
  drop temporary table if exists recon_tmp_ttranwithbrkpgid;

  drop temporary table if exists recon_tmp_tgid;
  drop temporary table if exists recon_tmp_tindex;
  drop temporary table if exists recon_tmp_tsql;
end $$

DELIMITER ;