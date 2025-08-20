DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_preprocess_comparison` $$
CREATE PROCEDURE `pr_run_preprocess_comparison`
(
  in in_recon_code text,
  in in_preprocess_code text,
  in in_job_gid int,
  in in_postprocess_flag text,
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

    Version : 1
  */

  declare v_recon_version text default '';
  declare v_recontype_code varchar(32) default '';

  declare v_source_head_sql text default '';
  declare v_comparison_head_sql text default '';

  declare v_source_headbrkp_sql text default '';
  declare v_comparison_headbrkp_sql text default '';

  declare v_preprocess_desc text default '';

  declare v_sql text default '';
  declare v_tmp_sql text default '';
  declare v_source_sql text default '';
  declare v_comparison_sql text default '';
  declare v_match_sql text default '';
  declare v_trangid_sql text default '';
  declare v_index_sql text default '';

  declare v_preprocess_code text default '';

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

  declare v_preprocessfilter_condition text default '';
  declare v_sourcebase_filter text default '';
  declare v_comparisonbase_filter text default '';
  declare v_comparison_filter text default '';

  declare v_preprocess_condition text default '';
  declare v_preprocess_notnull_condition text default '';

  declare v_fieldfilter_format text default '';
  declare v_comparisonfilter_format text default '';
  declare v_preprocess_groupby text default '';

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

  declare v_database_name text default '';
  declare v_table_name text default '';
  declare v_index_name text default '';
  declare v_sys_index_name text default '';

  declare v_recon_name text default '';
  declare v_recon_value_flag text default '';
  declare v_recon_date_flag text default '';
  declare v_field_type text default '';

  declare v_process_function text default '';
  declare v_set_recon_field text default '';

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

  drop temporary table if exists recon_tmp_tppmatch;
  drop temporary table if exists recon_tmp_tppmatchdtl;
  drop temporary table if exists recon_tmp_tppmatchdtlgid;
  drop temporary table if exists recon_tmp_tppmatchdup;
  drop temporary table if exists recon_tmp_tppmatchparentgid;
  drop temporary table if exists recon_tmp_tppmatchko;
  drop temporary table if exists recon_tmp_tppmatchkotran;
  drop temporary table if exists recon_tmp_tppmatchkotranbrkp;
  drop temporary table if exists recon_tmp_tppmatchdiff;
  drop temporary table if exists recon_tmp_tppmatchdiffdtl;
  drop temporary table if exists recon_tmp_tppmanymatch;
  drop temporary table if exists recon_tmp_tpppseudorows;
  drop temporary table if exists recon_tmp_tpptrangid;
  drop temporary table if exists recon_tmp_tpptranbrkpgid;
  drop temporary table if exists recon_tmp_tpptranwithbrkpgid;

  drop temporary table if exists recon_tmp_tppindex;
  drop temporary table if exists recon_tmp_tppsql;

  CREATE TEMPORARY TABLE recon_tmp_tppindex(
    table_name varchar(128) not null,
    index_name varchar(128) not null,
    sys_flag char(1) not null default 'N',
    PRIMARY KEY (table_name,index_name),
    key idx_sys_flag(sys_flag)
  ) ENGINE = MyISAM;

  insert into recon_tmp_tppindex select 'recon_tmp_tppsource','idx_tran_date','Y';
  insert into recon_tmp_tppindex select 'recon_tmp_tppsource','idx_excp_value','Y';
  insert into recon_tmp_tppindex select 'recon_tmp_tppsource','idx_recon_code','Y';
  insert into recon_tmp_tppindex select 'recon_tmp_tppsource','idx_dataset_date','Y';

  insert into recon_tmp_tppindex select 'recon_tmp_tppcomparison','idx_tran_date','Y';
  insert into recon_tmp_tppindex select 'recon_tmp_tppcomparison','idx_excp_value','Y';
  insert into recon_tmp_tppindex select 'recon_tmp_tppcomparison','idx_recon_code','Y';
  insert into recon_tmp_tppindex select 'recon_tmp_tppcomparison','idx_dataset_date','Y';

  /*
  drop table if exists recon_tmp_tppmatch;
  drop table if exists recon_tmp_tppmatchdtl;
  drop table if exists recon_tmp_tppmatchko;
  drop table if exists recon_tmp_tppmatchdiff;
  drop table if exists recon_tmp_tppmatchdiffdtl;
  */

  CREATE temporary TABLE recon_tmp_tppmatch(
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

  create temporary table recon_tmp_tppmatchdtl(
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

  create temporary table recon_tmp_tppmatchko(
    tran_gid int unsigned NOT NULL,
    ko_value decimal(15,2) not null default 0,
    excp_value decimal(15,2) not null default 0,
    ko_flag char(1) not null default 'N',
    ko_gid int unsigned not null default 0,
    ko_date date default null,
    PRIMARY KEY (tran_gid),
    key idx_ko_flag(ko_flag)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tppmatchdiff(
    tran_gid int unsigned NOT NULL,
    tran_mult tinyint not null default 0,
    tran_value decimal(15,2) not null default 0,
    excp_value decimal(15,2) not null default 0,
    mapped_value decimal(15,2) not null default 0,
    diff_value decimal(15,2) not null default 0,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tppmatchdiffdtl(
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

  create temporary table recon_tmp_tppmatchkotran(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned NOT NULL default 0,
    parent_tran_gid int unsigned NOT NULL default 0,
    ko_value decimal(15,2) not null default 0,
    ko_gid int unsigned not null default 0,
    PRIMARY KEY (tran_gid),
    key idx_parent_tran_gid(parent_tran_gid),
    key idx_ko_gid(ko_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tppmatchkotranbrkp(
    tranbrkp_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL default 0,
    ko_value decimal(15,2) not null default 0,
    ko_gid int unsigned not null default 0,
    PRIMARY KEY (tranbrkp_gid),
    key idx_tran_gid(tran_gid),
    key idx_ko_gid(ko_gid)
  ) ENGINE = MyISAM;

  /*
  drop table if exists recon_tmp_tppmanymatch;
  */

  CREATE temporary TABLE recon_tmp_tppmanymatch(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    source_value double(15,2) not null default 0,
    comparison_value double(15,2) not null default 0,
    matched_count int not null default 0,
    tran_mult tinyint not null default 0,
    matched_txt_json json NOT NULL,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tppmatchdtlgid(
    matchdtl_gid int unsigned NOT NULL,
    PRIMARY KEY (matchdtl_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tppmatchdup(
    matchdup_gid int unsigned NOT NULL AUTO_INCREMENT,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    rec_count int unsigned not null default 0,
    PRIMARY KEY (matchdup_gid),
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tppmatchparentgid(
    parent_tran_gid int unsigned NOT NULL,
    parent_tranbrkp_gid int unsigned not null default 0,
    rec_count int unsigned not null default 0,
    PRIMARY KEY (parent_tran_gid,parent_tranbrkp_gid),
    key idx_parent_tran_gid(parent_tran_gid),
    key idx_parent_tranbrkp_gid(parent_tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tpppseudorows(
    row int unsigned NOT NULL,
    PRIMARY KEY (row)
  ) ENGINE = MyISAM;

  insert into recon_tmp_tpppseudorows select 0 union select 1;

  CREATE temporary TABLE recon_tmp_tpptrangid(
    tran_gid int unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tpptranbrkpgid(
    tranbrkp_gid int unsigned NOT NULL,
    excp_value double(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    tran_gid int not null default 0,
    PRIMARY KEY (tranbrkp_gid),
    key idx_tran_gid(tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tpptranwithbrkpgid(
    tran_gid int unsigned not null,
    tranbrkp_gid int unsigned NOT NULL,
    rec_count int not null default 0,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;


  CREATE temporary TABLE recon_tmp_tppsql(
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
    recon_name,recon_rule_version,recontype_code
  into
    v_recon_name,v_recon_version,v_recontype_code
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

  preprocess_block:begin
    declare preprocess_done int default 0;
    declare preprocess_cursor cursor for
      select
		    a.preprocess_code,
        a.preprocess_desc,
        a.source_dataset_code,
        a.comparison_dataset_code,
        a.process_function,
        a.set_recon_field
      from recon_mst_tpreprocesshistory as a
      where a.recon_code = in_recon_code
      and a.preprocess_code = in_preprocess_code
      and a.recon_version = v_recon_version
      and a.process_method = 'QCD_COMPARISONEXP'
      and a.postprocess_flag = in_postprocess_flag
      and a.hold_flag = 'N'
      and a.active_status = 'Y'
      and a.delete_flag = 'N'
      order by a.preprocess_order;
    declare continue handler for not found set preprocess_done=1;

    open preprocess_cursor;

    preprocess_loop: loop
      fetch preprocess_cursor into v_preprocess_code,
                              v_preprocess_desc,
                              v_source_dataset_code,
                              v_comparison_dataset_code,
                              v_process_function,
                              v_set_recon_field;

      if preprocess_done = 1 then leave preprocess_loop; end if;

      set v_preprocess_code = ifnull(v_preprocess_code,'');
      set v_preprocess_desc = ifnull(v_preprocess_desc,'');

      set v_source_dataset_code = ifnull(v_source_dataset_code,'');
      set v_comparison_dataset_code = ifnull(v_comparison_dataset_code,'');

      set v_process_function = ifnull(v_process_function,'');
      set v_set_recon_field = ifnull(v_set_recon_field,'');

      set v_process_function = replace(v_process_function,'$FIELD$',v_set_recon_field);

      set v_group_flag = 'Y';
      set v_manytomany_match_flag = 'Y';

      if in_postprocess_flag = 'Y' then
        call pr_upd_job(in_job_gid,'P',concat('Applying Postprocess - ',v_preprocess_desc),@msg,@result);
      else
        call pr_upd_job(in_job_gid,'P',concat('Applying Preprocess - ',v_preprocess_desc),@msg,@result);
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

      if v_recontype_code = 'B'
        or v_recontype_code = 'W'
        or v_recontype_code = 'I' then
        set v_group_method_flag = 'C';
      end if;

      -- source from tran table
      set v_source_head_sql = concat('insert into recon_tmp_tppsource (',v_tran_fields,') ');

      set v_source_head_sql = concat(v_source_head_sql,' select ',v_tran_fields ,' from ',v_tran_table,' ');

      set v_source_head_sql = concat(v_source_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_source_head_sql = concat(v_source_head_sql,' and excp_value <> 0 and mapped_value = 0 ');
      else
        set v_source_head_sql = concat(v_source_head_sql,' and ko_gid = 0 ');
      end if;

      set v_comparison_head_sql = concat('insert into recon_tmp_tppcomparison (',v_tran_fields,') ');

      set v_comparison_head_sql = concat(v_comparison_head_sql,' select ',v_tran_fields ,' from ',v_tran_table,' ');

      set v_comparison_head_sql = concat(v_comparison_head_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and excp_value <> 0 and mapped_value = 0 ');
      else
        set v_comparison_head_sql = concat(v_comparison_head_sql,' and ko_gid = 0 ');
      end if;

      -- source from tranbrkp table
      set v_source_headbrkp_sql = concat('insert into recon_tmp_tppsource (',v_tranbrkp_fields,') ');

      set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' select ',v_tranbrkp_fields ,' from ',v_tranbrkp_table,' ');

      set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and excp_value <> 0 and tran_gid > 0 ');
      else
        set v_source_headbrkp_sql = concat(v_source_headbrkp_sql,' and ko_gid = 0 ');
      end if;

      -- comparison from tranbrkp table
      set v_comparison_headbrkp_sql = concat('insert into recon_tmp_tppcomparison (',v_tranbrkp_fields,') ');

      set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' select ',v_tranbrkp_fields ,' from ',v_tranbrkp_table,' ');

      set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' where recon_code = ',char(39),in_recon_code,char(39)) ;

      if v_recontype_code <> 'N' then
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and excp_value <> 0 and tran_gid > 0 ');
      else
        set v_comparison_headbrkp_sql = concat(v_comparison_headbrkp_sql,' and ko_gid = 0 ');
      end if;

          preprocessfilter_block:begin
            declare preprocessfilter_done int default 0;
            declare preprocessfilter_cursor cursor for
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
            order by filter_applied_on,filter_seqno,preprocessfilter_gid;

            declare continue handler for not found set preprocessfilter_done=1;

            open preprocessfilter_cursor;

            set v_sourcebase_filter = ' and (';
            set v_comparisonbase_filter = ' and (';

            preprocessfilter_loop: loop
              fetch preprocessfilter_cursor into v_filter_applied_on,
                                            v_filter_field,
                                            v_filter_criteria,
                                            v_filter_value_flag,
                                            v_filter_value,
                                            v_open_parentheses_flag,
                                            v_close_parentheses_flag,
                                            v_join_condition;

              if preprocessfilter_done = 1 then leave preprocessfilter_loop; end if;

              set v_filter_field = ifnull(v_filter_field,'');
              set v_filter_applied_on = substr(ifnull(v_filter_applied_on,''),1,1);
              set v_filter_value_flag = ifnull(v_filter_value_flag,'Y');
              set v_filter_value = ifnull(v_filter_value,'');

              if v_filter_value_flag = 'Y' then
                set v_filter_value = fn_get_filtervalue(in_recon_code,v_filter_value,'');
              end if;

              set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
              set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
              set v_join_condition = ifnull(v_join_condition,'');

              set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
              set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

              if v_join_condition = '' then
                set v_join_condition = 'and';
              end if;

              if v_filter_field = '' then
                set v_filter_value_flag = '';
                set v_filter_value = '';
              end if;

              set v_preprocessfilter_condition = concat(v_open_parentheses_flag,
                                                  fn_get_basefilterreconformat(in_recon_code,v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value_flag,v_filter_value),
                                                  v_close_parentheses_flag,' ',
                                                  v_join_condition,' ');

              if v_filter_applied_on = 'S' then
                set v_sourcebase_filter = concat(v_sourcebase_filter,v_preprocessfilter_condition);
              elseif v_filter_applied_on = 'C' then
                set v_comparisonbase_filter = concat(v_comparisonbase_filter,v_preprocessfilter_condition);
              end if;
            end loop preprocessfilter_loop;

            close preprocessfilter_cursor;
          end preprocessfilter_block;

          set v_sourcebase_filter = concat(v_sourcebase_filter,' 1 = 1) ');
          set v_comparisonbase_filter = concat(v_comparisonbase_filter,' 1 = 1) ');

          set v_preprocess_condition = ' and ';
          set v_preprocess_notnull_condition = ' and ';
          set v_preprocess_groupby = '';

          set v_source_condition = ' and ';
          set v_comparison_condition = ' and ';

          drop temporary table if exists recon_tmp_tppsource;
          drop temporary table if exists recon_tmp_tppcomparison;
          drop temporary table if exists recon_tmp_tppsourcedup;

          /*
          drop table if exists recon_tmp_tppsource;
          drop table if exists recon_tmp_tppcomparison;
          */

          create temporary table recon_tmp_tppsource select * from recon_trn_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tppsource ENGINE = MyISAM;
          alter table recon_tmp_tppsource add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tppsource(excp_value);
          create index idx_tran_date on recon_tmp_tppsource(tran_date);
          create index idx_recon_code on recon_tmp_tppsource(recon_code);
          create index idx_dataset_code on recon_tmp_tppsource(recon_code,dataset_code);

          create temporary table recon_tmp_tppcomparison select * from recon_trn_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tppcomparison ENGINE = MyISAM;
          alter table recon_tmp_tppcomparison add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tppcomparison(excp_value);
          create index idx_tran_date on recon_tmp_tppcomparison(tran_date);
          create index idx_recon_code on recon_tmp_tppcomparison(recon_code);
          create index idx_dataset_cdoe on recon_tmp_tppcomparison(recon_code,dataset_code);

          create temporary table recon_tmp_tppsourcedup select * from recon_trn_ttranwithbrkp where 1 = 2;
          alter table recon_tmp_tppsourcedup add primary key(tran_gid,tranbrkp_gid);
          create index idx_excp_value on recon_tmp_tppsourcedup(excp_value);
          create index idx_tran_date on recon_tmp_tppsourcedup(tran_date);
          create index idx_dataset_code on recon_tmp_tppsourcedup(recon_code,dataset_code);
          alter table recon_tmp_tppsourcedup ENGINE = MyISAM;

          delete from recon_tmp_tppindex where sys_flag <> 'Y';
          truncate recon_tmp_tppsql;

          condition_block:begin
            declare condition_done int default 0;
            declare condition_cursor cursor for
            select
              a.source_field,a.extraction_criteria,
              a.comparison_field,a.comparison_criteria,
              a.open_parentheses_flag,a.close_parentheses_flag,
              a.join_condition
            from recon_mst_tpreprocessconditionhistory as a
            where a.preprocess_code = v_preprocess_code
            and a.recon_version = v_recon_version
            and a.active_status = 'Y'
            and a.delete_flag = 'N'
            order by condition_seqno;

            declare continue handler for not found set condition_done=1;

            open condition_cursor;

            condition_loop: loop
              fetch condition_cursor into v_source_field,v_extraction_criteria,
                                     v_comparison_field,v_comparison_criteria,
                                     v_open_parentheses_flag,v_close_parentheses_flag,v_join_condition;
              if condition_done = 1 then leave condition_loop; end if;

              set v_index_name = concat('idx_',v_source_field);

              if not exists(select index_name from recon_tmp_tppindex
                            WHERE table_name = 'recon_tmp_tppsource'
                            and index_name = v_index_name) then

                if substr(v_source_field,1,3) = 'col' then
                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tppsource(',v_source_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_source_field,' on recon_tmp_tppsource(',v_source_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tppindex(table_name,index_name) select 'recon_tmp_tppsource',v_index_name;
              end if;

              set v_index_name = concat('idx_',v_comparison_field);

              if not exists(select index_name from recon_tmp_tppindex
                            WHERE table_name = 'recon_tmp_tppcomparison'
                            and index_name = v_index_name) then

                if substr(v_comparison_field,1,3) = 'col' then
                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tppcomparison(',v_comparison_field,'(255))');
                else
                  set v_index_sql = concat('create index idx_',v_comparison_field,' on recon_tmp_tppcomparison(',v_comparison_field,')');
                end if;

                call pr_run_sql(v_index_sql,@msg,@result);

                insert into recon_tmp_tppindex(table_name,index_name) select 'recon_tmp_tppcomparison',v_index_name;
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
                set v_sql = concat(v_sql,'update recon_tmp_tppsource set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format);

                insert into recon_tmp_tppsql(table_type,sql_query) values ('S',v_sql);

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
                set v_sql = concat(v_sql,'update recon_tmp_tppcomparison set ');
                set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');

                if v_recontype_code <> 'N' then
                  set v_sql = concat(v_sql,'where true ');
                end if;

                insert into recon_tmp_tppsql(table_type,sql_query) values ('C',v_sql);

								if v_manytomany_match_flag = 'Y' and v_recontype_code <> 'N' and v_recontype_code <> 'V' then
									set v_sql = '';
									set v_sql = concat(v_sql,'update recon_tmp_tppcomparison set ');
									set v_sql = concat(v_sql,v_field,'=',v_field_format,' ');
                  set v_sql = concat(v_sql,'where true ');

									insert into recon_tmp_tppsql(table_type,sql_query) values ('C',v_sql);
								end if;

                set v_comparison_criteria = 'EXACT';
                set v_comparison_filter = 0;
              end if;

              set v_source_field_format = fn_get_fieldfilterformat(v_source_field,v_extraction_criteria,0);
              set v_build_condition = concat(v_open_parentheses_flag,
                                             fn_get_comparisoncondition(in_recon_code,v_source_field_format,v_comparison_field,v_comparison_criteria,0),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition);

              set v_preprocess_condition = concat(v_preprocess_condition,' ',v_build_condition,' ');

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

              set v_preprocess_notnull_condition = concat(v_preprocess_notnull_condition,v_build_condition);

              set v_preprocess_groupby = concat(v_preprocess_groupby,',',v_source_field);
            end loop condition_loop;

            close condition_cursor;
          end condition_block;

          truncate recon_tmp_tppsource;
          truncate recon_tmp_tppcomparison;

          if v_source_condition = ' and ' or v_comparison_condition = ' and ' then
            set v_source_condition = ' and 1 = 2 ';
            set v_comparison_condition  = ' and 1 = 2 ';
            set v_preprocess_condition = ' and 1 = 2 ';
            set v_preprocess_notnull_condition = ' and 1 =2 ';
            set v_preprocess_groupby = ',tran_gid';
          else
            set v_source_condition = concat(v_source_condition, ' 1 = 1 ');
            set v_comparison_condition  = concat(v_comparison_condition,' 1 = 1 ');
            set v_preprocess_condition  = concat(v_preprocess_condition,' 1 = 1 ');
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
            select sql_query from recon_tmp_tppsql;
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
          truncate recon_tmp_tpppseudorows;
          insert into recon_tmp_tpppseudorows select 0 union select 1;

          -- get target addtional group field
          /*
          if v_group_flag = 'Y' then
            select group_concat(concat('b.',grp_field)) into v_grp_field from recon_mst_tthemegrpfield
            where preprocess_code = v_preprocess_code
            and active_status = 'Y'
            and delete_flag = 'N';

            set v_grp_field = ifnull(v_grp_field,'');

            if v_grp_field <> '' then
              if v_preprocess_groupby <> '' then
                set v_preprocess_groupby = concat(v_preprocess_groupby,',',v_grp_field);
              else
                set v_preprocess_groupby = v_grp_field;
              end if;
            end if;
					end if;
          */

          alter table recon_tmp_tppcomparison ENGINE = MyISAM;
          alter table recon_tmp_tppsource ENGINE = MyISAM;

          truncate recon_tmp_tpptranwithbrkpgid;

          -- insert source gid
					set v_match_sql = 'insert ignore into recon_tmp_tpptranwithbrkpgid (tran_gid,tranbrkp_gid)';
					set v_match_sql = concat(v_match_sql,' ');
					set v_match_sql = concat(v_match_sql,'select ');
					set v_match_sql = concat(v_match_sql,'distinct a.tran_gid,a.tranbrkp_gid ');

					set v_match_sql = concat(v_match_sql,'from recon_tmp_tppsource as a ');
					set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tppcomparison as b ');
					set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

					set v_match_sql = concat(v_match_sql,v_preprocess_condition,' ');

          call pr_run_sql(v_match_sql,@msg,@result);

          -- insert comparison gid
					set v_match_sql = 'insert ignore into recon_tmp_tpptranwithbrkpgid (tran_gid,tranbrkp_gid)';
					set v_match_sql = concat(v_match_sql,' ');
					set v_match_sql = concat(v_match_sql,'select ');
					set v_match_sql = concat(v_match_sql,'distinct b.tran_gid,b.tranbrkp_gid ');

					set v_match_sql = concat(v_match_sql,'from recon_tmp_tppsource as a ');
					set v_match_sql = concat(v_match_sql,'inner join recon_tmp_tppcomparison as b ');
					set v_match_sql = concat(v_match_sql,'on a.recon_code = b.recon_code ');

					set v_match_sql = concat(v_match_sql,v_preprocess_condition,' ');

          call pr_run_sql(v_match_sql,@msg,@result);

          -- update in tran table
          if v_recontype_code <> 'N' then
						set v_sql = concat("
							update ",v_tran_table," as a
							inner join recon_tmp_tpptranwithbrkpgid as b on a.tran_gid = b.tran_gid and b.tranbrkp_gid = 0
							set
								a.",v_set_recon_field," = ",v_process_function,"
							where a.excp_value <> 0
							and a.delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);

						-- update in tranbrkp table
						set v_sql = concat("
							update ",v_tranbrkp_table," as a
							inner join recon_tmp_tpptranwithbrkpgid as b on a.tran_gid = b.tran_gid and b.tranbrkp_gid = a.tranbrkp_gid
							set
								a.",v_set_recon_field," = ",v_process_function,"
							where a.excp_value <> 0
							and a.delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);
          else
						set v_sql = concat("
							update ",v_tran_table," as a
							inner join recon_tmp_tpptranwithbrkpgid as b on a.tran_gid = b.tran_gid and b.tranbrkp_gid = 0
							set
								a.",v_set_recon_field," = ",v_process_function,"
							where a.delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);

						-- update in tranbrkp table
						set v_sql = concat("
							update ",v_tranbrkp_table," as a
							inner join recon_tmp_tpptranwithbrkpgid as b on a.tran_gid = b.tran_gid and b.tranbrkp_gid = a.tranbrkp_gid
							set
								a.",v_set_recon_field," = ",v_process_function,"
							where a.delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);
          end if;

          truncate recon_tmp_tppsource;
          truncate recon_tmp_tppcomparison;
          truncate recon_tmp_tppsourcedup;
          truncate recon_tmp_tppmatch;
          truncate recon_tmp_tppmatchdtl;
          truncate recon_tmp_tppmatchdtlgid;
          truncate recon_tmp_tppmatchdup;
          truncate recon_tmp_tppmatchparentgid;
          truncate recon_tmp_tppmatchko;
          truncate recon_tmp_tppmanymatch;
          truncate recon_tmp_tpptrangid;
          truncate recon_tmp_tpptranbrkpgid;
    end loop preprocess_loop;

    close preprocess_cursor;
  end preprocess_block;

  set out_result = v_count;

  set out_msg = 'Preprocess updated successfully !';

  drop temporary table if exists recon_tmp_tppsource;
  drop temporary table if exists recon_tmp_tppcomparison;
  drop temporary table if exists recon_tmp_tppsourcedup;
  drop temporary table if exists recon_tmp_tppmatch;
  drop temporary table if exists recon_tmp_tppmatchdtl;
  drop temporary table if exists recon_tmp_tppmatchdtlgid;
  drop temporary table if exists recon_tmp_tppmatchdup;
  drop temporary table if exists recon_tmp_tppmatchparentgid;
  drop temporary table if exists recon_tmp_tppmatchko;
  drop temporary table if exists recon_tmp_tppmatchkotran;
  drop temporary table if exists recon_tmp_tppmatchkotranbrkp;
  drop temporary table if exists recon_tmp_tppmatchdiff;
  drop temporary table if exists recon_tmp_tppmatchdiffdtl;
  drop temporary table if exists recon_tmp_tppmanymatch;
  drop temporary table if exists recon_tmp_tpppseudorows;
  drop temporary table if exists recon_tmp_tpptrangid;
  drop temporary table if exists recon_tmp_tpptranbrkpgid;
  drop temporary table if exists recon_tmp_tpptranwithbrkpgid;
  drop temporary table if exists recon_tmp_tppindex;
  drop temporary table if exists recon_tmp_tppsql;
end $$

DELIMITER ;