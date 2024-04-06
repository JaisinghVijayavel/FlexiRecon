DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_clone_reconrule` $$
CREATE PROCEDURE `pr_clone_reconrule`(
  in in_recon_code varchar(32),
  in in_rule_name varchar(255),
  in in_source_dataset_code varchar(32),
  in in_comparison_dataset_code varchar(32),
  in in_clone_recon_code varchar(32),
  in in_clone_rule_code varchar(32),
  in in_clone_source_dataset_code varchar(32),
  in in_clone_comparison_dataset_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_sql text default '';
  declare v_txt text default '';
  declare v_rule_code text default '';
  declare v_rule_order decimal(5,2) default 0;

  drop temporary table if exists recon_tmp_treconfield;
  drop temporary table if exists recon_tmp_tsourcefield;
  drop temporary table if exists recon_tmp_tcomparisonfield;

  create temporary table recon_tmp_treconfield
  (
    recon_code varchar(32) not null,
    recon_field_name varchar(128) not null,
    PRIMARY KEY (recon_code,recon_field_name)
  );

  create temporary table recon_tmp_tsourcefield
  (
    dataset_code varchar(32) not null,
    dataset_field_name varchar(128) not null,
    PRIMARY KEY (dataset_code,dataset_field_name)
  );

  create temporary table recon_tmp_tcomparisonfield
  (
    dataset_code varchar(32) not null,
    dataset_field_name varchar(128) not null,
    PRIMARY KEY (dataset_code,dataset_field_name)
  );

  -- check source dataset mapping status
  if not exists(select recon_code from recon_mst_trecondataset
    where recon_code = in_recon_code
    and dataset_code = in_source_dataset_code
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Recon source dataset was not mapped !';

    leave me;
  end if;

  -- check comparison dataset mapping status
  if not exists(select recon_code from recon_mst_trecondataset
    where recon_code = in_recon_code
    and dataset_code = in_comparison_dataset_code
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Recon comparison dataset was not mapped !';

    leave me;
  end if;

  -- check clone source dataset status
  if not exists(select recon_code from recon_mst_trecondataset
    where recon_code = in_clone_recon_code
    and dataset_code = in_clone_source_dataset_code
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Clone source dataset was invalid !';

    leave me;
  end if;

  -- check clone comparison dataset status
  if not exists(select recon_code from recon_mst_trecondataset
    where recon_code = in_clone_recon_code
    and dataset_code = in_clone_comparison_dataset_code
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Clone comparison dataset was invalid !';

    leave me;
  end if;

  -- check clone rule code
  if not exists(select recon_code from recon_mst_trule
    where recon_code = in_clone_recon_code
    and rule_code = in_clone_rule_code
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Clone rule was invalid !';

    leave me;
  end if;

  -- temporary recon field
  insert into recon_tmp_treconfield
  (
    recon_code,
    recon_field_name
  )
  select
    a.recon_code,
    a.recon_field_name
  from recon_mst_treconfield as a
  inner join recon_mst_treconfield as b on a.recon_field_name = b.recon_field_name
    and b.recon_code = in_clone_recon_code
    and b.active_status = 'Y'
    and b.delete_flag = 'N'
  where a.recon_code = in_recon_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  -- temporary source field
  insert into recon_tmp_tsourcefield
  (
    dataset_code,
    dataset_field_name
  )
  select
    a.dataset_code,
    a.dataset_table_field
  from recon_mst_tdatasetfield as a
  inner join recon_mst_tdatasetfield as b on a.dataset_table_field = b.dataset_table_field
    and b.dataset_code = in_clone_source_dataset_code
    and b.active_status = 'Y'
    and b.delete_flag = 'N'
  where a.dataset_code = in_source_dataset_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  -- temporary comparison field
  insert into recon_tmp_tcomparisonfield
  (
    dataset_code,
    dataset_field_name
  )
  select
    a.dataset_code,
    a.dataset_table_field
  from recon_mst_tdatasetfield as a
  inner join recon_mst_tdatasetfield as b on a.dataset_table_field = b.dataset_table_field
    and b.dataset_code = in_clone_comparison_dataset_code
    and b.active_status = 'Y'
    and b.delete_flag = 'N'
  where a.dataset_code = in_comparison_dataset_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  -- clone rule
  set v_rule_code = fn_get_autocode('RULE');

  select
    max(rule_order) into v_rule_order
  from recon_mst_trule
  where recon_code = in_recon_code
  and active_status <> 'N'
  and delete_flag = 'N';

  set v_rule_order = round(ifnull(v_rule_order,0) + 1,0);

  insert into recon_mst_trule
  (
    rule_code,
    rule_name,
    recon_code,
    rule_apply_on,
    rule_order,
    source_dataset_code,
    source_acc_mode,
    comparison_dataset_code,
    comparison_acc_mode,
    group_flag,
    group_method_flag,
    manual_match_flag,
    hold_flag,
    probable_match_flag,
    system_match_flag,
    period_from,
    period_to,
    until_active_flag,
    clone_rule_code,
    active_status,
    insert_date,
    insert_by
  )
  select
    v_rule_code,
    in_rule_name,
    in_recon_code,
    rule_apply_on,
    v_rule_order,
    in_source_dataset_code,
    source_acc_mode,
    in_comparison_dataset_code,
    comparison_acc_mode,
    group_flag,
    group_method_flag,
    manual_match_flag,
    hold_flag,
    probable_match_flag,
    system_match_flag,
    curdate(),
    period_to,
    until_active_flag,
    rule_code,
    'D',
    sysdate(),
    in_user_code
  from recon_mst_trule
  where rule_code = in_clone_rule_code
  and delete_flag = 'N';

  -- clone rule condition
  insert into recon_mst_trulecondition
  (
    rule_code,
    source_field,
    extraction_criteria,
    extraction_filter,
    comparison_field,
    comparison_criteria,
    comparison_filter,
    open_parentheses_flag,
    close_parentheses_flag,
    join_condition,
    active_status,
    insert_date,
    insert_by
  )
  select
    v_rule_code,
    a.source_field,
    a.extraction_criteria,
    a.extraction_filter,
    a.comparison_field,
    a.comparison_criteria,
    a.comparison_filter,
    a.open_parentheses_flag,
    a.close_parentheses_flag,
    a.join_condition,
    a.active_status,
    sysdate(),
    in_user_code
  from recon_mst_trulecondition as a
  inner join recon_tmp_treconfield as b on a.source_field = b.recon_field_name
    and a.comparison_field = b.recon_field_name
  where a.rule_code = in_clone_rule_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  -- clone rule group field
  insert recon_mst_trulegrpfield
  (
    rule_code,
    grp_field,
    active_status,
    insert_date,
    insert_by
  )
  select
    v_rule_code,
    a.grp_field,
    a.active_status,
    sysdate(),
    in_user_code
  from recon_mst_trulegrpfield as a
  inner join recon_tmp_treconfield as b on a.grp_field = b.recon_field_name
  where a.rule_code = in_clone_rule_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  -- clone rule recorder
  insert recon_mst_trulerecorder
  (
    rule_code,
    recorder_applied_on,
    recorder_field,
    active_status,
    insert_date,
    insert_by
  )
  select
    v_rule_code,
    a.recorder_applied_on,
    a.recorder_field,
    a.active_status,
    sysdate(),
    in_user_code
  from recon_mst_trulerecorder as a
  inner join recon_tmp_treconfield as b on a.recorder_field = b.recon_field_name
  where a.rule_code = in_clone_rule_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  -- clone rule ruleselefilter
  insert recon_mst_truleselefilter
  (
    rule_code,
    filter_applied_on,
    filter_field,
    filter_criteria,
    add_filter,
    ident_criteria,
    ident_value,
    open_parentheses_flag,
    close_parentheses_flag,
    join_condition,
    active_status,
    insert_date,
    insert_by
  )
  select
    v_rule_code,
    a.filter_applied_on,
    a.filter_field,
    a.filter_criteria,
    a.add_filter,
    a.ident_criteria,
    a.ident_value,
    a.open_parentheses_flag,
    a.close_parentheses_flag,
    a.join_condition,
    a.active_status,
    sysdate(),
    in_user_code
  from recon_mst_truleselefilter as a
  inner join recon_tmp_treconfield as b on a.filter_field = b.recon_field_name
  where a.rule_code = in_clone_rule_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  drop temporary table if exists recon_tmp_treconfield;
  drop temporary table if exists recon_tmp_tsourcefield;
  drop temporary table if exists recon_tmp_tcomparisonfield;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;