DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_syncpdtheme` $$
CREATE PROCEDURE `pr_run_syncpdtheme`
(
  in in_recon_code text,
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_sql text default '';
  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  declare v_iuttheme_field text default '';
  declare v_noniuttheme_field text default '';
  declare v_iutflag_field text default '';
  declare v_iutvalue_field text default '';
  declare v_closingbalance_field text default '';

  declare v_recon_iutflag_field text default '';
  declare v_recon_iutvalue_field text default '';
  declare v_recon_closingbalance_field text default '';

  /*
    set v_tran_table = concat(in_recon_code,'_tran');
    set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  */

  set v_tran_table = 'recon_trn_ttran';
  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  select
    recon_field_name into v_iuttheme_field
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_desc = 'IUT Theme'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_iuttheme_field = fn_get_reconfieldfromdesc(in_recon_code,'IUT Theme');
  set v_noniuttheme_field = fn_get_reconfieldfromdesc(in_recon_code,'Non IUT Theme');
  set v_iutvalue_field = fn_get_reconfieldfromdesc(in_recon_code,'IUT Value');
  set v_iutflag_field = fn_get_reconfieldfromdesc(in_recon_code,'IUT IP/OP');
  set v_closingbalance_field = fn_get_reconfieldfromdesc(in_recon_code,'Closing Balance');

  set v_recon_iutflag_field = fn_get_reconfieldfromdesc('RE170','IUT Flag');
  set v_recon_iutvalue_field = fn_get_reconfieldfromdesc('RE170','IUT Value');
  set v_recon_closingbalance_field = fn_get_reconfieldfromdesc('RE170','Closing Balance');

  drop temporary table if exists recon_tmp_tiuttheme;

  create temporary table recon_tmp_tiuttheme(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    iut_theme text,
    iut_flag text,
    noniut_theme text,
    iut_value text,
    closing_balance text,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  -- move iut theme in temp table
  set v_sql = concat("insert into recon_tmp_tiuttheme(tran_gid,tranbrkp_gid,iut_theme,iut_flag,iut_value,closing_balance)
    select
      cast(col1 as signed),
      cast(col2 as signed),
      ",v_iuttheme_field,",
      ",v_iutflag_field,",
      ",v_iutvalue_field,",
      ",v_closingbalance_field,"
    from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and ",v_iuttheme_field," <> ''
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- move noniut theme in temp table
  set v_sql = concat("insert ignore into recon_tmp_tiuttheme(tran_gid,tranbrkp_gid,noniut_theme,iut_value,closing_balance)
    select
      cast(col1 as signed),
      cast(col2 as signed),
      ",v_noniuttheme_field,",
      ",v_iutvalue_field,",
      ",v_closingbalance_field,"
    from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and ",v_noniuttheme_field," <> ''
    and delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- sync iut theme in pd recon
  set v_sql = concat("update ",v_tran_table," as a
    inner join recon_tmp_tiuttheme as b on a.tran_gid = b.tran_gid
      and b.tranbrkp_gid = 0
      and b.iut_theme <> ''
    set
      a.theme_code = b.iut_theme,
      a.tran_remark2 = b.iut_theme,
      a.",v_recon_iutflag_field," = b.iut_flag,
      a.",v_recon_iutvalue_field," = b.iut_value,
      a.",v_recon_closingbalance_field," = b.closing_balance
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- sync iut theme in pd recon
  set v_sql = concat("update ",v_tranbrkp_table," as a
    inner join recon_tmp_tiuttheme as b on a.tran_gid = b.tran_gid
      and b.tranbrkp_gid = a.tranbrkp_gid
      and b.iut_theme <> ''
    set
      a.theme_code = b.iut_theme,
      a.tran_remark2 = b.iut_theme,
      a.",v_recon_iutflag_field," = b.iut_flag,
      a.",v_recon_iutvalue_field," = b.iut_value,
      a.",v_recon_closingbalance_field," = b.closing_balance
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- sync noniut theme in pd recon
  set v_sql = concat("update ",v_tran_table," as a
    inner join recon_tmp_tiuttheme as b on a.tran_gid = b.tran_gid
      and b.tranbrkp_gid = 0
      and b.noniut_theme <> ''
    set
      a.",v_recon_iutflag_field," = b.noniut_theme,
      a.",v_recon_iutvalue_field," = b.iut_value,
      a.",v_recon_closingbalance_field," = b.closing_balance
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- sync noniut theme in pd recon
  set v_sql = concat("update ",v_tranbrkp_table," as a
    inner join recon_tmp_tiuttheme as b on a.tran_gid = b.tran_gid
      and b.tranbrkp_gid = a.tranbrkp_gid
      and b.iut_flag <> ''
    set
      a.",v_recon_iutflag_field," = b.iut_flag,
      a.",v_recon_iutvalue_field," = b.iut_value,
      a.",v_recon_closingbalance_field," = b.closing_balance
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_tiuttheme;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;