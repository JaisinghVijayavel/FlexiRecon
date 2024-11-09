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

  set v_iuttheme_field = ifnull(v_iuttheme_field,'');

  drop temporary table if exists recon_tmp_tiuttheme;

  create temporary table recon_tmp_tiuttheme(
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    iut_theme text,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  -- move iut theme in temp table
  set v_sql = concat("insert into recon_tmp_tiuttheme(tran_gid,tranbrkp_gid,iut_theme)
    select
      cast(col1 as signed),
      cast(col2 as signed),
      ",v_iuttheme_field,"
    from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and ",v_iuttheme_field," <> ''
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
      a.tran_remark2 = b.iut_theme
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- sync iut theme in pd recon
  set v_sql = concat("update ",v_tranbrkp_table," as a
    inner join recon_tmp_tiuttheme as b on a.tran_gid = b.tran_gid
      and b.tranbrkp_gid = a.tranbrkp_gid
      and b.iut_theme <> ''
    set
      a.theme_code = b.iut_theme,
      a.tran_remark2 = b.iut_theme
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_tiuttheme;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;