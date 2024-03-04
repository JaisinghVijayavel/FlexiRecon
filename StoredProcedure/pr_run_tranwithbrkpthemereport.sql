DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tranwithbrkpthemereport` $$
CREATE PROCEDURE `pr_run_tranwithbrkpthemereport`(
  in in_recon_code varchar(32),
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 08-02-2024

    Updated By : Vijayavel
    updated Date :

    Version : 1
  */

  declare v_tran_field text default '';
  declare v_tranbrkp_field text default '';
  declare v_recontype_code text default '';

  declare v_count int default 0;
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttrantheme;
  drop temporary table if exists recon_tmp_ttranbrkp;
  drop temporary table if exists recon_tmp_ttranbrkptheme;

  CREATE temporary TABLE recon_tmp_tpseudorows(
    row int unsigned NOT NULL,
    PRIMARY KEY (row)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttran(
    tran_gid int unsigned NOT NULL,
    theme_json json,
    theme_desc text,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttrantheme(
    tran_gid int unsigned NOT NULL,
    theme_code varchar(32),
    theme_desc text,
    PRIMARY KEY (tran_gid,theme_code),
    key idx_tran_gid(tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttranbrkp(
    tranbrkp_gid int unsigned NOT NULL,
    theme_json json,
    theme_desc text,
    PRIMARY KEY (tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttranbrkptheme(
    tranbrkp_gid int unsigned NOT NULL,
    theme_code varchar(32),
    theme_desc text,
    PRIMARY KEY (tranbrkp_gid,theme_code),
    key idx_tranbrkp_gid(tranbrkp_gid)
  ) ENGINE = MyISAM;

  SELECT
	  group_concat(t.COLUMN_NAME) into v_tran_field
  FROM information_schema.columns as t
  WHERE t.table_schema=database()
  AND t.table_name = 'recon_trn_ttran';

  SELECT
	  group_concat(t.COLUMN_NAME) into v_tranbrkp_field
  FROM information_schema.columns as t
  WHERE t.table_schema=database()
  AND t.table_name = 'recon_trn_ttranbrkp';

  select recontype_code into v_recontype_code from recon_mst_trecon
  where recon_code = (select recon_code from recon_trn_tjob where job_gid = in_job_gid)
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_recontype_code = ifnull(v_recontype_code,'');

  -- get theme count
  select
    count(*) into v_count
  from recon_mst_ttheme
  where recon_code = in_recon_code
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_count = ifnull(v_count,0);

  if v_count > 0 then
    insert into recon_tmp_tpseudorows select row from pseudo_rows1 where row <= v_count;

    -- insert into theme
    insert into recon_tmp_ttran
    (
      tran_gid,
      theme_json
    )
    select
      tran_gid,
      cast(concat('["',replace(theme_code,',','","'),'"]') as json) as theme_json
    from recon_trn_ttran
    where recon_code = in_recon_code
    and theme_code <> ''
    and delete_flag = 'N';

    -- insert into tran theme
    insert into recon_tmp_ttrantheme
    (
      tran_gid,
      theme_code
    )
    select
      tran_gid,
      JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_ttran.theme_json, CONCAT('$[', recon_tmp_tpseudorows.row, ']'))) AS theme_code
    from recon_tmp_ttran
    inner join recon_tmp_tpseudorows
    where JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_ttran.theme_json, CONCAT('$[', recon_tmp_tpseudorows.row, ']'))) is not null;

    update recon_tmp_ttrantheme as a
    inner join recon_mst_ttheme as b on a.theme_code = b.theme_code
      and b.recon_code = in_recon_code
      and b.delete_flag = 'N'
    set a.theme_desc = b.theme_desc;

    truncate recon_tmp_ttran;

    insert into recon_tmp_ttran
    (
      tran_gid,
      theme_desc
    )
    select
       tran_gid,
       group_concat(theme_desc)
    from recon_tmp_ttrantheme
    group by tran_gid;

    -- insert into theme
    insert into recon_tmp_ttranbrkp
    (
      tranbrkp_gid,
      theme_json
    )
    select
      tranbrkp_gid,
      cast(concat('["',replace(theme_code,',','","'),'"]') as json) as theme_json
    from recon_trn_ttranbrkp
    where recon_code = in_recon_code
    and theme_code <> ''
    and delete_flag = 'N';

    -- insert into tranbrkp theme
    insert into recon_tmp_ttranbrkptheme
    (
      tranbrkp_gid,
      theme_code
    )
    select
      tranbrkp_gid,
      JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_ttranbrkp.theme_json, CONCAT('$[', recon_tmp_tpseudorows.row, ']'))) AS theme_code
    from recon_tmp_ttranbrkp
    inner join recon_tmp_tpseudorows
    where JSON_UNQUOTE(JSON_EXTRACT(recon_tmp_ttranbrkp.theme_json, CONCAT('$[', recon_tmp_tpseudorows.row, ']'))) is not null;

    update recon_tmp_ttranbrkptheme as a
    inner join recon_mst_ttheme as b on a.theme_code = b.theme_code
      and b.recon_code = in_recon_code
      and b.delete_flag = 'N'
    set a.theme_desc = b.theme_desc;

    truncate recon_tmp_ttranbrkp;

    insert into recon_tmp_ttranbrkp
    (
      tranbrkp_gid,
      theme_desc
    )
    select
       tranbrkp_gid,
       group_concat(theme_desc)
    from recon_tmp_ttranbrkptheme
    group by tranbrkp_gid;
  end if;

  set v_sql = concat('insert into recon_rpt_ttranwithbrkp(rptsession_gid,job_gid,dataset_name,',v_tran_field,') ');
  set v_sql = concat(v_sql,'select ');
  set v_sql = concat(v_sql,cast(in_rptsession_gid as nchar),' as rptsession_gid,');
  set v_sql = concat(v_sql,cast(in_job_gid as nchar),' as job_gid,');
  set v_sql = concat(v_sql,'b.dataset_name,');
  set v_sql = concat(v_sql,concat('a.',replace(v_tran_field,',',',a.')),' from recon_trn_ttran as a ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code ');
  set v_sql = concat(v_sql,'where true ');
  set v_sql = concat(v_sql,in_condition,' ');

  if v_recontype_code = 'N' then
    -- set v_sql = concat(v_sql,'and a.excp_value <> 0 ');
  -- else
    set v_sql = concat(v_sql,'and a.ko_gid = 0 ');
  end if;

  set v_sql = concat(v_sql,'and a.delete_flag = ''N'' ');

  call pr_run_sql(v_sql,@out_msg,@out_result);

  set v_sql = concat('insert into recon_rpt_ttranwithbrkp(rptsession_gid,job_gid,dataset_name,tranbrkp_dataset_name,');
  set v_sql = concat(v_sql,'base_tran_value,base_excp_value,base_acc_mode,');
  set v_sql = concat(v_sql,v_tranbrkp_field,') ');
  set v_sql = concat(v_sql,'select ');
  set v_sql = concat(v_sql,cast(in_rptsession_gid as nchar),' as rptsession_gid,');
  set v_sql = concat(v_sql,cast(in_job_gid as nchar),' as job_gid,');
  set v_sql = concat(v_sql,'b.dataset_name,');
  set v_sql = concat(v_sql,'c.dataset_name,');
  set v_sql = concat(v_sql,'d.tran_value,d.excp_value,d.tran_acc_mode,');
  set v_sql = concat(v_sql,concat('a.',replace(v_tranbrkp_field,',',',a.')),' from recon_trn_ttranbrkp as a ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code ');
  set v_sql = concat(v_sql,'left join recon_mst_tdataset as c on a.tranbrkp_dataset_code = c.dataset_code ');
  set v_sql = concat(v_sql,'left join recon_trn_ttran as d on a.tran_gid = d.tran_gid ');
  set v_sql = concat(v_sql,'where true ');
  set v_sql = concat(v_sql,in_condition,' ');

  if v_recontype_code <> 'N' then
    set v_sql = concat(v_sql,'and a.excp_value > 0 ');
    set v_sql = concat(v_sql,'and a.tran_gid > 0 ');
  else
    set v_sql = concat(v_sql,'and 1 = 2 ');
  end if;

  set v_sql = concat(v_sql,'and a.delete_flag = ''N'' ');

  call pr_run_sql(v_sql,@out_msg,@out_result);

  -- update theme in report tranbrkp table
  update recon_rpt_ttranwithbrkp as a
  inner join recon_tmp_ttranbrkp as b on a.tranbrkp_gid = b.tranbrkp_gid
  set a.theme_code = b.theme_desc
  where a.job_gid = in_job_gid
  and a.rptsession_gid = in_rptsession_gid;

  -- update theme in report tran table
  update recon_rpt_ttranwithbrkp as a
  inner join recon_tmp_ttran as b on a.tran_gid = b.tran_gid
  set a.theme_code = b.theme_desc
  where a.job_gid = in_job_gid
  and a.rptsession_gid = in_rptsession_gid;

  set out_msg = 'Success';
  set out_result = 1;

  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttrantheme;
  drop temporary table if exists recon_tmp_ttranbrkp;
  drop temporary table if exists recon_tmp_ttranbrkptheme;
end $$

DELIMITER ;