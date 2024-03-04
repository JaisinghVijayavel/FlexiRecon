DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tranbrkpthemereport` $$
CREATE PROCEDURE `pr_run_tranbrkpthemereport`
(
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
    Created Date : 27-02-2024

    Updated By : Vijayavel
    updated Date :

    Version : 1
  */

  declare v_count int default 0;
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttranbrkp;
  drop temporary table if exists recon_tmp_ttranbrkptheme;

  CREATE temporary TABLE recon_tmp_tpseudorows(
    row int unsigned NOT NULL,
    PRIMARY KEY (row)
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

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);
  set in_user_code = ifnull(in_user_code,'');

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

  set v_sql = concat(v_sql,"insert into recon_rpt_ttranbrkp
		select
		  ",cast(in_rptsession_gid as nchar)," as rptsession_gid,
		  ",cast(in_job_gid as nchar)," as job_gid,
      '", in_user_code ,"' as user_code,
      f.dataset_name,
      b.dataset_name as tranbrkp_name,
      ifnull(c.tran_value,d.tran_value) as base_value,
      ifnull(c.excp_value,d.excp_value) as base_excp_value,
      ifnull(c.tran_acc_mode,d.tran_acc_mode) as base_acc_mode,
      a.*
		from recon_trn_ttranbrkp as a
    left join recon_mst_tdataset as b on a.tranbrkp_dataset_code = b.dataset_code
    and b.delete_flag = 'N'
    left join recon_trn_ttran as c on a.tran_gid = c.tran_gid and c.delete_flag = 'N'
    left join recon_trn_ttranko as d on a.tran_gid = d.tran_gid and d.delete_flag = 'N'
    left join recon_mst_tdataset as f on a.dataset_code = f.dataset_code
      and f.delete_flag = 'N'
		where true ", in_condition," and a.tran_gid > 0 and a.delete_flag = 'N'
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- update theme in report table
  update recon_rpt_ttranbrkp as a
  inner join recon_tmp_ttranbrkp as b on a.tranbrkp_gid = b.tranbrkp_gid
  set a.theme_code = b.theme_desc
  where a.job_gid = in_job_gid
  and a.rptsession_gid = in_rptsession_gid;

  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttranbrkp;
  drop temporary table if exists recon_tmp_ttranbrkptheme;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;