DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tranthemereport` $$
CREATE PROCEDURE `pr_run_tranthemereport`(
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
  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttrantheme;

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
  end if;

  set @rec_slno := 0;

  set v_sql = concat(v_sql,"insert into recon_rpt_ttran
		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
		  @rec_slno:=@rec_slno+1,
      '", in_user_code ,"',
      b.dataset_name,
      a.*
		from recon_trn_ttran as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition,"

    union all

		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
		  @rec_slno:=@rec_slno+1,
      '", in_user_code ,"',
      b.dataset_name,
      a.*
		from recon_trn_ttranko as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition,"
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- update theme in report table
  update recon_rpt_ttran as a
  inner join recon_tmp_ttran as b on a.tran_gid = b.tran_gid
  set a.theme_code = b.theme_desc
  where a.job_gid = in_job_gid
  and a.rptsession_gid = in_rptsession_gid;

  drop temporary table if exists recon_tmp_tpseudorows;
  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttrantheme;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;