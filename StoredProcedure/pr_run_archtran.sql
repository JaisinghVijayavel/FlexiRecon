DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_archtran` $$
CREATE PROCEDURE `pr_run_archtran`(
  in in_recon_code varchar(32),
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_sorting_order text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 21-04-2025

    Updated By : Vijayavel
    updated Date :

    Version : 1
  */

  declare v_archival_db_name text default '';
  declare v_archival_code text default '';

  declare v_count int default 0;
  declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranko_table text default '';

  declare v_concurrent_ko_flag text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  drop temporary table if exists recon_tmp_ttran2;

  CREATE temporary TABLE recon_tmp_ttran2 select * from recon_trn_ttran where 1 = 2;

  alter table recon_tmp_ttran2 ENGINE = MyISAM;
  alter table recon_tmp_ttran2 add primary key(tran_gid);

  create index idx_excp_value on recon_tmp_ttran2(excp_value);
  create index idx_tran_date on recon_tmp_ttran2(tran_date);
  create index idx_recon_code on recon_tmp_ttran2(recon_code);
  create index idx_dataset_code on recon_tmp_ttran2(recon_code,dataset_code);

  -- get archival table name
  set v_sql = concat("select
      archival_code,recon_code
    into
      @archival_code,@recon_code
    from recon_trn_treconarchival
    where 1 = 1
    ",in_condition,"
    and delete_flag = 'N' order by reconarchival_gid limit 1");

  call pr_run_sql2(v_sql,@msg,@result);

  set v_archival_code = ifnull(@archival_code,'');
  set in_recon_code = ifnull(@recon_code,'');

  if v_archival_code <> '' then
    set v_archival_db_name = fn_get_configvalue('archival_db_name');

    if v_archival_db_name <> '' then
      set v_archival_db_name = concat(v_archival_db_name,'.');
    end if;

    set v_tran_table = concat(v_archival_db_name,v_archival_code,'_',in_recon_code,'_tran');
    set v_tranko_table = concat(v_archival_db_name,v_archival_code,'_',in_recon_code,'_tranko');
  else
    set out_msg = 'Failed';
    set out_result = 0;
    leave me;
  end if;

  -- delete record
  if in_job_gid = 0 and in_rptsession_gid = 0 then
    delete from recon_rpt_ttran
    where user_code = in_user_code
    and job_gid = 0
    and rptsession_gid = 0;
  end if;

  -- transfer to temporary table
  set v_sql = concat(v_sql,"insert into recon_tmp_ttran2
    select z.* from (
		select
      a.*
		from ",v_tran_table," as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N'

    union all

		select
      a.*
		from ",v_tranko_table," as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N'
    LOCK IN SHARE MODE) as z
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- calc exception value based on roundoff value
  update recon_tmp_ttran2 set
    excp_value = excp_value - roundoff_value;

  -- transfer records to report table
  set @rec_slno := 0;

  set v_sql = concat("insert into recon_rpt_ttran
		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
		  @rec_slno:=@rec_slno+1,
      '", in_user_code ,"',
      b.dataset_name,
      null as match_gid,
      a.*
		from recon_tmp_ttran2 as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N' ");

  call pr_run_sql(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_ttran2;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;