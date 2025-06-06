﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_tranbrkpreport` $$
CREATE PROCEDURE `pr_run_tranbrkpreport`
(
  in in_archival_code varchar(32),
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
    Created Date : 28-07-2023

    Updated By : Vijayavel
    updated Date : 24-04-2025

    Version : 5
  */

  declare v_count int default 0;
  declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

  declare v_concurrent_ko_flag text default '';
  declare v_table_prefix text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);
  set in_user_code = ifnull(in_user_code,'');

  drop temporary table if exists recon_tmp_ttranbrkp;

  CREATE temporary TABLE recon_tmp_ttranbrkp select * from recon_trn_ttranbrkp where 1 = 2;

  alter table recon_tmp_ttranbrkp ENGINE = MyISAM;
  alter table recon_tmp_ttranbrkp add primary key(tranbrkp_gid);

  create index idx_excp_value on recon_tmp_ttranbrkp(excp_value);
  create index idx_tran_value on recon_tmp_ttranbrkp(tran_value);
  create index idx_tran_gid on recon_tmp_ttranbrkp(tran_gid);
  create index idx_tran_date on recon_tmp_ttranbrkp(tran_date);
  create index idx_recon_code on recon_tmp_ttranbrkp(recon_code);
  create index idx_dataset_code on recon_tmp_ttranbrkp(recon_code,dataset_code);

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);
  set in_user_code = ifnull(in_user_code,'');

  -- get transaction table
  set v_table_prefix = fn_get_recontableprefix(in_archival_code,in_recon_code);

  set v_tran_table = concat(v_table_prefix,'tran');
  set v_tranbrkp_table = concat(v_table_prefix,'tranbrkp');

  set v_tranko_table = concat(v_table_prefix,'tranko');
  set v_tranbrkpko_table = concat(v_table_prefix,'tranbrkpko');

  -- transfer to temporary table
  set v_sql = concat("insert into recon_tmp_ttranbrkp
    select z.* from (
		select
      a.*
		from ",v_tranbrkp_table," as a
    left join recon_mst_tdataset as b on a.tranbrkp_dataset_code = b.dataset_code
      and b.delete_flag = 'N'
    left join ",v_tran_table," as c on a.tran_gid = c.tran_gid and c.delete_flag = 'N'
    left join ",v_tranko_table," as d on a.tran_gid = d.tran_gid and d.delete_flag = 'N'
    left join recon_mst_tdataset as f on a.dataset_code = f.dataset_code
      and f.delete_flag = 'N'
		where true ", in_condition," and a.delete_flag = 'N'

    union

		select
      a.*
		from ",v_tranbrkpko_table," as a
    left join recon_mst_tdataset as b on a.tranbrkp_dataset_code = b.dataset_code
      and b.delete_flag = 'N'
    left join ",v_tran_table," as c on a.tran_gid = c.tran_gid and c.delete_flag = 'N'
    left join ",v_tranko_table," as d on a.tran_gid = d.tran_gid and d.delete_flag = 'N'
    left join recon_mst_tdataset as f on a.dataset_code = f.dataset_code
      and f.delete_flag = 'N'
		where true ", in_condition," and a.delete_flag = 'N'
    LOCK IN SHARE MODE) as z
  ");

  call pr_run_sql(v_sql,@msg,@result);

  -- transfer records to report table
  set v_sql = concat("insert into recon_rpt_ttranbrkp
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
		from recon_tmp_ttranbrkp as a
    left join recon_mst_tdataset as b on a.tranbrkp_dataset_code = b.dataset_code
      and b.delete_flag = 'N'
    left join ",v_tran_table," as c on a.tran_gid = c.tran_gid and c.delete_flag = 'N'
    left join ",v_tranko_table," as d on a.tran_gid = d.tran_gid and d.delete_flag = 'N'
    left join recon_mst_tdataset as f on a.dataset_code = f.dataset_code
      and f.delete_flag = 'N'
		where true ", in_condition," and a.delete_flag = 'N' ",in_sorting_order,"
  ");

  call pr_run_sql(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_ttranbrkp;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;