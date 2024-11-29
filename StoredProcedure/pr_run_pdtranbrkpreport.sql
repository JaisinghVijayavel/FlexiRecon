DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_pdtranbrkpreport` $$
CREATE PROCEDURE `pr_run_pdtranbrkpreport`
(
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
    Created Date : 29-11-2024

    Updated By : Vijayavel
    updated Date : 

    Version : 1
  */

  declare v_count int default 0;
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);
  set in_user_code = ifnull(in_user_code,'');

  drop temporary table if exists recon_tmp_tpdtranbrkp;

  CREATE temporary TABLE recon_tmp_tpdtranbrkp select * from recon_trn_ttranbrkp where 1 = 2;

  alter table recon_tmp_tpdtranbrkp ENGINE = MyISAM;
  alter table recon_tmp_tpdtranbrkp add primary key(tranbrkp_gid);

  create index idx_excp_value on recon_tmp_tpdtranbrkp(excp_value);
  create index idx_tran_value on recon_tmp_tpdtranbrkp(tran_value);
  create index idx_tran_gid on recon_tmp_tpdtranbrkp(tran_gid);
  create index idx_tran_date on recon_tmp_tpdtranbrkp(tran_date);
  create index idx_recon_code on recon_tmp_tpdtranbrkp(recon_code);
  create index idx_dataset_code on recon_tmp_tpdtranbrkp(recon_code,dataset_code);

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);
  set in_user_code = ifnull(in_user_code,'');

  -- transfer to temporary table
  set v_sql = concat("insert into recon_tmp_tpdtranbrkp
		select
      a.*
		from recon_trn_ttranbrkp as a
    inner join recon_mst_tpdrecon as p on a.recon_code = p.pdrecon_code
      and p.active_status = 'Y'
      and p.delete_flag = 'N'
    left join recon_mst_tdataset as b on a.tranbrkp_dataset_code = b.dataset_code
      and b.delete_flag = 'N'
    left join recon_trn_ttran as c on a.tran_gid = c.tran_gid and c.delete_flag = 'N'
    left join recon_trn_ttranko as d on a.tran_gid = d.tran_gid and d.delete_flag = 'N'
    left join recon_mst_tdataset as f on a.dataset_code = f.dataset_code
      and f.delete_flag = 'N'
		where true ", in_condition," and a.delete_flag = 'N'

    union

		select
      a.*
		from recon_trn_ttranbrkpko as a
    inner join recon_mst_tpdrecon as p on a.recon_code = p.pdrecon_code
      and p.active_status = 'Y'
      and p.delete_flag = 'N'
    left join recon_mst_tdataset as b on a.tranbrkp_dataset_code = b.dataset_code
      and b.delete_flag = 'N'
    left join recon_trn_ttran as c on a.tran_gid = c.tran_gid and c.delete_flag = 'N'
    left join recon_trn_ttranko as d on a.tran_gid = d.tran_gid and d.delete_flag = 'N'
    left join recon_mst_tdataset as f on a.dataset_code = f.dataset_code
      and f.delete_flag = 'N'
		where true ", in_condition," and a.delete_flag = 'N'
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
		from recon_tmp_tpdtranbrkp as a
    left join recon_mst_tdataset as b on a.tranbrkp_dataset_code = b.dataset_code
      and b.delete_flag = 'N'
    left join recon_trn_ttran as c on a.tran_gid = c.tran_gid and c.delete_flag = 'N'
    left join recon_trn_ttranko as d on a.tran_gid = d.tran_gid and d.delete_flag = 'N'
    left join recon_mst_tdataset as f on a.dataset_code = f.dataset_code
      and f.delete_flag = 'N'
		where true ", in_condition," and a.delete_flag = 'N' ",in_sorting_order,"
  ");

  call pr_run_sql(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_tpdtranbrkp;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;