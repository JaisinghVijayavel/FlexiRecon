DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_accbalreport` $$
CREATE PROCEDURE `pr_run_accbalreport`
(
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_sorting_order text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_sql text default '';

  set v_sql = concat("insert into recon_rpt_taccbal
		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
      a.accbal_gid,
      '",in_user_code,"' as user_code,
      a.scheduler_gid,
      a.dataset_code,
      b.dataset_name,
      a.tran_date,
      a.bal_value,
      a.insert_date,
      a.insert_by,
      'N'
		from recon_trn_taccbal as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code and b.delete_flag = 'N'
		where a.delete_flag = 'N' ", in_condition," ",in_sorting_order,"
  ");

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;