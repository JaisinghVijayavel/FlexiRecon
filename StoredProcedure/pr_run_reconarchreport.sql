DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_reconarchreport` $$
CREATE PROCEDURE `pr_run_reconarchreport`
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
  declare v_sql text default '';

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);
  set in_user_code = ifnull(in_user_code,'');

  set v_sql = concat(v_sql,"insert into recon_rpt_treconarchival
    (
      rptsession_gid,
      job_gid,
      reconarchival_gid,
      user_code,
      archival_code,
      recon_code,
      recon_name,
      archival_date,
      archival_by,
      active_status
    )
    select z.* from (
		select
		  ",cast(in_rptsession_gid as nchar)," as rptsession_gid,
		  ",cast(in_job_gid as nchar)," as job_gid,
		  a.reconarchival_gid,
      '", in_user_code ,"' as user_code,
		  a.archival_code,
      a.recon_code,
      b.recon_name,
      a.archival_date,
      a.archival_by,
      a.active_status
    from recon_trn_treconarchival as a
		left join recon_mst_trecon as b on a.recon_code = b.recon_code and b.delete_flag = 'N'
		where true ", in_condition," ",in_sorting_order,"
    LOCK IN SHARE MODE) as z
  ");

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;