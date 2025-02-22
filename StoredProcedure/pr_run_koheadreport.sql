DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_koheadreport` $$
CREATE PROCEDURE `pr_run_koheadreport`
(
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  set in_job_gid = ifnull(in_job_gid,0);
  set in_rptsession_gid = ifnull(in_rptsession_gid,0);
  set in_user_code = ifnull(in_user_code,'');

  set v_sql = concat(v_sql,"insert into recon_rpt_tko
    (
      rptsession_gid,job_gid,kodtl_gid,user_code,ko_gid,ko_date,ko_by,
      recon_code,recon_name,rule_code,rule_name,reversal_flag,manual_matchoff,
      ko_reason,ko_remark,ko_gross_value,ko_value
    )
    select z.* from (
		select
		  ",cast(in_rptsession_gid as nchar)," as rptsession_gid,
		  ",cast(in_job_gid as nchar)," as job_gid,
		  a.ko_gid as kodtl_gid,
      '", in_user_code ,"' as user_code,
		  a.ko_gid,
      a.insert_date as ko_date,
      a.insert_by as ko_by,
		  a.recon_code,
		  d.recon_name,
		  a.rule_code,
		  e.rule_name,
		  a.reversal_flag,
		  a.manual_matchoff,
		  a.ko_reason,
		  a.ko_remark,
      a.ko_value,
		  a.ko_value
		from recon_trn_tko as a
		inner join recon_mst_trecon as d on a.recon_code = d.recon_code and d.delete_flag = 'N'
		left join recon_mst_trule as e on a.rule_code = e.rule_code and e.delete_flag = 'N'
		where true ", in_condition,"
    LOCK IN SHARE MODE) as z
    ");

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;