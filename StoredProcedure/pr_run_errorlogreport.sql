DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_errorlogreport` $$
CREATE PROCEDURE `pr_run_errorlogreport`
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

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  set v_sql = concat("insert into recon_rpt_terrorlog
		select
      ",cast(in_rptsession_gid as nchar),",
		  ",cast(in_job_gid as nchar)," as job_gid,
      '",in_user_code,"' as user_code,
      a.*
		from recon_trn_terrorlog as a
		where a.delete_flag = 'N' ", in_condition," ",in_sorting_order," 
  ");

  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;