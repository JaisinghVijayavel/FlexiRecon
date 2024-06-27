DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_theme` $$
CREATE PROCEDURE `pr_run_theme`(
  in in_recon_code varchar(32),
  in in_job_gid int,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_job_gid int default 0;
  declare v_job_input_param text default '';
  declare v_date_format text default '';

  set v_date_format = fn_get_configvalue('web_date_format');

  set v_job_input_param = concat(v_job_input_param,'Period From : ',date_format(in_period_from,v_date_format),char(13),char(10));
  set v_job_input_param = concat(v_job_input_param,'Period To : ',date_format(in_period_to,v_date_format),char(13),char(10));

  set in_job_gid = ifnull(in_job_gid,0);

  if in_job_gid = 0 then
    call pr_ins_job(in_recon_code,'T',0,'Theming','',in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

    set v_job_gid = @out_job_gid;
  else
    set v_job_gid = in_job_gid;
  end if;

  call pr_run_themedirect(in_recon_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,@msg,@result);
  call pr_run_theme_comparison(in_recon_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,@msg,@result);
  call pr_run_theme_comparisonagg(in_recon_code,v_job_gid,in_period_from,in_period_to,in_automatch_flag,@msg,@result);

  call pr_upd_jobwithparam(v_job_gid,v_job_input_param,'C','Completed',@msg,@result);

  set out_result = @result;
  set out_msg = @msg;
end $$

DELIMITER ;