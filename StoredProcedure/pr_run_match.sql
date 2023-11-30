DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_match` $$
CREATE PROCEDURE `pr_run_match`(
  in in_recon_code text,
  in in_ko_post_flag char(1),
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_ip_addr varchar(255),
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  if in_ko_post_flag = 'K' then
    call pr_run_ko(in_recon_code,in_period_from,in_period_to,in_automatch_flag,in_ip_addr,in_user_code,@msg,@result);
  elseif in_ko_post_flag = 'P' then
    call pr_post_bulk_tranbrkp(in_recon_code,in_period_from,in_period_to,in_automatch_flag,in_ip_addr,in_user_code,@msg,@result);
  else
    set @msg = 'Invalid match type !';
    set @result = 0;
  end if;

  set out_msg = @msg;
  set out_result = @result;
end $$

DELIMITER ;