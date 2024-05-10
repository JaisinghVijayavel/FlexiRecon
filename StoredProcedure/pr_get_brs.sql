DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_brs` $$
CREATE PROCEDURE `pr_get_brs`
(
  in in_recon_code varchar(32),
  in in_tran_date date,
  out out_msg text,
  out out_result int
)
me:begin
  declare v_min_tran_date date default null;

  select
    cast(min(ko_date) as date) into v_min_tran_date
  from recon_trn_tko
  where recon_code = in_recon_code
  and ko_date >= in_tran_date
  and delete_flag = 'N';

  if v_min_tran_date is null
    or datediff(in_tran_date,v_min_tran_date) >= 0 then
    call pr_get_brs_current(in_recon_code,in_tran_date,@msg,@result);
  else
    call pr_get_brs_rollback(in_recon_code,in_tran_date,@msg,@result);
  end if;

  set out_msg = @msg;
  set out_result = @result;
end $$

DELIMITER ;