DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reconsummary` $$
CREATE PROCEDURE `pr_get_reconsummary`
(
  in in_recon_code varchar(32),
  in in_tran_date date,
  out out_msg text,
  out out_result int
)
me:begin
  declare v_recontype_code varchar(32) default null;

  select
    recontype_code into v_recontype_code
  from recon_mst_trecon
  where recon_code = in_recon_code
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_recontype_code = ifnull(v_recontype_code,'');

  set @out_msg2 = 'Failed';
  set @out_result2 = 0;

  if v_recontype_code = 'B' then
    call pr_get_brs(in_recon_code,in_tran_date,@out_msg2,@out_result2);
  elseif v_recontype_code = 'I' then
    call pr_get_integrity(in_recon_code,in_tran_date,@out_msg2,@out_result2);
  elseif v_recontype_code = 'W' then
    call pr_get_proof(in_recon_code,in_tran_date,@out_msg2,@out_result2);
  else
    select 'Summary Not Available' as 'Result';
  end if;

  set out_msg = @out_msg2;
  set out_result = @out_result2;
end $$

DELIMITER ;