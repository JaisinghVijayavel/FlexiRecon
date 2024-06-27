DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_recon_mindate` $$
CREATE PROCEDURE `pr_get_recon_mindate`
(
  in in_recon_code varchar(32)
)
me:begin
  if exists(select min(tran_date) from recon_trn_ttran
    where recon_code = in_recon_code
    and excp_value <> 0
    and delete_flag = 'N') then
    select
      ifnull(min(tran_date),curdate()) as min_tran_date
    from recon_trn_ttran
    where recon_code = in_recon_code
    and excp_value <> 0
    and delete_flag = 'N';
  else
    select curdate() as min_tran_date;
  end if;
end $$

DELIMITER ;