DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_recon_mindate` $$
CREATE PROCEDURE `pr_get_recon_mindate`
(
  in in_recon_code varchar(32)
)
me:begin
  declare v_finyear_start_date date;
  declare v_min_tran_date date;

  if month(curdate()) > 3 then
    set v_finyear_start_date = cast(concat(cast(year(curdate()) as nchar),'-04-01') as date);
  else
    set v_finyear_start_date = cast(concat(cast(year(curdate())-1 as nchar),'-04-01') as date);
  end if;

  if exists(select tran_date from recon_trn_ttran
    where recon_code = in_recon_code
    and excp_value <> 0
    and delete_flag = 'N' limit 1 LOCK IN SHARE MODE) then
    select
      ifnull(min(tran_date),curdate()) into v_min_tran_date
    from recon_trn_ttran
    where recon_code = in_recon_code
    and excp_value <> 0
    and delete_flag = 'N' LOCK IN SHARE MODE;

    if datediff(v_finyear_start_date,v_min_tran_date) < 0 then
      set v_min_tran_date = v_finyear_start_date;
    end if;

    select DATE_FORMAT(v_min_tran_date,'%d/%m/%Y') as min_tran_date;
  else
    select DATE_FORMAT(curdate(),'%d/%m/%Y') as min_tran_date;
  end if;
end $$

DELIMITER ;