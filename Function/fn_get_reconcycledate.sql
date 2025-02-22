DELIMITER $$

DROP function IF EXISTS `fn_get_reconcycledate` $$
CREATE function `fn_get_reconcycledate`
(
  in_recon_code text
) returns date
begin
  declare v_recon_cycle_date date;

  if exists(select recon_cycle_date from recon_mst_trecon
		where recon_code = in_recon_code
		and period_from <= curdate()
		and (period_to is null
		or period_to >= curdate())
		and active_status = 'Y'
		and delete_flag = 'N') then

		select
			recon_cycle_date
		into
			v_recon_cycle_date
		from recon_mst_trecon
		where recon_code = in_recon_code
		and period_from <= curdate()
		and (period_to is null
		or period_to >= curdate())
		and active_status = 'Y'
		and delete_flag = 'N';

  end if;

  return v_recon_cycle_date;
end $$

DELIMITER ;