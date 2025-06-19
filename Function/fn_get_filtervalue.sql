DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_filtervalue` $$
CREATE FUNCTION `fn_get_filtervalue`(
  in_recon_code text,
  in_filter_value text,
  in_user_code text
) RETURNS text
begin
  declare v_closure_date text default '';
  declare v_cycle_date text default '';

  if in_filter_value = '$CURDATE$' then
    return cast(curdate() as nchar);
  elseif in_filter_value = '$CURDATETIME$' then
    return cast(sysdate() as nchar);
  elseif in_filter_value = '$RECONCODE$' then
    return in_recon_code;
  elseif in_filter_value = '$USERCODE$' then
    return in_user_code;
  elseif in_filter_value = '$RECONCLOSUREDATE$' then
    select
      cast(recon_closure_date as nchar)
    into
      v_closure_date
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_closure_date = ifnull(v_closure_date,'2000-01-01');

    return v_closure_date;
  elseif in_filter_value = '$RECONCYCLEDATE$' or in_filter_value = '$CYCLEDATE$' then
    select
      cast(recon_cycle_date as nchar)
    into
      v_cycle_date
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_cycle_date = ifnull(v_cycle_date,'2000-01-01');

    return v_cycle_date;
  else
    return in_filter_value;
  end if;
end $$

DELIMITER ;