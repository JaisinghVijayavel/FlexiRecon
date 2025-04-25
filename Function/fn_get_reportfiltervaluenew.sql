DELIMITER $$

DROP function IF EXISTS `fn_get_reportfiltervaluenew` $$
CREATE function `fn_get_reportfiltervaluenew`
(
  in_archival_code text,
  in_recon_code text,
  in_condition text,
  in_filter_value text,
  in_user_code text
) returns text
begin
  declare v_closure_date text default '';

  if in_filter_value = '$CURDATE$' then
    return cast(curdate() as nchar);
  elseif in_filter_value = '$CURDATETIME$' then
    return cast(sysdate() as nchar);
  elseif in_filter_value = '$RECONCODE$' then
    return in_recon_code;
  elseif in_filter_value = '$USERCODE$' then
    return in_user_code;
  elseif in_filter_value = '$ARCHIVALCODE$' then
    return in_archival_code;
  elseif in_filter_value = '$RECONCLOSUREDATE$' then
    select
      cast(recon_closure_date as nchar)
    into
      v_closure_date
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_closure_date = ifnull(v_clousre_date,'2000-01-01');

    return v_closure_date;
  elseif in_filter_value = '$CONDITION$' then
    return in_condition;
  else
    return in_filter_value;
  end if;
end $$

DELIMITER ;