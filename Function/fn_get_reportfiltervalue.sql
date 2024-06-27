DELIMITER $$

DROP function IF EXISTS `fn_get_reportfiltervalue` $$
CREATE function `fn_get_reportfiltervalue`
(
  in_recon_code text,
  in_user_code text,
  in_filter_value text
) returns text
begin
  if in_filter_value = '$CURDATE$' then
    return cast(curdate() as nchar);
  elseif in_filter_value = '$CURDATETIME$' then
    return cast(sysdate() as nchar);
  elseif in_filter_value = '$RECONCODE$' then
    return in_recon_code;
  elseif in_filter_value = '$USERCODE$' then
    return in_user_code;
  else
    return in_filter_value;
  end if;
end $$

DELIMITER ;