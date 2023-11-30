DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_fieldfilterformat` $$
CREATE FUNCTION `fn_get_fieldfilterformat`(
  in_filter_field varchar(128),
  in_filter_criteria text,
  in_add_filter int
) RETURNS text CHARSET latin1
begin
  declare v_txt text;

  
  set v_txt = trim(in_filter_criteria);

  set v_txt = replace(v_txt,'$FIELD$',in_filter_field);

  if upper(v_txt) = 'EXACT' then
    set v_txt = in_filter_field;
  end if;

  
  set v_txt = fn_get_filterformat(v_txt,in_add_filter);

  if in_filter_criteria = v_txt then
    set v_txt = in_filter_field;
  end if;

  return v_txt;
end $$

DELIMITER ;