DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_basefilterformat` $$
CREATE FUNCTION `fn_get_basefilterformat`(
  in_filter_field varchar(128),
  in_filter_criteria text,
  in_add_filter int,
  in_comparison_criteria text,
  in_ident_value text
) RETURNS text CHARSET latin1
begin
  declare v_txt text;

  
  set v_txt = trim(in_filter_criteria);

  set v_txt = replace(v_txt,'$FIELD$',in_filter_field);

  if upper(v_txt) = 'EXACT' then
    set v_txt = in_filter_field;
  end if;

  
  set v_txt = fn_get_filterformat(v_txt,in_add_filter);

  if in_comparison_criteria = 'EXACT' then
    set v_txt = concat(v_txt,' = ',char(39),in_ident_value,char(39),' ');
  elseif in_comparison_criteria = 'CONTAINS'  then
    set v_txt = concat(v_txt,' LIKE ',char(39),'%',in_ident_value,'%',char(39),' ');
  elseif in_comparison_criteria = 'BEGINS WITH'  then
    set v_txt = concat(v_txt,' LIKE ',char(39),in_ident_value,'%',char(39),' ');
  elseif in_comparison_criteria = 'ENDS WITH'  then
    set v_txt = concat(v_txt,' LIKE ',char(39),'%',in_ident_value,char(39),' ');
  elseif in_comparison_criteria = 'NOT CONTAINS'  then
    set v_txt = concat('ifnull(',v_txt,','''') NOT LIKE ',char(39),'%',in_ident_value,'%',char(39),' ');
  elseif instr(in_comparison_criteria,'$FIELD$') > 0 then
    set v_txt = replace(in_comparison_criteria,'$FIELD$',v_txt);
    set v_txt = concat(v_txt,' = ',char(39),in_ident_value,char(39),' ');
  else
    set v_txt = concat(v_txt,' ',in_comparison_criteria,char(39),in_ident_value,char(39),' ');
  end if;

  return v_txt;
end $$

DELIMITER ;