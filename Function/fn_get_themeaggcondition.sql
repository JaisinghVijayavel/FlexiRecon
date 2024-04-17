DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_themeaggcondition` $$
CREATE FUNCTION `fn_get_themeaggcondition`
(
  in_themeagg_applied_on text,
  in_themeagg_field text,
  in_themeagg_field_type text,
  in_themeagg_criteria text,
  in_themeagg_value_flag text,
  in_themeagg_value text
) RETURNS text
begin
  declare v_txt text;
  declare v_collation text;
  declare v_themeagg_field text;
  declare v_themeagg_value text;

  if in_themeagg_applied_on = 'S' then
    set v_themeagg_field = concat('a.',in_themeagg_field);
  else
    set v_themeagg_field = concat('b.',in_themeagg_field);
  end if;

  if in_themeagg_value_flag = 'N' then
    if in_themeagg_applied_on = 'S' then
      set v_themeagg_value = concat('b.',in_themeagg_value);
    else
      set v_themeagg_value = concat('a.',in_themeagg_value);
    end if;
  else
    set v_themeagg_value = ifnull(in_themeagg_value,'');

    if in_themeagg_field_type = 'NUMERIC' or in_themeagg_field_type = 'INTEGER' then
      if v_themeagg_value = '' then
        set v_themeagg_value = '0';
      end if;

      if cast(v_themeagg_value as decimal(15,2)) = 0 then
        set v_txt = concat(' ',v_themeagg_field,' is null ');

        return v_txt;
      end if;
    else
      set v_themeagg_value = concat(char(39),v_themeagg_value,char(39));
    end if;
  end if;

  if in_themeagg_criteria = 'EXACT' then
    set v_txt = concat(' ',v_themeagg_field,' = ',v_themeagg_value,' ');
  else
    set v_txt = concat(' ',v_themeagg_field,' ',in_themeagg_criteria,' ',v_themeagg_value,' ');
  end if;

  return v_txt;
end $$

DELIMITER ;