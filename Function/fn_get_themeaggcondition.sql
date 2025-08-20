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

    if (in_themeagg_field_type = 'NUMERIC' or in_themeagg_field_type = 'INTEGER') and
      (in_themeagg_criteria = '=' or in_themeagg_criteria = 'EXACT') then

      if v_themeagg_value = '' then
        set v_themeagg_value = '0';
      end if;

      if cast(v_themeagg_value as decimal(15,2)) = 0 then
        set v_txt = concat(" (",v_themeagg_field," is null or cast(if(",v_themeagg_field,"='','0',",v_themeagg_field,") as decimal(15,2)) = 0)");

        return v_txt;
      end if;
    else
      if trim(lower(v_themeagg_value)) <> 'null' then
        set v_themeagg_value = concat(char(39),v_themeagg_value,char(39));
      end if;
    end if;
  end if;

  if in_themeagg_field_type = 'NUMERIC' then
    set v_themeagg_field = concat('cast(',v_themeagg_field,' as decimal(15,2))');

    if in_themeagg_value_flag = 'N' then
      set v_themeagg_value = concat('cast(',v_themeagg_value,' as decimal(15,2))');
    end if;
  end if;

  if in_themeagg_field_type = 'INTEGER' then
    set v_themeagg_field = concat('cast(',v_themeagg_field,' as signed)');

    if in_themeagg_value_flag = 'N' then
      set v_themeagg_value = concat('cast(',v_themeagg_value,' as signed)');
    end if;
  end if;

  if in_themeagg_criteria = 'EXACT' then
    if trim(lower(v_themeagg_value)) <> 'null' then
      set v_txt = concat(' ',v_themeagg_field,' = ',v_themeagg_value,' ');
    else
      set v_txt = concat(' ',v_themeagg_field,' is null ');
    end if;
  else
    if instr(in_themeagg_criteria,'$FIELD$') > 0 or
       instr(in_themeagg_criteria,'$SOURCE_FIELD$') > 0 or
       instr(in_themeagg_criteria,'$COMPARISON_FIELD$') > 0 then

      set v_txt = in_themeagg_criteria;

      set v_txt = replace(v_txt,'$FIELD$','$SOURCE_FIELD$');
      set v_txt = replace(v_txt,'$SOURCE_FIELD$',v_themeagg_field);
      set v_txt = replace(v_txt,'$COMPARISON_FIELD$',v_themeagg_value);
    else
      set v_txt = concat(' ',v_themeagg_field,' ',in_themeagg_criteria,' ',v_themeagg_value,' ');
    end if;
  end if;

  if trim(v_txt) = '' then
    set v_txt = ' 1 = 1 ';
  end if;

  return v_txt;
end $$

DELIMITER ;