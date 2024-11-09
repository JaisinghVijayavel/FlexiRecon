DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_aggcondition` $$
CREATE FUNCTION `fn_get_aggcondition`
(
  in_agg_applied_on text,
  in_agg_field text,
  in_agg_field_type text,
  in_agg_criteria text,
  in_agg_value_flag text,
  in_agg_value text
) RETURNS text
begin
  declare v_txt text;
  declare v_collation text;
  declare v_agg_field text;
  declare v_agg_value text;

  if in_agg_applied_on = 'S' then
    set v_agg_field = concat('a.',in_agg_field);
  else
    set v_agg_field = concat('b.',in_agg_field);
  end if;

  if in_agg_value_flag = 'N' then
    if in_agg_applied_on = 'S' then
      set v_agg_value = concat('b.',in_agg_value);
    else
      set v_agg_value = concat('a.',in_agg_value);
    end if;
  else
    set v_agg_value = ifnull(in_agg_value,'');

    if in_agg_field_type = 'NUMERIC' or in_agg_field_type = 'INTEGER' then
      if v_agg_value = '' then
        set v_agg_value = '0';
      end if;

      if cast(v_agg_value as decimal(15,2)) = 0 then
        set v_txt = concat(' ',v_agg_field,' is null ');

        return v_txt;
      end if;
    else
      if trim(lower(v_agg_value)) <> 'null' then
        set v_agg_value = concat(char(39),v_agg_value,char(39));
      end if;
    end if;
  end if;

  if in_agg_field_type = 'NUMERIC' then
    set v_agg_field = concat('cast(',v_agg_field,' as decimal(15,2))');

    if in_agg_value_flag = 'N' then
      set v_agg_value = concat('cast(',v_agg_value,' as decimal(15,2))');
    end if;
  end if;

  if in_agg_field_type = 'INTEGER' then
    set v_agg_field = concat('cast(',v_agg_field,' as signed)');

    if in_agg_value_flag = 'N' then
      set v_agg_value = concat('cast(',v_agg_value,' as signed)');
    end if;
  end if;

  if in_agg_criteria = 'EXACT' then
    if trim(lower(v_agg_value)) <> 'null' then
      set v_txt = concat(' ',v_agg_field,' = ',v_agg_value,' ');
    else
      set v_txt = concat(' ',v_agg_field,' is null ');
    end if;
  else
    set v_txt = concat(' ',v_agg_field,' ',in_agg_criteria,' ',v_agg_value,' ');
  end if;

  return v_txt;
end $$

DELIMITER ;