DELIMITER $$

DROP function IF EXISTS `fn_get_expressionformat` $$
CREATE function `fn_get_expressionformat`
(
  in_recon_code varchar(32),
  in_set_recon_field text,
  in_expression text,
  in_cumulative_flag boolean
) returns text
me:begin
  declare v_split_col text;
  declare v_field_desc text;
  declare v_field_name text;
  declare v_field_type text;
  declare v_expression text;
  declare n integer;
  declare i integer;

  set v_expression = in_expression;
  set v_split_col = '';
  set i = 0;

  repeat
    if v_split_col <> '' then
      set n = instr(v_split_col,'[');

      if n > 0 then
        set v_split_col = substr(v_split_col,n+1);
        set v_field_name = fn_get_fieldcast(in_recon_code,v_split_col);

        -- replace in expression
        set v_split_col = concat("[",v_split_col,"]");

        if v_field_name <> '' then
          set v_expression = replace(v_expression,v_split_col,v_field_name);
        end if;
      end if;
    end if;

    set i = i + 1;

    set v_split_col = SPLIT(in_expression,']',i);
  until v_split_col = ''
  end repeat;

  if lower(mid(trim(v_expression),1,3)) = 'col' then
    set v_expression = concat("cast(",v_expression," as decimal(15,2))");
  end if;

  if in_cumulative_flag = true then
    set v_expression = concat('@cumulative_value := @cumulative_value + ',v_expression);
  end if;

  if lower(mid(trim(in_set_recon_field),1,3)) = 'col' and 1 = 2 then
    set v_expression = concat('cast(',v_expression,' as nchar)');
  else
    set v_field_type = fn_get_fieldtype(in_recon_code,in_set_recon_field);

    if v_field_type = 'TEXT' then
      set v_expression = concat('cast(',v_expression,' as nchar)');
    elseif v_field_type = 'INTEGER' then
      set v_expression = concat('cast(',v_expression,' as signed)');
    elseif v_field_type = 'NUMERIC' then
      set v_expression = concat('cast(',v_expression,' as decimal(15,2))');
    elseif v_field_type = 'DATE' then
      set v_expression = concat('cast(',v_expression,' as date)');
    elseif v_field_type = 'DATETIME' then
      set v_expression = concat('cast(',v_expression,' as datetime)');
    else
      set v_expression = concat('cast(',v_expression,' as nchar)');
    end if;
  end if;

  return v_expression;
end $$

DELIMITER ;