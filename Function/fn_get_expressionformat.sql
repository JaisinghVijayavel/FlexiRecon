DELIMITER $$

DROP function IF EXISTS `fn_get_expressionformat` $$
CREATE function `fn_get_expressionformat`
(
  in_recon_code varchar(32),
  in_expression text
) returns text
me:begin
  declare v_split_col text;
  declare v_field_desc text;
  declare v_field_name text;
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

  return v_expression;
end $$

DELIMITER ;