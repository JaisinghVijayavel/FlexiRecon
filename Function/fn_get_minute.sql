DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_minute` $$
CREATE FUNCTION `fn_get_minute`(cron_expression varchar(32)) RETURNS text
begin
  declare v_minute text;
  declare v_every_minute text;

  declare v_split text;

  declare v_int_minute int;
  declare v_cron_minute int;

  declare v_from int;
  declare v_to int;

  declare v_txt text;

  declare c int;
  declare r int;
  declare n int;

  set cron_expression = SPLIT(cron_expression,' ',1);

  set v_minute = date_format(sysdate(),'%i');

  if instr(cron_expression,'-') > 0 then
    -- from
    set v_txt = GET_NUM(SPLIT(cron_expression,'-',1));

    if v_txt <> '' then
      set v_from = cast(v_txt as unsigned);

      set v_from = v_from mod 61;
    else
      set v_from = 1;
    end if;

    -- to
    set v_txt = GET_NUM(SPLIT(cron_expression,'-',2));

    if v_txt <> '' then
      set v_to = cast(v_txt as unsigned);

      set v_to = v_to mod 61;
    else
      set v_to = 60;
    end if;

    set n = cast(v_minute as unsigned);

    if n >= v_from and n <= v_to then
      set v_minute = cast(n as nchar);
    else
      set v_minute = cast(v_from as nchar);
    end if;
  elseif not (cron_expression = '*' or cron_expression = '0') then
    set v_every_minute = GET_NUM(SPLIT(cron_expression,'/',2));

    if v_every_minute <> '' then
      set v_int_minute = cast(v_minute as unsigned);
      set v_cron_minute = cast(v_every_minute as unsigned);

      set c = v_int_minute div v_cron_minute;
      set r = v_int_minute mod v_cron_minute;

      set n = v_cron_minute * (c + 1);

      if n < 60 then
        set v_minute = cast(n as nchar);
      end if;
    else
      set v_split = GET_NUM(SPLIT(cron_expression,'/',1));

      if v_split <> '' then
        set v_minute = v_split;
      end if;
    end if;
  end if;

  return v_minute;
end $$

DELIMITER ;