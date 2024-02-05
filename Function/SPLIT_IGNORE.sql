DELIMITER $$

DROP FUNCTION IF EXISTS `SPLIT_IGNORE` $$
CREATE FUNCTION `SPLIT_IGNORE`(
  x text,
  delim VARCHAR(32),
  pos INT
) RETURNS text CHARSET latin1
begin
  declare txt_prefix text;
  declare txt_suffix text;

  declare pos_prefix int;

  declare txt text;
  declare n int;

  if pos = 0 then
    return x;
  end if;

  if pos > -1 then
    set pos_prefix = pos -1;
  else
    set pos_prefix = abs(pos) -1;
    set x = REVERSE(x);
  end if;

  set txt_prefix = SUBSTRING_INDEX(x, delim, pos_prefix);

  set txt = SUBSTRING_INDEX(x, delim, pos_prefix+1);

  set txt_suffix = SUBSTRING(x,length(txt)+length(delim)+1);

  if pos < 0 then
    set txt_prefix = REVERSE(txt_prefix);
    set txt_suffix = REVERSE(txt_suffix);
  end if;

  if txt_prefix = '' or txt_suffix = '' then
    set delim = '';
  end if;

  set txt = concat(txt_prefix,delim,txt_suffix);

  if txt = '' then
    return null;
  else
    return txt;
  end if;
end $$

DELIMITER ;