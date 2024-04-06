DELIMITER $$

DROP FUNCTION IF EXISTS `SPLIT` $$
CREATE FUNCTION `SPLIT`(
  x text,
  delim VARCHAR(32),
  pos INT
) RETURNS text CHARSET latin1
begin
  declare txt text;
  declare n int;

  if pos > -1 then
    set txt = REPLACE(SUBSTRING(SUBSTRING_INDEX(x, delim, pos),
       LENGTH(SUBSTRING_INDEX(x, delim, pos -1)) + 1),
       delim, '');
  else
    set x = REVERSE(x);
    set pos = abs(pos);

    set txt = REPLACE(SUBSTRING(SUBSTRING_INDEX(x, delim, pos),
       LENGTH(SUBSTRING_INDEX(x, delim, pos -1)) + 1),
       delim, '');

    set txt = REVERSE(txt);
    /*
    set txt = REVERSE(x);
    set n = INSTR(txt,delim);

    if n > 0 then
      set txt = REVERSE(SUBSTR(txt,1,n-1));
    else
      set txt = x;
    end if;
    */
  end if;

  /*
  if txt = '' then
    return null;
  else
    return txt;
  end if;
  */

  return txt;
end $$

DELIMITER ;