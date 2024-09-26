DELIMITER $$

DROP FUNCTION IF EXISTS `SPLIT_MID` $$
CREATE FUNCTION `SPLIT_MID`
(
  x text,
  delim VARCHAR(32),
  pos_start INT,
  pos_end int
) RETURNS text
begin
  declare txt text;
  declare c int;
  declare n int;
  declare i int;

  set txt = '';
  set c = GET_OCCURRENCE(x,delim)+1;
  set n = pos_start + pos_end - 1;

  if pos_start > c then
    return '';
  end if;

  if n > c then
    set n = c;
  end if;

  set i = pos_start;

  repeat
    set txt = concat(txt,SPLIT(x,delim,i));

    if i < n then
      set txt = concat(txt,delim);
    end if;

    set i = i + 1;
  until i > n
  end repeat;


  return txt;
end $$

DELIMITER ;