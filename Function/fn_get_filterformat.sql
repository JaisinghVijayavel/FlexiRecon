DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_filterformat` $$
CREATE FUNCTION `fn_get_filterformat`(
  in_field_value text,
  in_filter_flag int
) RETURNS text CHARSET latin1
begin
  declare v_txt text default '';
  set v_txt = in_field_value;
  set in_filter_flag = ifnull(in_filter_flag,0);

  if (in_filter_flag & 1) > 0 then set v_txt = concat('fn_get_numeric(',v_txt,')'); end if;
  if (in_filter_flag & 2) > 0 then set v_txt = concat('fn_get_alpha(',v_txt,')'); end if;
  if (in_filter_flag & 4) > 0 then set v_txt = concat('fn_get_alphanum(',v_txt,')'); end if;
  if (in_filter_flag & 8) > 0 then set v_txt = concat('fn_get_ignorespace(',v_txt,')'); end if;
  if (in_filter_flag & 16) > 0 then set v_txt = concat('fn_get_ignoresplchar(',v_txt,')'); end if;
  if (in_filter_flag & 32) > 0 then set v_txt = concat('fn_get_ignorenumeric(',v_txt,')'); end if;
  if (in_filter_flag & 64) > 0 then set v_txt = concat('fn_get_ignorealpha(',v_txt,')'); end if;
  if (in_filter_flag & 128) > 0 then set v_txt = concat('fn_get_ignorealphanum(',v_txt,')'); end if;
  if (in_filter_flag & 256) > 0 then set v_txt = concat('fn_get_ignoreprefixzero(',v_txt,')'); end if;

  return v_txt;
end $$

DELIMITER ;