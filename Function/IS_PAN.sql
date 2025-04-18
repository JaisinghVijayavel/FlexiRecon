DELIMITER $$

DROP FUNCTION IF EXISTS `IS_PAN` $$
CREATE FUNCTION `IS_PAN`
(
  pan_no text
) RETURNS integer
begin
  return pan_no REGEXP '(^[A-Za-z]{5}[0-9]{4}[A-Za-z]{1})';
end $$

DELIMITER ;