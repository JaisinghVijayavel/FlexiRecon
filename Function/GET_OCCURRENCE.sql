DELIMITER $$

DROP FUNCTION IF EXISTS `GET_OCCURRENCE`$$
CREATE FUNCTION  `GET_OCCURRENCE`(base_str text,chk_str text) RETURNS int
BEGIN
  RETURN ifnull(cast((length(base_str)-length(replace(base_str,chk_str,'')))/length(chk_str) as unsigned),0);
END;

 $$

DELIMITER ;