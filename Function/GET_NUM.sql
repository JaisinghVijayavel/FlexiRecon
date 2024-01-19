DELIMITER $$

DROP FUNCTION IF EXISTS `GET_NUM`$$
CREATE FUNCTION `GET_NUM`( str text ) RETURNS text CHARSET latin1
BEGIN
  DECLARE i, len int DEFAULT 1;
  DECLARE ret text DEFAULT '';
  DECLARE c CHAR(1);
  SET len = CHAR_LENGTH( str );
  REPEAT
    BEGIN
      SET c = MID( str, i, 1 );
      IF c REGEXP '[[:digit:]]' THEN
        SET ret=CONCAT(ret,c);
      END IF;
      SET i = i + 1; 
    END;
  UNTIL i > len END REPEAT;
  RETURN ret;
END;

 $$

DELIMITER ;