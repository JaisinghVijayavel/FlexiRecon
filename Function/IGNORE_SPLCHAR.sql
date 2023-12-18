DELIMITER $$

DROP FUNCTION IF EXISTS `IGNORE_SPLCHAR`$$
CREATE FUNCTION  `IGNORE_SPLCHAR`( str text ) RETURNS text CHARSET latin1
BEGIN
  DECLARE i, len int DEFAULT 1;
  DECLARE ret text DEFAULT '';
  DECLARE c text;
  SET len = CHAR_LENGTH( str );
  REPEAT
    BEGIN
      SET c = substr( str, i, 1 );
      IF c REGEXP '[[:alnum:][:space:]]' THEN
        SET ret=CONCAT(ret,substr( str, i, 1 ));
      END IF;
      SET i = i + 1;
    END;
  UNTIL i > len END REPEAT;
  RETURN ret;
END;

 $$

DELIMITER ;