DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_multiresultset_parse_json_array` $$
CREATE PROCEDURE `pr_run_multiresultset_parse_json_array`(IN json_text TEXT)
BEGIN
	
	DECLARE i INT DEFAULT 0;
	DECLARE total INT DEFAULT 0;
	DECLARE rpt_code VARCHAR(100);
	DECLARE rpt_name VARCHAR(255);
	DECLARE cond TEXT; 
	
	-- Count number of elements in array
	SET total = JSON_LENGTH(json_text);
	
	WHILE i < total DO
	   SET rpt_code = JSON_UNQUOTE(JSON_EXTRACT(json_text, CONCAT('$[', i, '].report_code')));
	   SET rpt_name = JSON_UNQUOTE(JSON_EXTRACT(json_text, CONCAT('$[', i, '].report_name')));
	   SET cond     = JSON_UNQUOTE(JSON_EXTRACT(json_text, CONCAT('$[', i, '].sql_condition')));
	
	   INSERT INTO tmp_report_conditions (report_code, report_name, sql_condition)
	   VALUES (rpt_code, rpt_name, cond);
	
	   SET i = i + 1;
	 END WHILE;
END $$

DELIMITER ;