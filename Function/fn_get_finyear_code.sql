DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_finyear_code` $$
CREATE FUNCTION `fn_get_finyear_code`(input_date DATETIME) RETURNS varchar(32)
BEGIN
    DECLARE fin_start_year INT default 0;
    DECLARE fin_end_year INT default 0;
    DECLARE fin_code VARCHAR(32) default '';

    IF MONTH(input_date) >= 4 THEN
        SET fin_start_year = YEAR(input_date);
        SET fin_end_year = YEAR(input_date) + 1;
    ELSE
        SET fin_start_year = YEAR(input_date) - 1;
        SET fin_end_year = YEAR(input_date);
    END IF;

    SET fin_code = CONCAT('FY ',cast(fin_start_year as nchar), '-', RIGHT(cast(fin_end_year as nchar), 2));
    RETURN fin_code;
END $$

DELIMITER ;