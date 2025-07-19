DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_finyearcode` $$
CREATE FUNCTION `fn_get_finyearcode`(in_tran_date DATE) RETURNS text
BEGIN
    DECLARE v_fin_start_year INT;
    DECLARE v_fin_end_year INT;
    DECLARE v_fin_code text;

    IF MONTH(in_tran_date) >= 4 THEN
        SET v_fin_start_year = YEAR(in_tran_date);
        SET v_fin_end_year = YEAR(in_tran_date) + 1;
    ELSE
        SET v_fin_start_year = YEAR(in_tran_date) - 1;
        SET v_fin_end_year = YEAR(in_tran_date);
    END IF;

    SET v_fin_code = CONCAT(cast(v_fin_start_year as nchar), '-', RIGHT(cast(v_fin_end_year as nchar), 2));
    RETURN v_fin_code;
END $$

DELIMITER ;