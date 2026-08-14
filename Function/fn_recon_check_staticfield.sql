DELIMITER $$

DROP function IF EXISTS `fn_recon_check_staticfield` $$

CREATE FUNCTION `fn_recon_check_staticfield`(in_input TEXT) RETURNS varchar(10)
BEGIN
    DECLARE v_pos INT DEFAULT 1;
    DECLARE v_start INT;
    DECLARE v_end INT;
    DECLARE v_val VARCHAR(255);
    DECLARE v_result VARCHAR(10) DEFAULT 'Valid';
    DECLARE v_cnt INT;

    check_loop: WHILE v_pos > 0 DO
        -- Find starting $
        SET v_start = LOCATE('$', in_input, v_pos);
        IF v_start = 0 THEN
            LEAVE check_loop;
        END IF;

        -- Find ending $
        SET v_end = LOCATE('$', in_input, v_start + 1);
        IF v_end = 0 THEN
            LEAVE check_loop;
        END IF;

        -- Extract token
        SET v_val = SUBSTRING(in_input, v_start, v_end - v_start + 1);

        -- Check existence in master table (case-insensitive)
        SELECT COUNT(*)
        INTO v_cnt
        FROM recon_mst_tstaticfields 
        WHERE delete_flag = 'N'
          AND LOWER(name) = LOWER(v_val);

        IF v_cnt = 0 THEN
            SET v_result = 'Invalid';
            LEAVE check_loop; -- stop at first invalid token
        END IF;

        SET v_pos = v_end + 1;
    END WHILE;

    RETURN v_result;
END $$

DELIMITER ;