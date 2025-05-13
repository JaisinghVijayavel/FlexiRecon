DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_create_datasetview` $$
CREATE PROCEDURE `pr_create_datasetview`(IN in_schema_name VARCHAR(255), IN in_dataset_code VARCHAR(64))
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_sql TEXT DEFAULT '';
    DECLARE v_view_name VARCHAR(128);
    DECLARE v_table_name VARCHAR(128);
    DECLARE v_select_line TEXT;

    -- Cursor variables
    DECLARE v_field_name VARCHAR(255);
    DECLARE v_table_field VARCHAR(255);
    DECLARE v_field_type VARCHAR(64);
    DECLARE v_field_length VARCHAR(64);

    -- Cursor declaration
    DECLARE cur CURSOR FOR
      SELECT
        dataset_table_field, field_name, field_type,field_length
      FROM recon_mst_tdatasetfield
      WHERE dataset_code = in_dataset_code
		  AND active_status = 'Y'
		  AND delete_flag = 'N'
      ORDER BY dataset_field_sno;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    -- Init view and table name
    SET v_view_name = CONCAT(in_dataset_code, '_view');
    SET v_table_name = in_dataset_code;

    -- Drop view if exists
    SET @drop_stmt = CONCAT('DROP VIEW IF EXISTS ', in_schema_name , '.', v_view_name);
    PREPARE stmt FROM @drop_stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Start building the SELECT clause
    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO v_table_field, v_field_name, v_field_type, v_field_length;
        IF done THEN
          LEAVE read_loop;
        END IF;

        -- Determine the appropriate CAST based on field_type
        IF UPPER(v_field_type) = 'NUMERIC' OR UPPER(v_field_type) = 'DECIMAL' THEN
          SET v_select_line = CONCAT('CAST(`', v_table_field, '` AS DECIMAL(',v_field_length,')) AS `', v_field_name, '`');
        ELSEIF UPPER(v_field_type) = 'DATE' THEN
          SET v_select_line = CONCAT('CAST(`', v_table_field, '` AS DATE) AS `', v_field_name, '`');
        ELSEIF UPPER(v_field_type) = 'INTEGER' THEN
          SET v_select_line = CONCAT('CAST(`', v_table_field, '` AS SIGNED) AS `', v_field_name, '`');
        ELSE
          -- Default fallback: no cast
          SET v_select_line = CONCAT('`', v_table_field, '` AS `', v_field_name, '`');
        END IF;

        -- Append to SQL
        SET v_sql = CONCAT( v_sql, v_select_line,', ');

    END LOOP;

    CLOSE cur;

    -- Trim the last comma and space
    SET v_sql = LEFT(v_sql, LENGTH(v_sql) - 2);

    -- Final CREATE VIEW SQL
    SET @full_sql = CONCAT('CREATE VIEW ', in_schema_name, '.', v_view_name, ' AS SELECT ', v_sql, ' FROM ', v_table_name, '');

    PREPARE stmt FROM @full_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$

DELIMITER ;