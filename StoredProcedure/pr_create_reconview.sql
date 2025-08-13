DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_create_reconview` $$
CREATE PROCEDURE `pr_create_reconview`( IN in_recon_code VARCHAR(64))
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE v_sql TEXT DEFAULT '';
  DECLARE v_view_name VARCHAR(128);

  declare v_concurrent_ko_flag text default '';
  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  DECLARE v_select_line TEXT;

  -- Cursor variables
  DECLARE v_recon_field_name VARCHAR(255);
  DECLARE v_recon_field_desc VARCHAR(255);
  DECLARE v_recon_field_type VARCHAR(64);
  DECLARE v_recon_field_length VARCHAR(64);
	DECLARE v_display_order decimal(14,3);

  -- Cursor declaration
  DECLARE cur CURSOR FOR
		SELECT * FROM
    (
      SELECT
        recon_field_name,
        recon_field_desc,
        recon_field_type,
        recon_field_length,
        display_order
      FROM recon_mst_treconfield
      WHERE recon_code = in_recon_code
      AND active_status = 'Y'
      AND system_field_flag = 'N'
      AND delete_flag = 'N'

      UNION ALL

      SELECT
			  b.field_name AS recon_field_name,
        b.field_alias_name AS recon_field_desc,
			  b.field_type AS recon_field_type,
        b.field_length AS recon_field_length,
        a.display_order
		  FROM recon_mst_tsystemfield AS a
		  INNER JOIN recon_mst_tfieldstru AS b ON a.field_name = b.field_name AND b.delete_flag = 'N'
		  WHERE a.active_status = 'Y'
      AND a.table_name = 'recon_trn_ttran'
      AND a.delete_flag = 'N'
    ) AS c ORDER BY c.display_order;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  -- Init view name
  SET v_view_name = CONCAT(in_recon_code, '_view');

  -- Drop view if exists
  SET @drop_stmt = CONCAT('DROP VIEW IF EXISTS ', v_view_name);
  PREPARE stmt FROM @drop_stmt;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

  -- Start building the SELECT clause
  OPEN cur;

  read_loop: LOOP
    FETCH cur INTO v_recon_field_name,v_recon_field_desc,v_recon_field_type,v_recon_field_length,v_display_order;

    IF done THEN LEAVE read_loop; END IF;

    -- Determine the appropriate CAST based on field_type
    IF (UPPER(v_recon_field_type) = 'NUMERIC' OR UPPER(v_recon_field_type) = 'DECIMAL') and v_recon_field_length != '' THEN
      SET v_select_line = CONCAT('CAST(`', v_recon_field_name, '` AS DECIMAL(',v_recon_field_length,')) AS `', v_recon_field_desc, '`');
    ELSEIF UPPER(v_recon_field_type) = 'DATE' THEN
      SET v_select_line = CONCAT('CAST(`', v_recon_field_name, '` AS DATE) AS `', v_recon_field_desc, '`');
    ELSEIF UPPER(v_recon_field_type) = 'INTEGER' THEN
      SET v_select_line = CONCAT('CAST(`', v_recon_field_name, '` AS SIGNED) AS `', v_recon_field_desc, '`');
    ELSE
      -- Default fallback: no cast
      SET v_select_line = CONCAT('`', v_recon_field_name, '` AS `', v_recon_field_desc, '`');
    END IF;

    -- Append to SQL
    SET v_sql = CONCAT( v_sql, v_select_line,', ');

  END LOOP;

  CLOSE cur;

	-- Trim the last comma and space
  SET v_sql = LEFT(v_sql, LENGTH(v_sql) - 2);

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
    set v_tran_table = concat(in_recon_code,'_tran');
    set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  else
    set v_tran_table = 'recon_trn_ttran';
    set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  -- Final CREATE VIEW SQL
  SET @full_sql = CONCAT("CREATE VIEW ", v_view_name, " AS
    SELECT ", v_sql, ",0 as 'Supporting Tran Id' FROM ", v_tran_table, "
    WHERE recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
    union
    SELECT ", v_sql, ",tranbrkp_gid as 'Supporting Tran Id' FROM ", v_tranbrkp_table, "
    WHERE recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
   ");
  -- select @full_sql;

  PREPARE stmt FROM @full_sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
END $$

DELIMITER ;