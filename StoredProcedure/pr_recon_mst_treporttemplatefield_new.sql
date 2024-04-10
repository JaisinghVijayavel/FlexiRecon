DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treporttemplatefield_new` $$
CREATE PROCEDURE `pr_recon_mst_treporttemplatefield_new`
(
  in in_reporttemplate_code varchar(32),
	in in_jsonArray longtext,
	out out_msg text,
	out out_result int
)
BEGIN
	DECLARE i INT DEFAULT 0;
	DECLARE rowCount INT DEFAULT JSON_LENGTH(in_jsonArray);
	DECLARE v_reporttemplate_code varchar(32);
	declare v_report_field,v_display_flag text;

	-- set v_reporttemplate_code = replace(JSON_EXTRACT(in_jsonArray, CONCAT('$[', 0, '].in_reporttemplate_code')),'"','');
  set v_reporttemplate_code = in_reporttemplate_code;

	#Delete existing record against reporttemplate_code;
	delete from recon_mst_treporttemplatefield
	where reporttemplate_code = v_reporttemplate_code;

  WHILE i < rowCount DO
		INSERT INTO recon_mst_treporttemplatefield
		(
			reporttemplate_code,
			report_field,
      display_desc,
			display_flag,
			display_order,
			active_status,
			insert_by,
			insert_date
		)
    SELECT
      replace(v_reporttemplate_code,'"',''),
      replace(JSON_EXTRACT(in_jsonArray, CONCAT('$[', i, '].in_report_field')),'"',''),
      replace(JSON_EXTRACT(in_jsonArray, CONCAT('$[', i, '].in_display_desc')),'"',''),
      replace(JSON_EXTRACT(in_jsonArray, CONCAT('$[', i, '].in_display_flag')),'"',''),
      replace(JSON_EXTRACT(in_jsonArray, CONCAT('$[', i, '].in_display_order')),'"',''),
      replace(JSON_EXTRACT(in_jsonArray, CONCAT('$[', i, '].in_active_status')),'"',''),
      replace(JSON_EXTRACT(in_jsonArray, CONCAT('$[', i, '].in_action_by')),'"',''),
      now();

    SET i = i + 1;
  END WHILE;

  set out_result = 1;
	set out_msg = 'Record saved successfully.. !';
END $$

DELIMITER ;