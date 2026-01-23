DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_ecfchecklist` $$
CREATE PROCEDURE `pr_get_ecfchecklist`
(
  in_checklist_json json
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 23-01-2026

    Updated By :
    updated Date :

    Version : 1
  */

	declare v_checklist_info text default '';
	declare v_checklist_json json;
	declare v_checklist_size integer default 0;

  drop temporary table if exists recon_tmp_tpseudorows2;

  CREATE TEMPORARY TABLE recon_tmp_tpseudorows2
  (
    row int(10) unsigned NOT NULL,
    PRIMARY KEY (row)
  ) ENGINE = MyISAM;


  select
    JSON_EXTRACT(in_checklist_json,'$.info'),
    cast(JSON_EXTRACT(in_checklist_json,'$.checklist') as json)
  into
    v_checklist_info,
    v_checklist_json;

  -- get the size
  select JSON_LENGTH(v_checklist_json) into v_checklist_size;

  set v_checklist_size = ifnull(v_checklist_size,0);

  insert into recon_tmp_tpseudorows2 select row from pseudo_rows1 where row <= v_checklist_size;


  select v_checklist_info,v_checklist_json;

  select
    JSON_UNQUOTE(JSON_EXTRACT(a.checklist_json, CONCAT('$[', b.row, '].checklist_desc'))) AS checklist_desc,
    JSON_UNQUOTE(JSON_EXTRACT(a.checklist_json, CONCAT('$[', b.row, '].checklist_category'))) AS checklist_category,
    JSON_UNQUOTE(JSON_EXTRACT(a.checklist_json, CONCAT('$[', b.row, '].checklist_template'))) AS checklist_template,
    JSON_UNQUOTE(JSON_EXTRACT(a.checklist_json, CONCAT('$[', b.row, '].hold_flag'))) AS hold_flag,
    JSON_UNQUOTE(JSON_EXTRACT(a.checklist_json, CONCAT('$[', b.row, '].reject_flag'))) AS reject_flag
  FROM (select v_checklist_json as checklist_json) as a
  JOIN recon_tmp_tpseudorows2 as b
  HAVING checklist_desc IS NOT NULL;

  drop temporary table if exists recon_tmp_tpseudorows2;
END $$

DELIMITER ;