DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_conditionpreprocess` $$
CREATE PROCEDURE `pr_get_conditionpreprocess`(
	in in_condition_type varchar(32),
  in in_field_type varchar(255),
  in in_preprocess_code varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
  /*
    Created By : vinoth
    Created Date : 11-03-2026

    Updated By :
    Updated Date :

    Version : 1
  */

  declare v_field_type text default '';

  set v_field_type = (select preprocessagg_field_type from recon_mst_tpreprocessaggfield
    where preprocessagg_field = in_field_type
    and preprocess_code=in_preprocess_code
    and delete_flag = 'N');

  set v_field_type = ifnull(v_field_type,'');

	select
    condition_criteria, condition_desc
	from recon_mst_tcondition
	where condition_type = in_condition_type
	and field_type = v_field_type
	and active_status = 'Y'
  and delete_flag = 'N';
END $$

DELIMITER ;