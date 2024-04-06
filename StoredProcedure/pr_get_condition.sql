DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_condition` $$
CREATE PROCEDURE `pr_get_condition`
(
	in in_condition_type varchar(32),
  in in_field_type varchar(255),
  in in_recon_code varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32)
)
BEGIN
  declare v_field_type text default '';

  set v_field_type = (select recon_field_type from recon_mst_treconfield
    where recon_field_desc = in_field_type
    and recon_code = in_recon_code
    and delete_flag = 'N');

  set v_field_type = ifnull(v_field_type,'');

  if v_field_type = '' then
    set v_field_type = (select field_type from recon_mst_tfieldstru
      where field_alias_name = in_field_type
      and delete_flag = 'N');
  end if;

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