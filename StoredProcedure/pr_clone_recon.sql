DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_clone_recon` $$
CREATE PROCEDURE `pr_clone_recon`
(
  in in_recon_name varchar(255),
  in in_clone_recon_code varchar(32),
  in in_user_code varchar(32),
  out out_recon_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_sql text default '';
  declare v_txt text default '';
  declare v_recon_code text default '';

  set out_recon_code='';
  set in_recon_name = ifnull(in_recon_name,'');

  if in_recon_name = '' then
    set out_msg = 'Recon name is blank !';
    set out_result = 0;
    leave me;
  end if;

  if exists(select recon_code from recon_mst_trecon
    where recon_name = in_recon_name
    and delete_flag = 'N') then
    set out_msg = 'Duplicate recon name !';
    set out_result = 0;
    leave me;
  end if;

  if exists(select recon_code from recon_mst_trecon
    where recon_code = in_clone_recon_code
    and delete_flag = 'N') then

    set v_recon_code = fn_get_autocode('RECON');
    set out_recon_code = v_recon_code;

    -- insert in recon table
    insert into recon_mst_trecon
    (
      recon_code,
      recon_name,
      recontype_code,
      recon_value_flag,
      recon_value_field,
      recon_date_flag,
      recon_date_field,
      recon_automatch_partial,
      threshold_code,
      threshold_plus_value,
      threshold_minus_value,
      unit_code,
      period_from,
      period_to,
      until_active_flag,
      clone_recon_code,
      active_status,
      insert_date,
      insert_by
    )
    select
      v_recon_code,
      in_recon_name,
      recontype_code,
      recon_value_flag,
      recon_value_field,
      recon_date_flag,
      recon_date_field,
      recon_automatch_partial,
      threshold_code,
      threshold_plus_value,
      threshold_minus_value,
      unit_code,
      curdate(),
      period_to,
      until_active_flag,
      recon_code,
      'D',
      sysdate(),
      in_user_code
    from recon_mst_trecon
    where recon_code = in_clone_recon_code
    and delete_flag = 'N';

    -- insert in recon field table
    insert into recon_mst_treconfield
    (
      recon_code,
      recon_field_seqno,
      recon_field_name,
      recon_field_desc,
      recon_field_sno,
      display_flag,
      display_order,
      recon_field_type,
      recon_field_length,
      system_field_flag,
      active_status,
      insert_date,
      insert_by
    )
    select
      v_recon_code,
      recon_field_seqno,
      recon_field_name,
      recon_field_desc,
      recon_field_sno,
      display_flag,
      display_order,
      recon_field_type,
      recon_field_length,
      system_field_flag,
      active_status,
      sysdate(),
      in_user_code
    from recon_mst_treconfield
    where recon_code = in_clone_recon_code
    and active_status = 'Y'
    and delete_flag = 'N';
  end if;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;