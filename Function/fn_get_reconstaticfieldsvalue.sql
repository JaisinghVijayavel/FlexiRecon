DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_reconstaticfieldsvalue` $$
CREATE FUNCTION `fn_get_reconstaticfieldsvalue`(
  in_archival_code text,
  in_recon_code text,
  in_condition text,
  in_filter_value text,
  in_user_code text
) RETURNS text
begin
  /*
    Created By : Hari
    Created Date :

    Updated By : Vijayavel
    updated Date : 05-11-2025

    Version : 1
  */

  declare v_closure_date text default '';
  declare v_cycle_date text default '';
  declare v_txt text default '';

  if in_filter_value = '$CURDATE$' then
    return cast(curdate() as nchar);
  elseif in_filter_value = '$CURDATETIME$' then
    return cast(sysdate() as nchar);
  elseif in_filter_value = '$RECONCODE$' then
    return in_recon_code;
  elseif in_filter_value = '$USERCODE$' then
    return in_user_code;
  elseif in_filter_value = '$ARCHIVALCODE$' then
    return in_archival_code;
  elseif in_filter_value = '$RECONCLOSUREDATE$' then
    select
      cast(recon_closure_date as nchar)
    into
      v_closure_date
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';
    set v_closure_date = ifnull(v_closure_date,'2000-01-01');
    return v_closure_date;
  elseif in_filter_value = '$RECONCYCLEDATE$' or in_filter_value = '$CYCLEDATE$' then
    select
      cast(recon_cycle_date as nchar)
    into
      v_cycle_date
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';
    set v_cycle_date = ifnull(v_cycle_date,'2000-01-01');
    return v_cycle_date;
  elseif in_filter_value = '$CONDITION$' then
    return in_condition;
  elseif exists(select ref_gid from recon_mst_treference
                where recon_code = in_recon_code
                and recon_code = in_filter_value
                and active_status = 'Y'
                and delete_flag = 'N') then

    select
      ref_value into v_txt
    from recon_mst_treference
    where recon_code = in_recon_code
    and recon_code = in_filter_value
    and active_status = 'Y'
    and delete_flag = 'N';

    set v_txt = ifnull(v_txt,'');

    return v_txt;
  else
    return in_filter_value;
  end if;
end $$

DELIMITER ;