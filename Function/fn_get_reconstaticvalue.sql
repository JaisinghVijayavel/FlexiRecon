DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_reconstaticvalue` $$
CREATE FUNCTION `fn_get_reconstaticvalue`(
  in_archival_code text,
  in_recon_code text,
  in_value text,
  in_user_code text
) RETURNS text
begin
  /*
    Created By : Vijayavel
    Created Date : 20-07-2026

    Updated By :
    updated Date :

    Version : 1
  */

  declare v_static_value text default '';

  set v_static_value = in_value;

	set v_static_value = REPLACE(v_static_value, '$CURDATE$', fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_value,'$CURDATE$',in_user_code) );
	set v_static_value = REPLACE(v_static_value, '$CURDATETIME$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_value,'$CURDATETIME$',in_user_code) );
	set v_static_value = REPLACE(v_static_value, '$RECONCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_value,'$RECONCODE$',in_user_code) );
	set v_static_value = REPLACE(v_static_value, '$USERCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_value,'$USERCODE$',in_user_code) );
	set v_static_value = REPLACE(v_static_value, '$CYCLEDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_value,'$CYCLEDATE$',in_user_code) );
	set v_static_value = REPLACE(v_static_value, '$RECONCYCLEDATE$', fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_value,'$RECONCYCLEDATE$',in_user_code) );
	set v_static_value = REPLACE(v_static_value, '$RECONCLOSUREDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_value,'$RECONCLOSUREDATE$',in_user_code) );
	set v_static_value = REPLACE(v_static_value, '$ARCHIVALCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_value,'$ARCHIVALCODE$',in_user_code) );

  return v_static_value;
end $$

DELIMITER ;