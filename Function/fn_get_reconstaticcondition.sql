DELIMITER $$

DROP FUNCTION IF EXISTS `fn_get_reconstaticcondition` $$
CREATE FUNCTION `fn_get_reconstaticcondition`(
  in_archival_code text,
  in_recon_code text,
  in_condition text,
  in_user_code text
) RETURNS text
begin
  /*
    Created By : Vijayavel
    Created Date : 06-03-2026

    Updated By :
    updated Date :

    Version : 1
  */

  declare v_condition text default '';

  set v_condition = in_condition;

	set v_condition = REPLACE(v_condition, '$CURDATE$', fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$CURDATE$',in_user_code) );
	set v_condition = REPLACE(v_condition, '$CURDATETIME$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$CURDATETIME$',in_user_code) );
	set v_condition = REPLACE(v_condition, '$RECONCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$RECONCODE$',in_user_code) );
	set v_condition = REPLACE(v_condition, '$USERCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$USERCODE$',in_user_code) );
	set v_condition = REPLACE(v_condition, '$CYCLEDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$CYCLEDATE$',in_user_code) );
	set v_condition = REPLACE(v_condition, '$RECONCYCLEDATE$', fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$RECONCYCLEDATE$',in_user_code) );
	set v_condition = REPLACE(v_condition, '$RECONCLOSUREDATE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$RECONCLOSUREDATE$',in_user_code) );
	set v_condition = REPLACE(v_condition, '$ARCHIVALCODE$',  fn_get_reconstaticfieldsvalue(in_archival_code,in_recon_code,in_condition,'$ARCHIVALCODE$',in_user_code) );

  return v_condition;
end $$

DELIMITER ;