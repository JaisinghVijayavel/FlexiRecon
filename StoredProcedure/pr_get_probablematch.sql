DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_probablematch` $$
CREATE PROCEDURE `pr_get_probablematch`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 16-07-2026

    Updated By : Vijayavel
    Updated Date :

    Version : 1
  */

  declare v_sql text default '';
  declare v_table_name text default '';

  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (period_to is null
    or period_to >= curdate())
    and active_status = 'Y'
    and delete_flag = 'N') then

    set out_msg = 'Please select valid recon !';
    set out_result = 0;

    leave me;
  end if;

  set v_table_name = concat(in_recon_code,"_RPT_AMT_MATCHED");

  if exists(select TABLE_NAME from INFORMATION_SCHEMA.TABLES
    where TABLE_SCHEMA = database()
    and TABLE_TYPE = 'BASE TABLE'
    and TABLE_NAME = v_table_name) then
    -- return the probable match
    set v_sql = concat("select * from ",v_table_name);
    call pr_run_sql2(v_sql,@msg2,@result2);

    set out_msg = 'Success';
    set out_result = 1;
  else
    set out_msg = 'Invalid table name !';
    set out_result = 0;
  end if;

end $$

DELIMITER ;