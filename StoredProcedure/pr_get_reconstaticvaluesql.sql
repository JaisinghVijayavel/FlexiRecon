DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reconstaticvaluesql` $$
CREATE PROCEDURE `pr_get_reconstaticvaluesql`(
  in in_sql text,
  in in_archival_code text,
  in in_recon_code text,
  in in_condition text,
  in in_user_code text,
  out out_sql text,
  out out_msg text,
  out out_result int
)
begin
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 05-11-2025

    Version : 1
  */

  declare v_sql text default '';
  declare v_closure_date text default '';
  declare v_cycle_date text default '';

  declare v_ref_code text default '';
  declare v_ref_value text default '';

  select
    cast(recon_closure_date as nchar),
    cast(recon_cycle_date as nchar)
  into
    v_closure_date,
    v_cycle_date
  from recon_mst_trecon
  where recon_code = in_recon_code
  and delete_flag = 'N';

  set v_closure_date = ifnull(v_closure_date,'2000-01-01');
  set v_cycle_date = ifnull(v_cycle_date,'2000-01-01');

  set v_sql = in_sql;

	-- Replace placeholders
	set v_sql = REPLACE(v_sql,'$CURDATE$',cast(curdate() as nchar));

	set v_sql = REPLACE(v_sql,'$CURDATETIME$',cast(sysdate() as nchar));

	set v_sql = REPLACE(v_sql,'$RECONCODE$',in_recon_code);

	set v_sql = REPLACE(v_sql,'$USERCODE$',in_user_code);

	set v_sql = REPLACE(v_sql,'$CYCLEDATE$',v_cycle_date);

	set v_sql = REPLACE(v_sql,'$RECONCYCLEDATE$',v_cycle_date);

	set v_sql = REPLACE(v_sql,'$RECONCLOSUREDATE$',v_closure_date);

	set v_sql = REPLACE(v_sql,'$ARCHIVALCODE$',in_archival_code);

	-- refvalue block
	refvalue_block:begin
		declare refvalue_done int default 0;
		declare refvalue_cursor cursor for
			select ref_code,ref_value from recon_mst_treference
			where recon_code = in_recon_code
			and active_status = 'Y'
			and delete_flag = 'N';
		declare continue handler for not found set refvalue_done=1;

		open refvalue_cursor;

		refvalue_loop: loop
			fetch refvalue_cursor into v_ref_code,v_ref_value;
			if refvalue_done = 1 then leave refvalue_loop; end if;

			set v_ref_code = ifnull(v_ref_code,'');
			set v_ref_value = ifnull(v_ref_value,'');

	    set v_sql = REPLACE(v_sql,v_ref_code,v_ref_value);
		end loop refvalue_loop;

		close refvalue_cursor;
	end refvalue_block;

  -- set out parameter values
  set out_sql = v_sql;
  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;