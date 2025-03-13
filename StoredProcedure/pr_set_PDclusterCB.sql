DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_PDclusterCB` $$
CREATE PROCEDURE `pr_set_PDclusterCB`
(
  in in_recon_code text,
  in in_cluster_name text,
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By - Vijayavel
    Created Date - 12-03-2025

    Updated By -
    Updated Date -

	  Version - 001
	*/

  declare v_pdrecon_code text default '';
  declare v_sql text default '';
  declare v_unitds_code text default 'DS276';
  declare v_cbds_code text default 'DS426';
  declare v_dsdb_name text default '';

  declare v_unit_name text default '';
  declare v_cycle_date date default null;

  set v_dsdb_name = fn_get_configvalue('dataset_db_name');

  if v_dsdb_name <> '' then
    set v_unitds_code = concat(v_dsdb_name,'.',v_unitds_code);
    set v_cbds_code = concat(v_dsdb_name,'.',v_cbds_code);
  end if;

  -- unit master
  -- col2 - Unit Name
  -- col4 - Recon Code
  -- col5 - Cluster Name
  -- col7 - New/Old

	-- pdrecon block
	pdrecon_block:begin
		declare pdrecon_done int default 0;
		declare pdrecon_cursor cursor for
		select pdrecon_code from recon_mst_tpdrecon
			where cluster_name = in_cluster_name
			and active_status = 'Y'
			and delete_flag = 'N';
		declare continue handler for not found set pdrecon_done=1;

		open pdrecon_cursor;

		pdrecon_loop: loop
			fetch pdrecon_cursor into v_pdrecon_code;
			if pdrecon_done = 1 then leave pdrecon_loop; end if;

      set @unit_name = '';

      -- get unit name
      set v_sql = concat("select col2 into @unit_name from ",v_unitds_code,"
        where col4 = '",v_pdrecon_code,"'
        and delete_flag = 'N'
        ");

      call pr_run_sql2(v_sql,@msg,@result);

      set v_unit_name = ifnull(@unit_name,'');

      -- get recon cycle date
      select
        recon_cycle_date
      into
        v_cycle_date
      from recon_mst_trecon
      where recon_code = v_pdrecon_code
      and active_status = 'Y'
      and delete_flag = 'N';

      -- update IUT CB Flag
      update recon_trn_ttran set
        col74 = null
      where recon_code = in_recon_code
      and col38 = v_pdrecon_code
      and delete_flag = 'N';

      -- validate IUT CB
      call pr_set_PDclusterIUTIP_CB(in_recon_code,v_pdrecon_code,v_cycle_date,in_cluster_name,v_unit_name,v_cbds_code,'IP Refund',@msg,@result);
      call pr_set_PDclusterIUTIP_CB(in_recon_code,v_pdrecon_code,v_cycle_date,in_cluster_name,v_unit_name,v_cbds_code,'IP Deposit',@msg,@result);
      call pr_set_PDclusterIUTUHID_CB(in_recon_code,v_pdrecon_code,v_cycle_date,in_cluster_name,v_unit_name,v_cbds_code,@msg,@result);
		end loop pdrecon_loop;

		close pdrecon_cursor;
	end pdrecon_block;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;