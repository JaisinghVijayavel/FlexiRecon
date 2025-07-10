DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_PDLegacyTheme` $$
CREATE PROCEDURE `pr_set_PDLegacyTheme`
(
  in in_recon_code text,
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By - Vijayavel
    Created Date - 12-03-2025

    Updated By - Vijayavel
    Updated Date - 13-03-2025

	  Version - 002
	*/

  declare v_pdrecon_code text default '';
  declare v_sql text default '';

  declare v_concurrent_flag text default '';
  declare v_tran_table text default '';
  declare v_tranbrkp_table text default '';

  -- concurrent ko flag
  set v_concurrent_flag = fn_get_configvalue('concurrent_ko_flag');

  set in_recon_code = trim(in_recon_code);

  if in_recon_code = '' then
    set in_recon_code = null;
  end if;


  -- col32 - Line Ref No
  -- col33 - Theme Ref No
  -- col36 - Entry Ref No

  -- col48 Legacy Theme = theme_code
  -- col49	Legacy Line Ref No = col32
  -- col50	Legacy Theme Ref No = col33
  -- col51	Legacy Entry Ref No = col36

	-- pdrecon block
	pdrecon_block:begin
		declare pdrecon_done int default 0;
		declare pdrecon_cursor cursor for
		select pdrecon_code from recon_mst_tpdrecon
			where true
      and pdrecon_code = ifnull(in_recon_code,pdrecon_code)
      and active_status = 'Y'
			and delete_flag = 'N';
		declare continue handler for not found set pdrecon_done=1;

		open pdrecon_cursor;

		pdrecon_loop: loop
			fetch pdrecon_cursor into v_pdrecon_code;
			if pdrecon_done = 1 then leave pdrecon_loop; end if;

      if v_concurrent_flag = 'Y' then
        set v_tran_table = concat(v_pdrecon_code,'_tran');
        set v_tranbrkp_table = concat(v_pdrecon_code,'_tranbrkp');
      else
        set v_tran_table = 'recon_trn_ttran';
        set v_tranbrkp_table = 'recon_trn_ttranbrkp';
      end if;

      -- update Legacy Theme
      set v_sql = concat("
      update ",v_tran_table," set
        col48 = '',
        col49 = '',
        col50 = '',
        col51 = ''
      where recon_code = '",v_pdrecon_code,"'
      and delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);

      -- update Legacy Theme
      set v_sql = concat("
      update ",v_tranbrkp_table," set
        col48 = '',
        col49 = '',
        col50 = '',
        col51 = ''
      where recon_code = '",v_pdrecon_code,"'
      and delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);

      -- update Legacy Theme
      set v_sql = concat("
      update ",v_tran_table," set
        col48 = theme_code,
        col49 = col32,
        col50 = col33,
        col51 = col36
      where recon_code = '",v_pdrecon_code,"'
      and delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);

      -- update Legacy Theme
      set v_sql = concat("
      update ",v_tranbrkp_table," set
        col48 = theme_code,
        col49 = col32,
        col50 = col33,
        col51 = col36
      where recon_code = '",v_pdrecon_code,"'
      and delete_flag = 'N'");

      call pr_run_sql2(v_sql,@msg,@result);
		end loop pdrecon_loop;

		close pdrecon_cursor;
	end pdrecon_block;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;