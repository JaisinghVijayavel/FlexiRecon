DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_clearrecon` $$
CREATE PROCEDURE `pr_set_clearrecon`
(
  in in_recon_code varchar(32)
)
begin
  declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';
	declare v_koroundoff_table text default '';

  declare v_concurrent_ko_flag text default '';

  declare v_dataset_code text default '';
  declare v_dataset_db text default '';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  -- get dataset db name
  set v_dataset_db = fn_get_configvalue('dataset_db_name');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(in_recon_code,'_tran');
	  set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');

	  set v_tranko_table = concat(in_recon_code,'_tranko');
	  set v_tranbrkpko_table = concat(in_recon_code,'_tranbrkpko');

	  set v_ko_table = concat(in_recon_code,'_ko');
	  set v_kodtl_table = concat(in_recon_code,'_kodtl');
	  set v_koroundoff_table = concat(in_recon_code,'_koroundoff');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

	  set v_tranko_table = 'recon_trn_ttranko';
	  set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';

	  set v_ko_table = 'recon_trn_tko';
	  set v_kodtl_table = 'recon_trn_tkodtl';
	  set v_koroundoff_table = 'recon_trn_tkoroundoff';
  end if;

  -- kodtl
  set v_sql = concat("
  delete from ",v_kodtl_table,"
  where ko_gid in
  (
    select ko_gid from ",v_ko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
  )
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- koroundoff
  set v_sql = concat("
  delete from ",v_koroundoff_table,"
  where ko_gid in
  (
    select ko_gid from ",v_ko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'
  )
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- ko
  set v_sql = concat("
  delete from ",v_ko_table,"
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- tran
  set v_sql = concat("
  delete from ",v_tran_table,"
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- tranko
  set v_sql = concat("
  delete from ",v_tranko_table,"
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- tranbrkp
  set v_sql = concat("
  delete from ",v_tranbrkp_table,"
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- tranbrkpko
  set v_sql = concat("
  delete from ",v_tranbrkpko_table,"
  where recon_code = '",in_recon_code,"'
  and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg2,@result2);

	-- recondataset block
	recondataset_block:begin
		declare recondataset_done int default 0;
		declare recondataset_cursor cursor for
			select
				dataset_code
			from recon_mst_trecondataset
			where recon_code = in_recon_code
			and dataset_type in ('B','T','S')
			and active_status = 'Y'
			and delete_flag = 'N'
			order by 1;
		declare continue handler for not found set recondataset_done=1;

		open recondataset_cursor;

		recondataset_loop: loop
			fetch recondataset_cursor into v_dataset_code;
			if recondataset_done = 1 then leave recondataset_loop; end if;

			if v_dataset_code <> '' then
        if v_dataset_db <> '' then
          set v_dataset_code = concat(v_dataset_db,'.',v_dataset_code);
        end if;

        set v_sql = concat('truncate ',v_dataset_code);

        call pr_run_sql2(v_sql,@msg2,@result2);
			end if;
		end loop recondataset_loop;

		close recondataset_cursor;
	end recondataset_block;
end $$

DELIMITER ;