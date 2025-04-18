DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_del_scheduler` $$
CREATE PROCEDURE `pr_del_scheduler`(
  in in_scheduler_gid int,
  in in_remark varchar(255),
  in in_user_code varchar(32),
  out out_result int,
  out out_msg text
)
me:begin
  /*
    Created By - Vijayavel
    Created Date - 14-03-2025

    Updated By - Vijayavel
    Updated Date -

	  Version - 001
	*/

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_recon_code text default '';

  declare v_concurrent_ko_flag text default '';

  declare v_dataset_code text default '';
  declare v_dataset_db_name text default '';
  declare v_sql text default '';

  declare err_msg text default '';
  declare err_flag boolean default false;
  declare v_count int default 0;

  drop temporary table if exists recon_tmp_trecon;

  CREATE temporary TABLE recon_tmp_trecon(
    recon_code varchar(32),
    PRIMARY KEY (recon_code)
  ) ENGINE = MyISAM;

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  insert into recon_tmp_trecon
		select d.recon_code from con_trn_tscheduler as a
		inner join con_mst_tpipeline as b on a.pipeline_code = b.pipeline_code and b.delete_flag = 'N'
		inner join recon_mst_trecondataset as c on b.target_dataset_code = c.dataset_code
			and c.active_status = 'Y'
			and c.delete_flag = 'N'
		inner join recon_mst_trecon as d on c.recon_code = d.recon_code
			and d.active_status = 'Y'
			and d.delete_flag = 'N'
		where a.scheduler_gid = in_scheduler_gid
		and a.delete_flag = 'N'
		group by d.recon_code;

  if exists(select scheduler_gid from recon_trn_tscheduler
    where scheduler_gid = in_scheduler_gid
    and scheduler_status in ('C','F')
    and delete_flag = 'N') then
    select
      b.target_dataset_code into v_dataset_code
    from con_trn_tscheduler as a
    inner join con_mst_tpipeline as b on a.pipeline_code = b.pipeline_code
    where a.scheduler_gid = in_scheduler_gid
    and a.delete_flag = 'N';
  else
    set err_flag = true;
    set err_msg = concat(err_msg,'Invalid scheduler !,');
  end if;

  if v_dataset_code = 'KOMANUAL' then
    if exists
      (
        select a.tran_gid from recon_trn_tmanualtran as a
        where a.scheduler_gid = in_scheduler_gid
        and a.ko_status <> 'P'
        and a.delete_flag = 'N'
      ) or exists
      (
        select a.tran_gid from recon_trn_tmanualtranmatch as a
        where a.scheduler_gid = in_scheduler_gid
        and a.ko_status <> 'P'
        and a.delete_flag = 'N'
      ) then
      set err_flag = true;
      set err_msg = concat(err_msg,'Access denied !');
    end if;
  elseif v_dataset_code = 'POSTMANUAL' then
    if exists
      (
        select a.tran_gid from recon_trn_tmanualtranbrkp as a
        where a.scheduler_gid = in_scheduler_gid
        and a.tranbrkp_status <> 'P'
        and a.delete_flag = 'N'
      ) or exists
      (
        select a.tran_gid from recon_trn_tmanualtranbrkppost as a
        where a.scheduler_gid = in_scheduler_gid
        and a.tranbrkp_status <> 'P'
        and a.delete_flag = 'N'
      ) then
      set err_flag = true;
      set err_msg = concat(err_msg,'Access denied !');
    end if;
  else
    set v_count = 0;

		-- recon block
		recon_block:begin
			declare recon_done int default 0;
			declare recon_cursor cursor for
			select recon_code from recon_tmp_trecon;
			declare continue handler for not found set recon_done=1;

			open recon_cursor;

			recon_loop: loop
				fetch recon_cursor into v_recon_code;
				if recon_done = 1 then leave recon_loop; end if;

				if v_concurrent_ko_flag = 'Y' then
					set v_tran_table = concat(v_recon_code,'_tran');
					set v_tranbrkp_table = concat(v_recon_code,'_tranbrkp');

					set v_tranko_table = concat(v_recon_code,'_tranko');
					set v_tranbrkpko_table = concat(v_recon_code,'_tranbrkpko');
				else
					set v_tran_table = 'recon_trn_ttran';
					set v_tranbrkp_table = 'recon_trn_ttranbrkp';

					set v_tranko_table = 'recon_trn_ttranko';
					set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';
				end if;

				-- tran tables
				set v_sql = concat("select count(*) into @v_count from ",v_tran_table," as a
						where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
						and (a.excp_value <> a.tran_value or a.mapped_value > 0)
						and a.delete_flag = 'N'");

				call pr_run_sql2(v_sql,@msg2,@result2);

				set v_count = v_count + ifnull(@v_count,0);

				-- tranko tables
				set v_sql = concat("select count(*) into @v_count from ",v_tranko_table," as a
						where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
						and a.delete_flag = 'N'");

				call pr_run_sql2(v_sql,@msg2,@result2);

				-- tranbrkp tables
				set v_sql = concat("select count(*) into @v_count from ",v_tranbrkp_table," as a
						where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
						and a.tran_gid > 0
						and a.delete_flag = 'N'");

				call pr_run_sql2(v_sql,@msg2,@result2);

				set v_count = v_count + ifnull(@v_count,0);

				-- tranbrkpko tables
				set v_sql = concat("select count(*) into @v_count from ",v_tranbrkpko_table," as a
						where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
						and a.delete_flag = 'N'");

				call pr_run_sql2(v_sql,@msg2,@result2);

				set v_count = v_count + ifnull(@v_count,0);
			end loop recon_loop;

			close recon_cursor;
		end recon_block;

    if v_count > 0 then
      set err_flag = true;
      set err_msg = concat(err_msg,'Access denied !');
    end if;
  end if;

  if err_flag = true then
    set out_result = 0;
    set out_msg = err_msg;
    leave me;
  end if;

  -- update in scheduler table
  update recon_trn_tscheduler set
    scheduler_status = 'D',
    scheduler_remark = in_remark,
    update_date = sysdate(),
    update_by = in_user_code
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  if v_dataset_code = 'KOMANUAL' then
    delete from recon_trn_tmanualtran where scheduler_gid = in_scheduler_gid;
  elseif v_dataset_code = 'POSTMANUAL' then
    delete from recon_trn_tmanualtranbrkp where scheduler_gid = in_scheduler_gid;
  elseif v_dataset_code = 'ACCBALANCE' then
    delete from recon_trn_taccbal where scheduler_gid = in_scheduler_gid;
  else
		-- recon1 block
		recon1_block:begin
			declare recon1_done int default 0;
			declare recon1_cursor cursor for
			select recon_code from recon_tmp_trecon;
			declare continue handler for not found set recon1_done=1;

			open recon1_cursor;

			recon1_loop: loop
				fetch recon1_cursor into v_recon_code;
				if recon1_done = 1 then leave recon1_loop; end if;

				if v_concurrent_ko_flag = 'Y' then
					set v_tran_table = concat(v_recon_code,'_tran');
					set v_tranbrkp_table = concat(v_recon_code,'_tranbrkp');
				else
					set v_tran_table = 'recon_trn_ttran';
					set v_tranbrkp_table = 'recon_trn_ttranbrkp';
				end if;

        -- delete record(s) from tran table
        set v_sql = concat("delete from ",v_tran_table,"
          where scheduler_gid = ",cast(in_scheduler_gid as nchar));

        call pr_run_sql2(v_sql,@msg2,@result2);

        -- delete record(s) from tranbrkp table
        set v_sql = concat("delete from ",v_tranbrkp_table,"
          where scheduler_gid = ",cast(in_scheduler_gid as nchar));

        call pr_run_sql2(v_sql,@msg2,@result2);
			end loop recon1_loop;

			close recon1_cursor;
		end recon1_block;
  end if;

  -- get dataset database name
  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');
  set v_dataset_db_name = ifnull(v_dataset_db_name,'');

  if v_dataset_db_name <> '' then
    set v_dataset_code = concat(v_dataset_db_name,'.',v_dataset_code);
  end if;

  set v_sql = concat("delete from ",
    v_dataset_code,"
    where scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and delete_flag = 'N'
  ");

  call pr_run_sql1(v_sql,@msg,@result);

  drop temporary table if exists recon_tmp_trecon;

  set out_result = 1;
  set out_msg = 'File deleted successfully !';
end $$

DELIMITER ;