DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_reconarchival` $$
CREATE PROCEDURE `pr_set_reconarchival`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
begin
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 26-08-2025

    Version : 2
  */

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';
	declare v_koroundoff_table text default '';

  declare v_dataset_code text default '';
  declare v_dataset_name text default '';
  declare v_dataset_table text default '';

	declare v_rule_table text default '';
	declare v_preprocess_table text default '';
	declare v_theme_table text default '';

  declare v_sql text default '';
  declare v_table text default '';
  declare v_table_prefix text default '';
  declare v_table_prefix_code text default '';
  declare v_archival_code text default '';

  declare v_src_table text default '';
  declare v_dest_table text default '';

  declare v_condition text default '';
  declare v_dataset_db_name text default '';
  declare v_recontype_code text default '';

  declare v_archival_db_name text default '';
  declare v_archival_db_flag text default '';
  declare v_archival_db_prefix text default '';

  declare v_archival_qry text default '';

  declare v_concurrent_ko_flag text default '';

  declare v_recon_code text default '';
  declare v_recon_name text default '';
  declare v_recon_version text default '';

  declare v_lookup_filter text default '';

  declare v_filter_field text default '';
  declare v_filter_criteria text default '';
  declare v_filter_value_flag text default '';
  declare v_filter_value text default '';

  declare v_open_parentheses_flag text default '';
  declare v_close_parentheses_flag text default '';
  declare v_join_condition text default '';

  declare v_job_gid int default 0;

  -- recon validation
  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (period_to >= curdate() or period_to is null)
    and active_status = 'Y'
    and delete_flag = 'N') then
    set out_msg = 'Invalid recon !';
    set out_result = 0;
  else
    select
      recon_code,recon_name,recon_rule_version,recontype_code
    into
      v_recon_code,v_recon_name,v_recon_version,v_recontype_code
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_recon_code = ifnull(v_recon_code,'');
    set v_recon_name = ifnull(v_recon_name,'');
    set v_recon_version = ifnull(v_recon_version,'');
    set v_recontype_code = ifnull(v_recontype_code,'');
  end if;

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

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

	set v_rule_table = 'recon_mst_trule';
	set v_preprocess_table = 'recon_mst_tpreprocess';
	set v_theme_table = 'recon_mst_ttheme';

  -- get archival db info
  set v_archival_db_name = fn_get_configvalue('archival_db_name');
  set v_archival_db_flag = fn_get_configvalue('archival_db_flag');
  set v_archival_db_prefix = fn_get_configvalue('archival_db_prefix');

  -- generate archival code
  set v_archival_code = fn_get_autocode('RA');

  -- archival table prefix
  set v_table_prefix_code = concat(v_archival_code,'_',in_recon_code);
  set v_table_prefix = v_table_prefix_code;

  -- get archival db
  if v_archival_db_name = '' then
    if v_archival_db_flag = 'Y' then
      set v_archival_db_name = concat(v_archival_db_prefix,v_table_prefix_code);

      -- create database
      set v_sql = concat('create database if not exists ',v_archival_db_name);

      call pr_run_sql2(v_sql,@msg,@result);
    end if;
  end if;

  -- set archival db name in table prefix
  if v_archival_db_name <> '' then
    set v_table_prefix = concat(v_archival_db_name,'.',v_table_prefix);
  end if;

  -- insert into archival table
  insert into recon_trn_treconarchival
  (
    recon_code,
    recon_version,
    archival_code,
    archival_date,
    archival_by,
    archival_db_name,
    active_status,
    insert_date,
    insert_by
  )
  select
    in_recon_code,
    v_recon_version,
    v_archival_code,
    sysdate(),
    in_user_code,
    v_archival_db_name,
    'Y',
    sysdate(),
    in_user_code;

  call pr_ins_job(v_recon_code,'AR',0,
                  concat('Recon Archival:',v_archival_code,'/',v_recon_code,'-',v_recon_name),
                  '',in_user_code,'','I','Initiated...',@out_job_gid,@msg,@result);

  set v_job_gid = @out_job_gid;

  -- recon koseq
  set v_table = concat(v_table_prefix,'_recon_mst_tkoseq');
  set v_sql = concat('create table ',v_table,' like recon_mst_tkoseq');
  call pr_run_sql2(v_sql,@msg,@result);

  set v_sql = concat("insert into ",v_table,"
    select * from recon_mst_tkoseq
    where recon_code = '",in_recon_code,"'
    and recon_version = '",v_recon_version,"'
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@result,@msg);

  -- find recon type code and its condition
  if v_recontype_code <> 'N' then
    set v_condition = ' and excp_value <> 0 ';
  end if;

  -- create tran table
  set v_table = concat(v_table_prefix,'_tran');
  set v_sql = concat('create table ',v_table,' like recon_trn_ttran');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create tranko table
  set v_table = concat(v_table_prefix,'_tranko');
  set v_sql = concat('create table ',v_table,' like recon_trn_ttranko');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create tranbrkp table
  set v_table = concat(v_table_prefix,'_tranbrkp');
  set v_sql = concat('create table ',v_table,' like recon_trn_ttranbrkp');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create tranbrkpko table
  set v_table = concat(v_table_prefix,'_tranbrkpko');
  set v_sql = concat('create table ',v_table,' like recon_trn_ttranbrkpko');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create ko table
  set v_table = concat(v_table_prefix,'_ko');
  set v_sql = concat('create table ',v_table,' like recon_trn_tko');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create kodtl table
  set v_table = concat(v_table_prefix,'_kodtl');
  set v_sql = concat('create table ',v_table,' like recon_trn_tkodtl');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create koroundoff table
  set v_table = concat(v_table_prefix,'_koroundoff');
  set v_sql = concat('create table ',v_table,' like recon_trn_tkoroundoff');
  call pr_run_sql2(v_sql,@msg,@result);

  -- transfer data
  -- tran table
  call pr_upd_job(v_job_gid,'P','Archiving tran table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_tran');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select * from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    ",v_condition,"
    and delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);

  -- tranbrkp table
  call pr_upd_job(v_job_gid,'P','Archiving tranbrkp table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_tranbrkp');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select * from ",v_tranbrkp_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);

  /*
  -- tranko table
  call pr_upd_job(v_job_gid,'P','Archiving tranko table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_tranko');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select * from ",v_tranko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);

  -- tranbrkpko table
  call pr_upd_job(v_job_gid,'P','Archiving tranbrkpko table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_tranbrkpko');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select * from ",v_tranbrkpko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);

  -- ko table
  call pr_upd_job(v_job_gid,'P','Archiving ko table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_ko');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select * from ",v_ko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);

  -- kodtl table
  call pr_upd_job(v_job_gid,'P','Archiving kodtl table...',@msg,@result);

  set v_ko_table = concat(v_table_prefix,'_ko');
  set v_table = concat(v_table_prefix,'_kodtl');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select a.* from ",v_kodtl_table," as a
    inner join ",v_ko_table," as b on a.ko_gid = b.ko_gid
      and b.delete_flag = 'N'
    where b.recon_code = '",in_recon_code,"'
    and a.delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);

  -- koroundoff table
  call pr_upd_job(v_job_gid,'P','Archiving koroundoff table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_koroundoff');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select a.* from ",v_koroundoff_table," as a
    inner join ",v_ko_table," as b on a.ko_gid = b.ko_gid
      and b.delete_flag = 'N'
    where b.recon_code = '",in_recon_code,"'
    and a.delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);
  */

  -- get dataset db name
  set v_dataset_db_name = fn_get_configvalue('dataset_db_name');

  if v_dataset_db_name <> '' then
    set v_dataset_db_name = concat(v_dataset_db_name,'.');
  end if;

	-- dataset block
	dataset_block:begin
		declare dataset_done int default 0;
		declare dataset_cursor cursor for
		select
      a.dataset_code,b.dataset_name
    from recon_mst_trecondataset as a
    left join recon_mst_tdataset as b on a.dataset_code = b.dataset_code
      and b.delete_flag = 'N'
    where a.recon_code = in_recon_code
			and a.active_status = 'Y'
			and a.dataset_type = 'L'
			and a.delete_flag = 'N';
		declare continue handler for not found set dataset_done=1;

		open dataset_cursor;

		dataset_loop: loop
			fetch dataset_cursor into v_dataset_code,v_dataset_name;
			if dataset_done = 1 then leave dataset_loop; end if;

      set v_dataset_name = ifnull(v_dataset_name,'');
	    call pr_upd_job(v_job_gid,'P',concat('Archiving ',v_dataset_name,'...'),@msg,@result);

      -- create archive dataset table
      set v_table = concat(v_archival_db_name,'.',v_table_prefix_code,'_',v_dataset_code);

      -- archive dataset table data
      set v_dataset_table = concat(v_dataset_db_name,v_dataset_code);

      -- create dataset table
      set v_sql = concat("create table ",v_table," like ",v_dataset_table);
      call pr_run_sql2(v_sql,@msg,@result);

        -- filter condition
        set v_lookup_filter = ' and (';

				-- filter block
				filter_block:begin
					declare filter_done int default 0;

					declare filter_cursor cursor for
					  select
              filter_applied_on,
              filter_field,
              filter_criteria,
              filter_value_flag,
              filter_value,
              open_parentheses_flag,
              close_parentheses_flag,
              join_condition
            from recon_mst_tarchivaldatasetfilter
            where dataset_code = v_dataset_code
            and recon_code = v_recon_code
            and filter_applied_on = 'LOOKUP'
            and active_status = 'Y'
            and delete_flag = 'N'
            order by filter_seqno;

					declare continue handler for not found set filter_done=1;

					open filter_cursor;

					filter_loop: loop
						fetch filter_cursor into
              v_filter_applied_on,
              v_filter_field,
              v_filter_criteria,
              v_filter_value_flag,
              v_filter_value,
              v_open_parentheses_flag,
              v_close_parentheses_flag,
              v_join_condition;

						if filter_done = 1 then leave filter_loop; end if;

            set v_filter_field = ifnull(v_filter_field,'');
            set v_filter_criteria = ifnull(v_filter_criteria,'');

            set v_filter_value_flag = ifnull(v_filter_value_flag,'Y');
            set v_filter_value = ifnull(v_filter_value,'');

            if v_filter_value_flag = 'Y' then
              set v_filter_value = fn_get_filtervalue(v_dataset_code,v_filter_value,'');
            end if;

            set v_open_parentheses_flag = ifnull(v_open_parentheses_flag,'');
            set v_close_parentheses_flag = ifnull(v_close_parentheses_flag,'');
            set v_join_condition = ifnull(v_join_condition,'');

            if v_join_condition = '' then
              set v_join_condition = 'and';
            end if;

            if v_filter_field = '' then
              set v_filter_value_flag = '';
              set v_filter_value = '';
            end if;

            set v_open_parentheses_flag = if(v_open_parentheses_flag = 'Y','(','');
            set v_close_parentheses_flag = if(v_close_parentheses_flag = 'Y',')','');

            set v_filter_field = concat('a.',v_filter_field);
            set v_filter_field = fn_get_dsfieldnamecast(v_dataset_code,v_filter_field);

            set v_lookup_filter = concat(v_lookup_filter,
                                             v_open_parentheses_flag,
                                             fn_get_basefilterformat(v_filter_field,'EXACT',0,v_filter_criteria,v_filter_value_flag,v_filter_value),
                                             v_close_parentheses_flag,' ',
                                             v_join_condition,' ');
					end loop filter_loop;

					close filter_cursor;
				end filter_block;

        set v_lookup_filter = concat(v_lookup_filter,' 1 = 1) ');

      -- transfer dataset data
      set v_sql = concat("insert into ",v_table,"
        select z.* from (
        select * from ",v_dataset_table,"
        where delete_flag = 'N' LOCK IN SHARE MODE) as z");

      call pr_run_sql2(v_sql,@msg,@result);
		end loop dataset_loop;

		close dataset_cursor;
	end dataset_block;

	-- archtable block
	archtable_block:begin
		declare archtable_done int default 0;
		declare archtable_cursor cursor for
		select archival_table,archival_select_qry from recon_mst_tarchivaltable
      where active_status = 'Y'
      and delete_flag = 'N';
		declare continue handler for not found set archtable_done=1;

		open archtable_cursor;

		archtable_loop: loop
			fetch archtable_cursor into v_src_table,v_archival_qry;
			if archtable_done = 1 then leave archtable_loop; end if;

      set v_dest_table = concat(v_table_prefix,'_',v_src_table);

      -- create table
      set v_sql = concat("create table ",v_dest_table," like ",v_src_table);
			call pr_run_sql2(v_sql,@result,@msg);

      set v_archival_qry = replace(v_archival_qry,'$RECONCODE$',in_recon_code);
      set v_archival_qry = replace(v_archival_qry,'$RECONVERSION$',v_recon_version);

      -- transfer data
      set v_sql = concat("insert into ",v_dest_table," ",v_archival_qry);

			call pr_run_sql2(v_sql,@result,@msg);
		end loop archtable_loop;
		close archtable_cursor;
	end archtable_block;

	call pr_upd_job(v_job_gid,'C','Completed',@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;