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
    updated Date : 25-04-2025

    Version : 1
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

  declare v_condition text default '';
  declare v_dataset_db_name text default '';
  declare v_recontype_code text default '';

  declare v_archival_db_name text default '';
  declare v_archival_db_flag text default '';
  declare v_archival_db_prefix text default '';

  declare v_concurrent_ko_flag text default '';

  declare v_recon_code text default '';
  declare v_recon_name text default '';

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
      recon_code,recon_name
    into
      v_recon_code,v_recon_name
    from recon_mst_trecon
    where recon_code = in_recon_code
    and delete_flag = 'N';

    set v_recon_code = ifnull(v_recon_code,'');
    set v_recon_name = ifnull(v_recon_name,'');
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

  -- find recon type code and its condition
  select
    recontype_code
  into
    v_recontype_code
  from recon_mst_trecon
  where recon_code = in_recon_code
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_recontype_code = ifnull(v_recontype_code,'');

  if v_recontype_code <> 'N' then
    set v_condition = ' and excp_value <> 0 ';
  end if;


  -- create tran table
  set v_table = concat(v_table_prefix,'_tran');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_ttran where 1 = 2');

  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(tran_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add scheduler_gid
  set v_sql = concat('create index idx_scheduler_gid on ',v_table,'(scheduler_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code
  set v_sql = concat('create index idx_recon_code on ',v_table,'(recon_code)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,dataset_code
  set v_sql = concat('create index idx_dataset_code on ',v_table,'(recon_code,dataset_code)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,tran_value
  set v_sql = concat('create index idx_tran_value on ',v_table,'(recon_code,tran_value)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,excp_value
  set v_sql = concat('create index idx_excp_value on ',v_table,'(recon_code,excp_value)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create tranko table
  set v_table = concat(v_table_prefix,'_tranko');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_ttran where 1 = 2');

  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(tran_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add scheduler_gid
  set v_sql = concat('create index idx_scheduler_gid on ',v_table,'(scheduler_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code
  set v_sql = concat('create index idx_recon_code on ',v_table,'(recon_code)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,dataset_code
  set v_sql = concat('create index idx_dataset_code on ',v_table,'(recon_code,dataset_code)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,tran_value
  set v_sql = concat('create index idx_tran_value on ',v_table,'(recon_code,tran_value)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,excp_value
  set v_sql = concat('create index idx_excp_value on ',v_table,'(recon_code,excp_value)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create tranbrkp table
  set v_table = concat(v_table_prefix,'_tranbrkp');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_ttranbrkp where 1 = 2');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(tranbrkp_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add scheduler_gid
  set v_sql = concat('create index idx_scheduler_gid on ',v_table,'(scheduler_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add tran_gid
  set v_sql = concat('create index idx_tran_gid on ',v_table,'(tran_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code
  set v_sql = concat('create index idx_recon_code on ',v_table,'(recon_code)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,dataset_code
  set v_sql = concat('create index idx_dataset_code on ',v_table,'(recon_code,dataset_code)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,tran_value
  set v_sql = concat('create index idx_tran_value on ',v_table,'(recon_code,tran_value)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,excp_value
  set v_sql = concat('create index idx_excp_value on ',v_table,'(recon_code,excp_value)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create tranbrkpko table
  set v_table = concat(v_table_prefix,'_tranbrkpko');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_ttranbrkp where 1 = 2');

  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(tranbrkp_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add scheduler_gid
  set v_sql = concat('create index idx_scheduler_gid on ',v_table,'(scheduler_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add tran_gid
  set v_sql = concat('create index idx_tran_gid on ',v_table,'(tran_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code
  set v_sql = concat('create index idx_recon_code on ',v_table,'(recon_code)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,dataset_code
  set v_sql = concat('create index idx_dataset_code on ',v_table,'(recon_code,dataset_code)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,tran_value
  set v_sql = concat('create index idx_tran_value on ',v_table,'(recon_code,tran_value)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code,excp_value
  set v_sql = concat('create index idx_excp_value on ',v_table,'(recon_code,excp_value)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create ko table
  set v_table = concat(v_table_prefix,'_ko');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_tko where 1 = 2');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(ko_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add job_gid
  set v_sql = concat('create index idx_job_gid on ',v_table,'(job_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add ko_date
  set v_sql = concat('create index idx_ko_date on ',v_table,'(recon_code,ko_date)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add rule_code
  set v_sql = concat('create index idx_rule_code on ',v_table,'(recon_code,rule_code)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create kodtl table
  set v_table = concat(v_table_prefix,'_kodtl');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_tkodtl where 1 = 2');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(kodtl_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add ko_gid
  set v_sql = concat('create index idx_ko_gid on ',v_table,'(ko_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create koroundoff table
  set v_table = concat(v_table_prefix,'_koroundoff');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_tkoroundoff where 1 = 2');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(koroundoff_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add ko_gid
  set v_sql = concat('create index idx_ko_gid on ',v_table,'(ko_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add tran_gid
  set v_sql = concat('create index idx_tran_gid on ',v_table,'(tran_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add tranbrkp_gid
  set v_sql = concat('create index idx_tranbrkp_gid on ',v_table,'(tranbrkp_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create rule master
  set v_table = concat(v_table_prefix,'_rule');
  set v_sql = concat('create table ',v_table,' select * from recon_mst_trule where 1 = 2');

  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(rule_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create preprocess master
  set v_table = concat(v_table_prefix,'_preprocess');
  set v_sql = concat('create table ',v_table,' select * from recon_mst_tpreprocess where 1 = 2');

  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(preprocess_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create theme master
  set v_table = concat(v_table_prefix,'_theme');
  set v_sql = concat('create table ',v_table,' select * from recon_mst_ttheme where 1 = 2');

  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(theme_gid)');
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

  -- tranko table
  call pr_upd_job(v_job_gid,'P','Archiving tranko table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_tranko');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select * from ",v_tranko_table,"
    where recon_code = '",in_recon_code,"'
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

  -- rule table
  call pr_upd_job(v_job_gid,'P','Archiving rule table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_rule');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select * from ",v_rule_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);

  -- preprocess table
  call pr_upd_job(v_job_gid,'P','Archiving process table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_preprocess');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select * from ",v_preprocess_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);

  -- theme table
  call pr_upd_job(v_job_gid,'P','Archiving theme table...',@msg,@result);

  set v_table = concat(v_table_prefix,'_theme');

  set v_sql = concat("insert into ",v_table,"
    select z.* from (
    select * from ",v_theme_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N' LOCK IN SHARE MODE) as z");

  call pr_run_sql2(v_sql,@msg,@result);

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

      -- create archive dataset table
      set v_table = concat(v_table_prefix_code,'_',v_dataset_code);
      call pr_create_datasettable(v_archival_db_name,v_table,@msg3,@result3);

      -- to add archival db name (ex. table = db_name.table_name)
      set v_table = concat(v_table_prefix,'_',v_dataset_code);

	    call pr_upd_job(v_job_gid,'P',concat('Archiving ',v_dataset_name,'...'),@msg,@result);

      -- archive dataset table data
      set v_dataset_table = concat(v_dataset_db_name,v_dataset_code);

      set v_sql = concat("insert into ",v_table,"
        select z.* from (
        select * from ",v_dataset_table,"
        where delete_flag = 'N' LOCK IN SHARE MODE) as z");

      call pr_run_sql2(v_sql,@msg,@result);
		end loop dataset_loop;

		close dataset_cursor;
	end dataset_block;

	call pr_upd_job(v_job_gid,'C','Completed',@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;