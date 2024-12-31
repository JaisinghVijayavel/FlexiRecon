﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_reconarchival` $$
CREATE PROCEDURE `pr_set_reconarchival`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
begin

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';
	declare v_koroundoff_table text default '';

  declare v_sql text default '';
  declare v_table text default '';
  declare v_table_prefix text default '';
  declare v_archival_code text default '';

  declare v_condition text default '';
  declare v_archival_db_name text default '';
  declare v_recontype_code text default '';

  -- get archival db name
  set v_archival_db_name = fn_get_configvalue('archival_db_name');

  -- recon tran table
	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

	set v_tranko_table = 'recon_trn_ttranko';
	set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';

	set v_ko_table = 'recon_trn_tko';
	set v_kodtl_table = 'recon_trn_tkodtl';
	set v_koroundoff_table = 'recon_trn_tkoroundoff';

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

  -- generate archival code
  set v_archival_code = fn_get_autocode('RA');

  -- insert into archival table
  insert into recon_trn_treconarchival
  (
    recon_code,
    archival_code,
    archival_date,
    archival_by,
    active_status,
    insert_date,
    insert_by
  )
  select
    in_recon_code,
    v_archival_code,
    sysdate(),
    in_user_code,
    'Y',
    sysdate(),
    in_user_code;

  set v_table_prefix = concat(in_recon_code,'_',v_archival_code);

  if v_archival_db_name <> '' then
    set v_table_prefix = concat(v_archival_db_name,'.',v_table_prefix);
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

  -- transfer data
  -- tran table
  set v_table = concat(v_table_prefix,'_tran');

  set v_sql = concat("insert into ",v_table,"
    select * from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    ",v_condition,"
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);

  -- tranko table
  set v_table = concat(v_table_prefix,'_tranko');

  set v_sql = concat("insert into ",v_table,"
    select * from ",v_tranko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);

  -- tranbrkp table
  set v_table = concat(v_table_prefix,'_tranbrkp');

  set v_sql = concat("insert into ",v_table,"
    select * from ",v_tranbrkp_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);

  -- tranbrkpko table
  set v_table = concat(v_table_prefix,'_tranbrkpko');

  set v_sql = concat("insert into ",v_table,"
    select * from ",v_tranbrkpko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);

  -- ko table
  set v_table = concat(v_table_prefix,'_ko');

  set v_sql = concat("insert into ",v_table,"
    select * from ",v_ko_table,"
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);

  -- kodtl table
  set v_ko_table = concat(v_table_prefix,'_ko');
  set v_table = concat(v_table_prefix,'_kodtl');

  set v_sql = concat("insert into ",v_table,"
    select a.* from ",v_kodtl_table," as a 
    inner join ",v_ko_table," as b on a.ko_gid = b.ko_gid
      and b.delete_flag = 'N'
    where b.recon_code = '",in_recon_code,"'
    and a.delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);

  -- koroundoff table
  set v_table = concat(v_table_prefix,'_koroundoff');

  set v_sql = concat("insert into ",v_table,"
    select a.* from ",v_koroundoff_table," as a
    inner join ",v_ko_table," as b on a.ko_gid = b.ko_gid
      and b.delete_flag = 'N'
    where b.recon_code = '",in_recon_code,"'
    and a.delete_flag = 'N'");

  call pr_run_sql2(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;