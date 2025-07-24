DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_recontable` $$
CREATE PROCEDURE `pr_set_recontable`
(
  in in_recon_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  /*
    Created By : Vijayavel
    Created Date : 18-07-2025

    Updated By : Vijayavel
    updated Date : 18-07-2025

    Version : 1
  */

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

  declare v_condition text default '';
  declare v_concurrent_ko_flag text default '';

  declare v_max_tran_gid int default 0;
  declare v_max_tranbrkp_gid int default 0;
  declare v_max_ko_gid int default 0;
  declare v_max_kodtl_gid int default 0;
  declare v_max_koroundoff_gid int default 0;
  declare v_db_name text default '';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag <> 'Y' then
    set out_msg = 'Failed ! Concurrent flag not set !';
    set out_result = 0;
    leave me;
  end if;

  -- recon tran table
	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

	set v_tranko_table = 'recon_trn_ttranko';
	set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';

	set v_ko_table = 'recon_trn_tko';
	set v_kodtl_table = 'recon_trn_tkodtl';
	set v_koroundoff_table = 'recon_trn_tkoroundoff';

  set v_table_prefix = in_recon_code;

  -- get db name
  select database() into v_db_name;

  -- get auto increment value
  -- tran table
  select
    AUTO_INCREMENT into v_max_tran_gid
  from information_schema.TABLES
  where TABLE_SCHEMA = v_db_name
  and TABLE_NAME = 'recon_trn_ttran';

  set v_max_tran_gid = ifnull(v_max_tran_gid,0) + 1;

  -- tranbrkp table
  select
    AUTO_INCREMENT into v_max_tranbrkp_gid
  from information_schema.TABLES
  where TABLE_SCHEMA = v_db_name
  and TABLE_NAME = 'recon_trn_ttranbrkp';

  set v_max_tranbrkp_gid = ifnull(v_max_tranbrkp_gid,0) + 1;

  -- ko table
  select
    AUTO_INCREMENT into v_max_ko_gid
  from information_schema.TABLES
  where TABLE_SCHEMA = v_db_name
  and TABLE_NAME = 'recon_trn_tko';

  set v_max_ko_gid = ifnull(v_max_ko_gid,0) + 1;

  -- kodtl table
  select
    AUTO_INCREMENT into v_max_kodtl_gid
  from information_schema.TABLES
  where TABLE_SCHEMA = v_db_name
  and TABLE_NAME = 'recon_trn_tkodtl';

  set v_max_kodtl_gid = ifnull(v_max_kodtl_gid,0) + 1;

  -- kodtl table
  select
    AUTO_INCREMENT into v_max_koroundoff_gid
  from information_schema.TABLES
  where TABLE_SCHEMA = v_db_name
  and TABLE_NAME = 'recon_trn_tkoroundoff';

  set v_max_koroundoff_gid = ifnull(v_max_koroundoff_gid,0) + 1;

  -- create tran table
  set v_table = concat(v_table_prefix,'_tran');
  set v_sql = concat('create table ',v_table,' like recon_trn_ttran');

  call pr_run_sql2(v_sql,@msg,@result);

  -- set auto increment
  set v_sql = concat('alter table ',v_table,' AUTO_INCREMENT = ',cast(v_max_tran_gid as nchar));
  call pr_run_sql2(v_sql,@msg,@result);

  -- create tranko table
  set v_table = concat(v_table_prefix,'_tranko');
  set v_sql = concat('create table ',v_table,' like recon_trn_ttranko');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create tranbrkp table
  set v_table = concat(v_table_prefix,'_tranbrkp');
  set v_sql = concat('create table ',v_table,' like recon_trn_ttranbrkp');
  call pr_run_sql2(v_sql,@msg,@result);

  -- set auto increment
  set v_sql = concat('alter table ',v_table,' AUTO_INCREMENT = ',cast(v_max_tranbrkp_gid as nchar));
  call pr_run_sql2(v_sql,@msg,@result);

  -- create tranbrkpko table
  set v_table = concat(v_table_prefix,'_tranbrkpko');
  set v_sql = concat('create table ',v_table,' like recon_trn_ttranbrkpko');
  call pr_run_sql2(v_sql,@msg,@result);

  -- create ko table
  set v_table = concat(v_table_prefix,'_ko');
  set v_sql = concat('create table ',v_table,' like recon_trn_tko');
  call pr_run_sql2(v_sql,@msg,@result);

  -- set auto increment
  set v_sql = concat('alter table ',v_table,' AUTO_INCREMENT = ',cast(v_max_ko_gid as nchar));
  call pr_run_sql2(v_sql,@msg,@result);

  -- create kodtl table
  set v_table = concat(v_table_prefix,'_kodtl');
  set v_sql = concat('create table ',v_table,' like recon_trn_tkodtl');
  call pr_run_sql2(v_sql,@msg,@result);

  -- set auto increment
  set v_sql = concat('alter table ',v_table,' AUTO_INCREMENT = ',cast(v_max_kodtl_gid as nchar));
  call pr_run_sql2(v_sql,@msg,@result);

  -- create koroundoff table
  set v_table = concat(v_table_prefix,'_koroundoff');
  set v_sql = concat('create table ',v_table,' like recon_trn_tkoroundoff');
  call pr_run_sql2(v_sql,@msg,@result);

  -- set auto increment
  set v_sql = concat('alter table ',v_table,' AUTO_INCREMENT = ',cast(v_max_koroundoff_gid as nchar));
  call pr_run_sql2(v_sql,@msg,@result);

  /*
  -- create tran table
  set v_table = concat(v_table_prefix,'_tran');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_ttran where 1 = 2');

  call pr_run_sql2(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(tran_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- change primary key to auto_increment
  set v_sql = concat('alter table ',v_table,' change column tran_gid tran_gid int unsigned not null AUTO_INCREMENT');
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

  -- change primary key to auto_increment
  set v_sql = concat('alter table ',v_table,' change column tranbrkp_gid tranbrkp_gid int unsigned not null AUTO_INCREMENT');
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

  -- change primary key to auto_increment
  set v_sql = concat('alter table ',v_table,' change column ko_gid ko_gid int unsigned not null AUTO_INCREMENT');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add job_gid
  set v_sql = concat('create index idx_job_gid on ',v_table,'(job_gid)');
  call pr_run_sql2(v_sql,@msg,@result);

  -- add recon_code
  set v_sql = concat('create index idx_recon_code on ',v_table,'(recon_code)');
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

  -- change primary key to auto_increment
  set v_sql = concat('alter table ',v_table,' change column kodtl_gid kodtl_gid int unsigned not null AUTO_INCREMENT');
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

  -- change primary key to auto_increment
  set v_sql = concat('alter table ',v_table,' change column koroundoff_gid koroundoff_gid int unsigned not null AUTO_INCREMENT');
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
  */

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;