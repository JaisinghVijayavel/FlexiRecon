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
  declare v_sql text default '';
  declare v_table text default '';
  declare v_table_prefix text default '';
  declare v_archival_code text default '';

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

  -- create tran table
  set v_table = concat(v_table_prefix,'_tran');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_ttran where 1 = 2');

  call pr_run_sql(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(tran_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add scheduler_gid
  set v_sql = concat('create index idx_scheduler_gid on ',v_table,'(scheduler_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code
  set v_sql = concat('create index idx_recon_code on ',v_table,'(recon_code)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,dataset_code
  set v_sql = concat('create index idx_dataset_code on ',v_table,'(recon_code,dataset_code)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,tran_value
  set v_sql = concat('create index idx_tran_value on ',v_table,'(recon_code,tran_value)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,excp_value
  set v_sql = concat('create index idx_excp_value on ',v_table,'(recon_code,excp_value)');
  call pr_run_sql(v_sql,@msg,@result);

  -- create tranko table
  set v_table = concat(v_table_prefix,'_tranko');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_ttran where 1 = 2');

  call pr_run_sql(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(tran_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add scheduler_gid
  set v_sql = concat('create index idx_scheduler_gid on ',v_table,'(scheduler_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code
  set v_sql = concat('create index idx_recon_code on ',v_table,'(recon_code)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,dataset_code
  set v_sql = concat('create index idx_dataset_code on ',v_table,'(recon_code,dataset_code)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,tran_value
  set v_sql = concat('create index idx_tran_value on ',v_table,'(recon_code,tran_value)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,excp_value
  set v_sql = concat('create index idx_excp_value on ',v_table,'(recon_code,excp_value)');
  call pr_run_sql(v_sql,@msg,@result);

  -- create tranbrkp table
  set v_table = concat(v_table_prefix,'_tranbrkp');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_ttranbrkp where 1 = 2');
  call pr_run_sql(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(tranbrkp_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add scheduler_gid
  set v_sql = concat('create index idx_scheduler_gid on ',v_table,'(scheduler_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add tran_gid
  set v_sql = concat('create index idx_tran_gid on ',v_table,'(tran_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code
  set v_sql = concat('create index idx_recon_code on ',v_table,'(recon_code)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,dataset_code
  set v_sql = concat('create index idx_dataset_code on ',v_table,'(recon_code,dataset_code)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,tran_value
  set v_sql = concat('create index idx_tran_value on ',v_table,'(recon_code,tran_value)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,excp_value
  set v_sql = concat('create index idx_excp_value on ',v_table,'(recon_code,excp_value)');
  call pr_run_sql(v_sql,@msg,@result);

  -- create tranbrkpko table
  set v_table = concat(v_table_prefix,'_tranbrkpko');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_ttranbrkp where 1 = 2');

  call pr_run_sql(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(tranbrkp_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add scheduler_gid
  set v_sql = concat('create index idx_scheduler_gid on ',v_table,'(scheduler_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add tran_gid
  set v_sql = concat('create index idx_tran_gid on ',v_table,'(tran_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code
  set v_sql = concat('create index idx_recon_code on ',v_table,'(recon_code)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,dataset_code
  set v_sql = concat('create index idx_dataset_code on ',v_table,'(recon_code,dataset_code)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,tran_value
  set v_sql = concat('create index idx_tran_value on ',v_table,'(recon_code,tran_value)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add recon_code,excp_value
  set v_sql = concat('create index idx_excp_value on ',v_table,'(recon_code,excp_value)');
  call pr_run_sql(v_sql,@msg,@result);

  -- create ko table
  set v_table = concat(v_table_prefix,'_ko');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_tko where 1 = 2');
  call pr_run_sql(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(ko_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add job_gid
  set v_sql = concat('create index idx_job_gid on ',v_table,'(job_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add ko_date
  set v_sql = concat('create index idx_ko_date on ',v_table,'(recon_code,ko_date)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add rule_code
  set v_sql = concat('create index idx_rule_code on ',v_table,'(recon_code,rule_code)');
  call pr_run_sql(v_sql,@msg,@result);

  -- create kodtl table
  set v_table = concat(v_table_prefix,'_kodtl');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_tkodtl where 1 = 2');
  call pr_run_sql(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(kodtl_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add ko_gid
  set v_sql = concat('create index idx_ko_gid on ',v_table,'(ko_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- create koroundoff table
  set v_table = concat(v_table_prefix,'_koroundoff');
  set v_sql = concat('create table ',v_table,' select * from recon_trn_tkoroundoff where 1 = 2');
  call pr_run_sql(v_sql,@msg,@result);

  -- add primary key
  set v_sql = concat('alter table ',v_table,' add primary key(koroundoff_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add ko_gid
  set v_sql = concat('create index idx_ko_gid on ',v_table,'(ko_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add tran_gid
  set v_sql = concat('create index idx_tran_gid on ',v_table,'(tran_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  -- add tranbrkp_gid
  set v_sql = concat('create index idx_tranbrkp_gid on ',v_table,'(tranbrkp_gid)');
  call pr_run_sql(v_sql,@msg,@result);

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;