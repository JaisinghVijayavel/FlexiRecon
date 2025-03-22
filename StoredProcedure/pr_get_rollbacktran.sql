DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_rollbacktran` $$
CREATE PROCEDURE `pr_get_rollbacktran`
(
  in in_recon_code varchar(32),
  in in_tran_date date,
  out out_msg text,
  out out_result int
)
me:begin
	declare v_sql text default '';
  declare v_next_tran_date date;

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';
	declare v_koroundoff_table text default '';

  declare v_concurrent_ko_flag text default '';

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

  -- get next tran date
  set v_next_tran_date = date_add(in_tran_date,interval 1 day);

  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttranbrkp;
  drop temporary table if exists recon_tmp_tko;
  drop temporary table if exists recon_tmp_tkodtl;
  drop temporary table if exists recon_tmp_tkoroundoff;
  drop temporary table if exists recon_tmp_ttrangid;

  /*
  drop table if exists recon_tmp_ttran;
  drop table if exists recon_tmp_ttranbrkp;
  drop table if exists recon_tmp_tko;
  drop table if exists recon_tmp_tkodtl;
  */

  create temporary table recon_tmp_ttran select * from recon_trn_ttran where 1 = 2;
  alter table recon_tmp_ttran add primary key(tran_gid);
  create index idx_excp_value on recon_tmp_ttran(excp_value);
  create index idx_tran_date on recon_tmp_ttran(tran_date);
  create index idx_dataset_code on recon_tmp_ttran(recon_code,dataset_code);
  alter table recon_tmp_ttran ENGINE = MyISAM;

  create temporary table recon_tmp_ttranbrkp select * from recon_trn_ttranbrkp where 1 = 2;
  alter table recon_tmp_ttranbrkp add primary key(tranbrkp_gid);
  create index idx_tran_gid on recon_tmp_ttranbrkp(tran_gid);
  create index idx_excp_value on recon_tmp_ttranbrkp(excp_value);
  create index idx_tran_date on recon_tmp_ttranbrkp(tran_date);
  create index idx_dataset_code on recon_tmp_ttranbrkp(recon_code,dataset_code);
  alter table recon_tmp_ttranbrkp ENGINE = MyISAM;

  create temporary table recon_tmp_tko select * from recon_trn_tko where 1 = 2;
  alter table recon_tmp_tko add primary key(ko_gid);
  create index idx_ko_date on recon_tmp_tko(ko_date);
  alter table recon_tmp_tko ENGINE = MyISAM;

  create temporary table recon_tmp_tkodtl select * from recon_trn_tkodtl where 1 = 2;
  alter table recon_tmp_tkodtl add primary key(kodtl_gid);
  create index idx_ko_gid on recon_tmp_tkodtl(ko_gid);
  create index idx_tran_gid on recon_tmp_tkodtl(tran_gid);
  create index idx_tranbrkp_gid on recon_tmp_tkodtl(tranbrkp_gid);
  alter table recon_tmp_tkodtl ENGINE = MyISAM;

  create temporary table recon_tmp_tkoroundoff select * from recon_trn_tkoroundoff where 1 = 2;
  alter table recon_tmp_tkoroundoff add primary key(koroundoff_gid);
  create index idx_ko_gid on recon_tmp_tkoroundoff(ko_gid);
  create index idx_tran_gid on recon_tmp_tkoroundoff(tran_gid);
  create index idx_tranbrkp_gid on recon_tmp_tkoroundoff(tranbrkp_gid);
  alter table recon_tmp_tkoroundoff ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_ttrangid(
    tran_gid int unsigned NOT NULL,
    ko_value double(15,2) not null default 0,
    roundoff_value double(15,2) not null default 0,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  -- tran table
  set v_sql = concat("
  insert into recon_tmp_ttran
  select z.* from
  (
    select * from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and tran_date < '",cast(v_next_tran_date as nchar),"'
    and delete_flag = 'N'
    LOCK IN SHARE MODE
  ) as z");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- tranbrkp table
  set v_sql = concat("
  insert into recon_tmp_ttranbrkp
  select z.* from
  (
    select * from ",v_tranbrkp_table,"
    where recon_code = '",in_recon_code,"'
    and tran_date < '",cast(v_next_tran_date as nchar),"'
    and delete_flag = 'N'
    LOCK IN SHARE MODE
  ) as z");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- ko table
  /*
  insert into recon_tmp_tko
    select * from recon_trn_tko
    where ko_date >= v_next_tran_date
    and recon_code = in_recon_code
    and delete_flag = 'N';
  */

  -- kodtl table
  set v_sql = concat("
  insert into recon_tmp_tkodtl
  select z.* from
  (
    select b.* from ",v_ko_table," as a
    inner join ",v_kodtl_table," as b on a.ko_gid = b.ko_gid and b.delete_flag = 'N'
    where a.ko_date >= '",cast(v_next_tran_date as nchar),"'
    and a.recon_code = '",in_recon_code,"'
    and a.delete_flag = 'N'
    LOCK IN SHARE MODE
  ) as z");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- koroundoff table
  set v_sql = concat("
  insert into recon_tmp_tkoroundoff
  select z.* from
  (
    select b.* from ",v_ko_table," as a
    inner join ",v_koroundoff_table," as b on a.ko_gid = b.ko_gid and b.delete_flag = 'N'
    where a.ko_date >= '",cast(v_next_tran_date as nchar),"'
    and a.recon_code = '",in_recon_code,"'
    and a.delete_flag = 'N'
    LOCK IN SHARE MODE
  ) as z");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
  insert into recon_tmp_ttrangid (tran_gid,ko_value,roundoff_value)
  select z.* from
  (
    select
      a.tran_gid,
      sum(a.ko_value*a.ko_mult),
      sum(ifnull(b.roundoff_value,0)) as roundoff_value
    from ",v_kodtl_table," as a
		left join ",v_koroundoff_table," as b on a.ko_gid = b.ko_gid
			and a.tran_gid = b.tran_gid
			and a.tranbrkp_gid = b.tranbrkp_gid
			and b.delete_flag = 'N'
    group by a.tran_gid
    LOCK IN SHARE MODE
  ) as z");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- insert knockoff transactions
  set v_sql = concat("
  insert into recon_tmp_ttran
  select z.* from
  (
    select b.* from recon_tmp_ttrangid as a
    inner join ",v_tranko_table," as b on a.tran_gid = b.tran_gid
    LOCK IN SHARE MODE
  ) as z");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
  insert into recon_tmp_ttranbrkp
  select z.* from
  (
    select b.* from recon_tmp_tkodtl as a
    inner join ",v_tranbrkpko_table," as b on a.tranbrkp_gid = b.tranbrkp_gid
    where b.tranbrkp_gid > 0
    LOCK IN SHARE MODE
  ) as z");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- update in tran table
  update recon_tmp_ttran as a
  inner join recon_tmp_ttrangid as b on a.tran_gid = b.tran_gid
  set
    a.ko_gid = 0,
    a.ko_date = null,
    a.excp_value = a.excp_value + (b.ko_value*a.tran_mult),
    a.roundoff_value = a.roundoff_value - b.roundoff_value;

  -- update in tranbrkp table
  update recon_tmp_ttranbrkp as a
  inner join recon_tmp_tkodtl as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid and b.delete_flag = 'N'
  set
    a.ko_gid = 0,
    a.ko_date = null,
    a.excp_value = a.tran_value
  where b.tranbrkp_gid > 0;

  set out_msg = 'Success';
  set out_result = 1;

  drop temporary table if exists recon_tmp_ttrangid;
end $$

DELIMITER ;