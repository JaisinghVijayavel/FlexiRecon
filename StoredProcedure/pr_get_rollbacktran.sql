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
  declare v_next_tran_date date;

  set v_next_tran_date = date_add(in_tran_date,interval 1 day);

  drop temporary table if exists recon_tmp_ttran;
  drop temporary table if exists recon_tmp_ttranbrkp;
  drop temporary table if exists recon_tmp_tko;
  drop temporary table if exists recon_tmp_tkodtl;
  drop temporary table if exists recon_tmp_ttrangid;

  drop table if exists recon_tmp_ttran;
  drop table if exists recon_tmp_ttranbrkp;
  drop table if exists recon_tmp_tko;
  drop table if exists recon_tmp_tkodtl;

  create /*temporary*/ table recon_tmp_ttran select * from recon_trn_ttran where 1 = 2;
  alter table recon_tmp_ttran ENGINE = MyISAM;
  alter table recon_tmp_ttran add primary key(tran_gid);
  create index idx_excp_value on recon_tmp_ttran(excp_value);
  create index idx_tran_date on recon_tmp_ttran(tran_date);
  create index idx_dataset_code on recon_tmp_ttran(recon_code,dataset_code);

  create /*temporary*/ table recon_tmp_ttranbrkp select * from recon_trn_ttranbrkp where 1 = 2;
  alter table recon_tmp_ttranbrkp ENGINE = MyISAM;
  alter table recon_tmp_ttranbrkp add primary key(tranbrkp_gid);
  create index idx_tran_gid on recon_tmp_ttranbrkp(tran_gid);
  create index idx_excp_value on recon_tmp_ttranbrkp(excp_value);
  create index idx_tran_date on recon_tmp_ttranbrkp(tran_date);
  create index idx_dataset_code on recon_tmp_ttranbrkp(recon_code,dataset_code);

  create /*temporary*/ table recon_tmp_tko select * from recon_trn_tko where 1 = 2;
  alter table recon_tmp_tko ENGINE = MyISAM;
  alter table recon_tmp_tko add primary key(ko_gid);
  create index idx_ko_date on recon_tmp_tko(ko_date);

  create /*temporary*/ table recon_tmp_tkodtl select * from recon_trn_tkodtl where 1 = 2;
  alter table recon_tmp_tkodtl ENGINE = MyISAM;
  alter table recon_tmp_tkodtl add primary key(kodtl_gid);
  create index idx_ko_gid on recon_tmp_tkodtl(ko_gid);
  create index idx_tran_gid on recon_tmp_tkodtl(tran_gid);
  create index idx_tranbrkp_gid on recon_tmp_tkodtl(tranbrkp_gid);

  CREATE temporary TABLE recon_tmp_ttrangid(
    tran_gid int unsigned NOT NULL,
    ko_value double(15,2) not null default 0,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  -- tran table
  insert into recon_tmp_ttran
    select * from recon_trn_ttran
    where recon_code = in_recon_code
    and tran_date < v_next_tran_date
    and delete_flag = 'N';

  -- tranbrkp table
  insert into recon_tmp_ttranbrkp
    select * from recon_trn_ttranbrkp
    where recon_code = in_recon_code
    and tran_date < v_next_tran_date
    and delete_flag = 'N';

  -- ko table
  insert into recon_tmp_tko
    select * from recon_trn_tko
    where ko_date >= v_next_tran_date
    and recon_code = in_recon_code
    and delete_flag = 'N';

  -- kodtl table
  insert into recon_tmp_tkodtl
    select b.* from recon_trn_tko as a
    inner join recon_trn_tkodtl as b on a.ko_gid = b.ko_gid and b.delete_flag = 'N'
    where a.ko_date >= v_next_tran_date
    and a.recon_code = in_recon_code
    and a.delete_flag = 'N';

  insert into recon_tmp_ttrangid (tran_gid,ko_value)
    select tran_gid,sum(ko_value*ko_mult) from recon_tmp_tkodtl
    group by tran_gid;

  -- insert knockoff transactions
  insert into recon_tmp_ttran
    select b.* from recon_tmp_ttrangid as a
    inner join recon_trn_ttranko as b on a.tran_gid = b.tran_gid;

  insert into recon_tmp_ttranbrkp
    select b.* from recon_tmp_tkodtl as a
    inner join recon_trn_ttranbrkpko as b on a.tranbrkp_gid = b.tranbrkp_gid
    where b.tranbrkp_gid > 0;

  -- update in tran table
  update recon_tmp_ttran as a
  inner join recon_tmp_ttrangid as b on a.tran_gid = b.tran_gid 
  set
    a.ko_gid = 0,
    a.ko_date = null,
    a.excp_value = a.excp_value + (b.ko_value*a.tran_mult);

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