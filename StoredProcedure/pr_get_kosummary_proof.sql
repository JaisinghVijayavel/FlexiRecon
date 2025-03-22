﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_kosummary_proof` $$
CREATE PROCEDURE `pr_get_kosummary_proof`(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  in in_conversion_type varchar(2),
  in in_dataset_formt varchar(25),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Muthu
    Created Date : 01-01-2025

    Updated By : Muthu
    updated Date : 22-03-2025

    Version : 3
  */

  declare v_rptsession_gid int default 0;
  declare v_rec_count int default 0;
  declare v_condition text default '';
  declare v_recontype text default '';

  declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';

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
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

	  set v_tranko_table = 'recon_trn_ttranko';
	  set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';

	  set v_ko_table = 'recon_trn_tko';
	  set v_kodtl_table = 'recon_trn_tkodtl';
  end if;

  drop temporary table if exists recon_tmp_tkodtl;
  drop temporary table if exists recon_tmp_treconcode;
  drop temporary table if exists recon_tmp_tkosumm;
  drop temporary table if exists recon_tmp_tkosumm1;

  select
    recontype_code into v_recontype
  from recon_mst_trecon
  where recon_code = in_recon_code
  and active_status = 'Y'
  and delete_flag = 'N';

  create temporary table recon_tmp_tkodtl
  (
    kodtl_gid int not null,
    recon_code varchar(32) default null,
    recon_name text default null,
    rule_code varchar(32) default null,
    rule_name text default null,
    rule_order decimal(9,2),
    tran_gid int not null default 0,
    dataset_code varchar(32) default null,
    tran_acc_mode char(1) default null,
    tran_mult tinyint not null default 0,
    manual_matchoff char(1) default null,
    ko_value double(15,2) default null,
    key idx_recon_code (recon_code),
    key idx_tran_gid (tran_gid),
    key idx_rule_order (rule_order),
    key idx_manual_matchoff (manual_matchoff),
    PRIMARY KEY (kodtl_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_treconcode
  (
    recon_code varchar(32) not null,
    recon_name text default null,
    PRIMARY KEY (recon_code)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tkosumm
  (
    kosumm_gid int not null AUTO_INCREMENT,
    recon_code varchar(32) default null,
    dataset_code varchar(32) default null,
    rec_slno int(10) NOT NULL default 0,
    row_desc text default null,
    dr_count int default null,
    dr_value double(15,2) default null,
    cr_count int default null,
    cr_value double(15,2) default null,
    tot_count int default null,
    tot_value double(15,2) default null,
    fontbold_flag char(1) not null default 'N',
    backcolor_flag char(1) default 'N',
    forecolor varchar(32) default null,
    backcolor varchar(32) default null,
    PRIMARY KEY (kosumm_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tkosumm1
  (
    kosumm_gid int not null,
    recon_code varchar(32) default null,
    dataset_code varchar(32) default null,
    rec_slno int(10) NOT NULL default 0,
    row_desc text default null,
    dr_count int default null,
    dr_value double(15,2) default null,
    cr_count int default null,
    cr_value double(15,2) default null,
    tot_count int default null,
    tot_value double(15,2) default null,
    fontbold_flag char(1) not null default 'N',
    backcolor_flag char(1) default 'N',
    forecolor varchar(32) default null,
    backcolor varchar(32) default null,
    PRIMARY KEY (kosumm_gid)
  ) ENGINE = MyISAM;

  insert into recon_tmp_treconcode
    select a.* from (
    select recon_code,recon_name from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and active_status = 'Y'
    and delete_flag = 'N'
    LOCK IN SHARE MODE) as a;

  -- generate condition
  set v_condition = concat(" and a.recon_code '",in_recon_code,"'
    and a.ko_date >= '",date_format(in_period_from,'%Y-%m-%d'),"'
    and a.ko_date <= '",date_format(in_period_to,'%Y-%m-%d'),"' ");

  set v_sql = concat("
  insert into recon_tmp_tkodtl
  (
    kodtl_gid,recon_code,tran_gid,ko_value,manual_matchoff,recon_name,dataset_code,tran_acc_mode,tran_mult,rule_name,rule_order
  )
  select a.* from (
  select
    d.kodtl_gid,r.recon_code,d.tran_gid,d.ko_value,k.manual_matchoff,
    r.recon_name,t.dataset_code,if(d.ko_mult=-1,'D','C'),d.ko_mult,e.rule_name,e.rule_order
  from recon_tmp_treconcode as r
  inner join ",v_ko_table," as k on r.recon_code = k.recon_code
  inner join ",v_kodtl_table," as d on k.ko_gid = d.ko_gid and d.delete_flag = 'N'
  inner join ",v_tran_table," as t on d.tran_gid = t.tran_gid and t.delete_flag = 'N'
  left join recon_mst_trule as e on k.rule_code = e.rule_code and e.delete_flag = 'N'
  where k.ko_date >= '",cast(in_period_from as nchar),"'
  and k.ko_date <= '",cast(in_period_to as nchar),"'
  and k.delete_flag = 'N'
  order by e.rule_order
  LOCK IN SHARE MODE) as a");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set v_sql = concat("
  insert into recon_tmp_tkodtl
  (
    kodtl_gid,recon_code,tran_gid,ko_value,manual_matchoff,recon_name,dataset_code,tran_acc_mode,tran_mult,rule_name,rule_order
  )
  select a.* from (
  select
    d.kodtl_gid,r.recon_code,d.tran_gid,d.ko_value,k.manual_matchoff,
    r.recon_name,t.dataset_code,if(d.ko_mult=-1,'D','C'),d.ko_mult,e.rule_name,e.rule_order
  from recon_tmp_treconcode as r
  inner join ",v_ko_table," as k on r.recon_code = k.recon_code
  inner join ",v_kodtl_table," as d on k.ko_gid = d.ko_gid and d.delete_flag = 'N'
  inner join ",v_tranko_table," as t on d.tran_gid = t.tran_gid and t.delete_flag = 'N'
  left join recon_mst_trule as e on k.rule_code = e.rule_code and e.delete_flag = 'N'
  where k.ko_date >= '",cast(in_period_from as nchar),"'
  and k.ko_date <= '",cast(in_period_to as nchar),"'
  and k.delete_flag = 'N'
  order by e.rule_order
  LOCK IN SHARE MODE) as a");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- insert in ko summary
  -- insert recon_name
  set @row_slno = 0;

  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    row_desc,
    fontbold_flag,
    backcolor_flag,
    forecolor,
    backcolor
  )
  select
    distinct @row_slno = @row_slno + 1,
    recon_code,
    recon_name,
    'Y',
    'Y',
    'Blue',
    'White'
  from recon_tmp_tkodtl;

  -- insert rule based
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    dr_count,
    dr_value,
    cr_count,
    cr_value,
    tot_count,
    tot_value
  )
  select
    @row_slno = @row_slno + 1,
    recon_code,
    dataset_code,
    concat('  ',rule_name),
    count(distinct if(tran_acc_mode = 'D',tran_gid,null)) as dr_count,
    sum(if(tran_acc_mode = 'D',ko_value,0)) as dr_value,
    count(distinct if(tran_acc_mode = 'C',tran_gid,null)) as cr_count,
    sum(if(tran_acc_mode = 'C',ko_value,0)) as cr_value,
    count(distinct tran_gid),
    abs(sum(ko_value*tran_mult))
  from recon_tmp_tkodtl
  where 1=1 and manual_matchoff = 'N'
  group by recon_code,rule_order;

  -- insert manual
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    dr_count,
    dr_value,
    cr_count,
    cr_value,
    tot_count,
    tot_value
  )
  select
    @row_slno = @row_slno + 1,
    recon_code,
    dataset_code,
    '  Manual KO' as matchoff_type,
    count(distinct if(tran_acc_mode = 'D',tran_gid,null)) as dr_count,
    sum(if(tran_acc_mode = 'D',ko_value,0)) as dr_value,
    count(distinct if(tran_acc_mode = 'C',tran_gid,null)) as cr_count,
    sum(if(tran_acc_mode = 'C',ko_value,0)) as cr_value,
    count(distinct tran_gid),
    abs(sum(ko_value*tran_mult))
    -- sum(ko_value)
  from recon_tmp_tkodtl
  where manual_matchoff = 'Y'
  -- group by recon_code,dataset_code,matchoff_type;
  group by recon_code,matchoff_type;

  insert into recon_tmp_tkosumm1
    select * from recon_tmp_tkosumm where dr_count is not null;

  -- reconaccwise total
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    dr_count,
    dr_value,
    cr_count,
    cr_value,
    tot_count,
    tot_value,
    fontbold_flag,
    backcolor_flag,
    forecolor,
    backcolor
  )
  select
    @row_slno = @row_slno + 1,
    recon_code,
    dataset_code,
    'Sub Total',
    ifnull(sum(dr_count),0) as dr_count,
    ifnull(sum(dr_value),0) as dr_value,
    ifnull(sum(cr_count),0) as cr_count,
    ifnull(sum(cr_value),0) as cr_value,
    ifnull(sum(dr_count),0)+ifnull(sum(cr_count),0),
    abs(sum(ifnull(dr_value,0))-sum(ifnull(cr_value,0))),
    'Y',
    'Y',
    'Red',
    'Yellow'
  from recon_tmp_tkosumm1
  group by recon_code;

  -- insert blank line
  insert into recon_tmp_tkosumm
  (
    rec_slno,
    recon_code,
    dataset_code,
    row_desc,
    backcolor,
    forecolor
  )
  select
    @row_slno = @row_slno + 1,
    recon_code,
    'XXX9999999999999999',
    '',
    'White',
    'White'
  from recon_tmp_tkosumm1
  group by recon_code;

  -- return result
  select
    row_desc as 'Row Labels',
    ifnull(dr_count,0) as 'Dr Count',
    ifnull(dr_value,0) as 'Dr Value',
    (select fn_get_currency_format(dr_value,'INR',in_conversion_type,'MIS')) as 'Formal Dr Value',
    ifnull(cr_count,0) as 'Cr Count',
    ifnull(cr_value,0) as 'Cr Value',
    (select fn_get_currency_format(cr_value,'INR',in_conversion_type,'MIS')) as 'Formal Cr Value',
    /*tot_count as 'Total Count',
    format(tot_value,2,'en_IN') as 'Total Value',*/
    ifnull(backcolor,'White') as backcolor,
    ifnull(forecolor,'Black') as forecolor,
    v_recontype as recontype
  from recon_tmp_tkosumm
  order by recon_code,dataset_code,rec_slno;

  drop temporary table if exists recon_tmp_treconcode;
  drop temporary table if exists recon_tmp_tkosumm;
  drop temporary table if exists recon_tmp_tkosumm1;
  drop temporary table if exists recon_tmp_tko;
end $$
DELIMITER ;