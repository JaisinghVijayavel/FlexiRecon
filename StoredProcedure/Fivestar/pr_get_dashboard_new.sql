DELIMITER $$
DROP PROCEDURE IF EXISTS `pr_get_dashboard_new` $$

CREATE PROCEDURE `pr_get_dashboard_new`(
  in in_recon_gid text,
  in in_period_from date,
  in in_period_to date,
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  

  declare v_recon_count int default 0;
  declare v_acc_count int default 0;
  declare v_tran_count int default 0;
  declare v_count int default 0;
  declare v_ko_count int default 0;
  declare v_ko_manual_count int default 0;
  declare v_ko_system_count int default 0;
  declare v_ko_partialexcp_count int default 0;
  declare v_ko_zeroexcp_count int default 0;
  declare v_openingexcp_count int default 0;
  declare v_excp_count int default 0;

  drop temporary table if exists recon_tmp_trecongid;

  create temporary table recon_tmp_trecongid
  (
    recon_gid int(10) unsigned NOT NULL,
    PRIMARY KEY (recon_gid)
  );

  if in_recon_gid = '0' then
    insert into recon_tmp_trecongid
      select recon_gid from recon_mst_trecon
      where active_status = 'Y'
      and period_from <= curdate()
      and (period_to >= curdate()
      or until_active_flag = 'Y')
      and delete_flag = 'N';
  else
    set @sql = concat("insert into recon_tmp_trecongid
      select recon_gid from recon_mst_trecon
      where recon_gid in (",in_recon_gid,")
      and active_status = 'Y'
      and period_from <= curdate()
      and (period_to >= curdate()
      or until_active_flag = 'Y')
      and delete_flag = 'N'");

    call pr_run_sql(@sql,@msg,@result);
  end if;

  -- recon count
  select count(*) into v_recon_count from recon_tmp_trecongid;

  -- acc count
  select count(distinct b.acc_no) into v_acc_count from recon_tmp_trecongid as r
  inner join recon_mst_treconacc as b on r.recon_gid = b.recon_gid and b.delete_flag = 'N';

  -- opening exception
  select count(*) into v_openingexcp_count from recon_tmp_trecongid as r
  inner join recon_trn_ttran as t on r.recon_gid = t.recon_gid
    and t.tran_date < in_period_from
    and t.excp_amount > 0
    and t.delete_flag = 'N';

  set v_openingexcp_count = ifnull(v_openingexcp_count,0);

  -- transaction count
  select count(*) into v_count from recon_tmp_trecongid as r
  inner join recon_trn_ttran as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N';

  set v_tran_count = v_tran_count + ifnull(v_count,0);

  select count(*) into v_count from recon_tmp_trecongid as r
  inner join recon_trn_ttranko as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N';

  set v_tran_count = v_tran_count + ifnull(v_count,0);

  -- exception count
  select count(*) into v_count from recon_tmp_trecongid as r
  inner join recon_trn_ttran as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.excp_amount > 0
    and t.delete_flag = 'N';

  set v_excp_count = v_excp_count + ifnull(v_count,0);

  -- ko count
  select
    count(distinct t.tran_gid)
  into
    v_count
  from recon_tmp_trecongid as r
  inner join recon_trn_ttran as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N'
  inner join recon_trn_tkodtl as k on t.tran_gid = k.tran_gid
    and k.delete_flag = 'N';

  set v_ko_count = v_ko_count + ifnull(v_count,0);

  select
    count(distinct t.tran_gid)
  into
    v_count
  from recon_tmp_trecongid as r
  inner join recon_trn_ttranko as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N'
  inner join recon_trn_tkodtl as k on t.tran_gid = k.tran_gid
    and k.delete_flag = 'N';

  set v_ko_count = v_ko_count + ifnull(v_count,0);

  -- ko_system_count
  select
    count(distinct t.tran_gid)
  into
    v_count
  from recon_tmp_trecongid as r
  inner join recon_trn_ttran as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N'
  inner join recon_trn_tkodtl as k on t.tran_gid = k.tran_gid
    and k.delete_flag = 'N'
  inner join recon_trn_tko as h on k.ko_gid = h.ko_gid
    and h.manual_matchoff = 'N'
    and h.delete_flag = 'N';

  set v_ko_system_count = v_ko_system_count + ifnull(v_count,0);

  select
    count(distinct t.tran_gid)
  into
    v_count
  from recon_tmp_trecongid as r
  inner join recon_trn_ttranko as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N'
  inner join recon_trn_tkodtl as k on t.tran_gid = k.tran_gid
    and k.delete_flag = 'N'
  inner join recon_trn_tko as h on k.ko_gid = h.ko_gid
    and h.manual_matchoff = 'N'
    and h.delete_flag = 'N';

  set v_ko_system_count = v_ko_system_count + ifnull(v_count,0);

  -- ko_manual_count
  select
    count(distinct t.tran_gid)
  into
    v_count
  from recon_tmp_trecongid as r
  inner join recon_trn_ttran as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N'
  inner join recon_trn_tkodtl as k on t.tran_gid = k.tran_gid
    and k.delete_flag = 'N'
  inner join recon_trn_tko as h on k.ko_gid = h.ko_gid
    and h.manual_matchoff = 'Y'
    and h.delete_flag = 'N';

  set v_ko_manual_count = v_ko_manual_count + ifnull(v_count,0);

  select
    count(distinct t.tran_gid)
  into
    v_count
  from recon_tmp_trecongid as r
  inner join recon_trn_ttranko as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.delete_flag = 'N'
  inner join recon_trn_tkodtl as k on t.tran_gid = k.tran_gid
    and k.delete_flag = 'N'
  inner join recon_trn_tko as h on k.ko_gid = h.ko_gid
    and h.manual_matchoff = 'Y'
    and h.delete_flag = 'N';

  set v_ko_manual_count = v_ko_manual_count + ifnull(v_count,0);

  -- v_ko_zeroexcp_count
  select
    count(distinct t.tran_gid)
  into
    v_count
  from recon_tmp_trecongid as r
  inner join recon_trn_ttranko as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.excp_amount = 0
    and t.delete_flag = 'N'
  inner join recon_trn_tkodtl as k on t.tran_gid = k.tran_gid
    and k.delete_flag = 'N'
  inner join recon_trn_tko as h on k.ko_gid = h.ko_gid
    and h.delete_flag = 'N';

  set v_ko_zeroexcp_count = v_ko_zeroexcp_count + ifnull(v_count,0);

  select
    count(distinct t.tran_gid)
  into
    v_count
  from recon_tmp_trecongid as r
  inner join recon_trn_ttran as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.excp_amount = 0
    and t.delete_flag = 'N'
  inner join recon_trn_tkodtl as k on t.tran_gid = k.tran_gid
    and k.delete_flag = 'N'
  inner join recon_trn_tko as h on k.ko_gid = h.ko_gid
    and h.delete_flag = 'N';

  set v_ko_zeroexcp_count = v_ko_zeroexcp_count + ifnull(v_count,0);

  -- v_ko_partialexcp_count
  select
    count(distinct t.tran_gid)
  into
    v_count
  from recon_tmp_trecongid as r
  inner join recon_trn_ttran as t on r.recon_gid = t.recon_gid
    and t.tran_date >= in_period_from
    and t.tran_date <= in_period_to
    and t.excp_amount > 0
    and t.delete_flag = 'N'
  inner join recon_trn_tkodtl as k on t.tran_gid = k.tran_gid
    and k.delete_flag = 'N'
  inner join recon_trn_tko as h on k.ko_gid = h.ko_gid
    and h.delete_flag = 'N';

  set v_ko_partialexcp_count = v_ko_partialexcp_count + ifnull(v_count,0);


  select  v_recon_count          as recon_count,
          v_acc_count            as acc_count,
          v_tran_count           as tran_count,
          v_ko_count             as ko_count,
          v_ko_system_count      as ko_system_count,
          v_ko_count-v_ko_system_count      as ko_manual_count,
          v_excp_count           as excp_count,
          v_openingexcp_count    as opening_excp_count,
          v_ko_zeroexcp_count    as ko_zeroexcp_count,
          v_ko_partialexcp_count as ko_partialexcp_count;

  select
    '' as ko_month,
    0 as manual_ko_count,
    0 as system_ko_count,
    0 as ko_count,
    '' as ko_month1;

  set v_count = v_excp_count;

  if v_count = 0 then
    set v_count = 1;
  end if;

  select
    ag.aging_desc,ifnull(ex.excp_count,0) as excp_count,ifnull(ex.excp_percent,0) as excp_percent
  from recon_mst_taging as ag
  left join
  (
    select
      c.aging_gid,
      c.aging_desc,
      sum(if(t.tran_gid is null,0,1)) as excp_count,
      cast((count(*)/v_count)*100 as decimal(6,2)) as excp_percent
    from recon_tmp_trecongid as r
    inner join recon_trn_ttran as t on r.recon_gid = t.recon_gid
      and t.tran_date >= in_period_from
      and t.tran_date <= in_period_to
      and t.excp_amount > 0
      and t.delete_flag = 'N'
    right join recon_mst_taging as c on datediff(curdate(),t.tran_date) between c.aging_from and c.aging_to
      and c.delete_flag = 'N'
    where true
    group by c.aging_gid,c.aging_desc
  ) as ex on ag.aging_gid = ex.aging_gid;

  drop temporary table if exists recon_tmp_trecongid;
end $$

DELIMITER ;