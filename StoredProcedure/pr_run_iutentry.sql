DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_iutentry` $$
CREATE PROCEDURE `pr_run_iutentry`
(
  in in_scheduler_gid int,
  in in_job_gid int,
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 03-04-2025

    Version : 2
  */

  declare v_recon_code text default '';
  declare v_job_gid int default 0;

	declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';
  declare v_ds_db_name text default '';
	declare v_ds_unit_table text default 'DS276';

  declare v_concurrent_ko_flag text default '';

	-- set tran table
  /*
	set v_tran_table = concat(in_recon_code,'_tran');
	set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  */

  -- get recon code
  select
    distinct recon_code into v_recon_code
  from recon_trn_tiutentry
  where scheduler_gid = in_scheduler_gid
  and recon_code <> ''
  and delete_flag = 'N' limit 1;

  set v_recon_code = ifnull(v_recon_code,'');

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(v_recon_code,'_tran');
	  set v_tranbrkp_table = concat(v_recon_code,'_tranbrkp');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  -- dataset table
  set v_ds_db_name = fn_get_configvalue('dataset_db_name');

  if v_ds_db_name <> '' then
    set v_ds_unit_table = concat(v_ds_db_name,'.',v_ds_unit_table);
  end if;

  drop temporary table if exists recon_tmp_trefno;
  drop temporary table if exists recon_tmp_trefgid;
  drop temporary table if exists recon_tmp_treftxtgid;

  CREATE temporary TABLE recon_tmp_trefno(
    entry_ref_no varchar(32) not null,
    PRIMARY KEY (entry_ref_no)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_trefgid(
    ref_tran_gid int not null,
    ref_tranbrkp_gid int not null,
    PRIMARY KEY (ref_tran_gid,ref_tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_treftxtgid(
    ref_tran_gid int not null,
    ref_tranbrkp_gid int not null,
    recon_code varchar(32),
    entry_ref_no varchar(32),
    entry_value decimal(15,2) not null default 0,
    tran_gid int not null default 0,
    bill_no varchar(32) default null,
    ipop_no varchar(32) default null,
    from_unit varchar(255) default null,
    to_unit varchar(255) default null,
    reftxt_tran_gid varchar(32),
    reftxt_tranbrkp_gid varchar(32),
    valid_flag char(1) not null default 'N',
    PRIMARY KEY (ref_tran_gid,ref_tranbrkp_gid)
  ) ENGINE = MyISAM;

  -- set job_gid
  set in_job_gid = ifnull(in_job_gid,0);
  set v_job_gid = in_job_gid;

  -- from unit update
  set v_sql = concat("update recon_trn_tiutentry as a
    inner join ",v_ds_unit_table," as b on a.from_loc_code = b.col1
      and b.delete_flag = 'N'
    set a.from_recon_code = b.col4,
        a.from_unit_name = b.col6
    where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and a.iutentry_status = 'P'
    and a.delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  -- to unit update
  set v_sql = concat("update recon_trn_tiutentry as a
    inner join ",v_ds_unit_table," as b on a.to_loc_code = b.col1
      and b.delete_flag = 'N'
    set a.to_recon_code = b.col4,
        a.to_unit_name = b.col6
    where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and a.iutentry_status = 'P'
    and a.delete_flag = 'N'
    ");

  call pr_run_sql2(v_sql,@msg,@result);


  -- Blank entry_ref_no
  update recon_trn_tiutentry
  set
    iutentry_status = 'F',
    iutentry_failed_reason = concat(ifnull(concat(iutentry_failed_reason,','),''),'Blank entry ref no')
  where scheduler_gid = in_scheduler_gid
  and (entry_ref_no is null or entry_ref_no = '')
  and delete_flag = 'N';

  -- check duplicate entry_ref_no
  -- iut entry table validation
  insert into recon_tmp_trefno(entry_ref_no)
    select entry_ref_no from recon_trn_tiutentry
    where scheduler_gid <> in_scheduler_gid
    and entry_ref_no <> ''
    and iutentry_status = 'C'
    and delete_flag = 'N'
    group by entry_ref_no;

  update recon_trn_tiutentry as a
  inner join recon_tmp_trefno as b on a.entry_ref_no = b.entry_ref_no
  set
    a.iutentry_status = 'F',
    a.iutentry_failed_reason = concat(ifnull(concat(a.iutentry_failed_reason,','),''),'Duplicate entry ref no')
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  -- iut entry table validation
  truncate recon_tmp_trefno;

  insert into recon_tmp_trefno(entry_ref_no)
    select entry_ref_no from recon_trn_tiutentry
    where scheduler_gid = in_scheduler_gid
    and entry_ref_no <> ''
    and iutentry_status = 'P'
    and delete_flag = 'N'
    group by entry_ref_no
    having sum(entry_value) <> 0;

  -- Amount not tallied
  update recon_trn_tiutentry as a
  inner join recon_tmp_trefno as b on a.entry_ref_no = b.entry_ref_no
  set
    a.iutentry_status = 'F',
    a.iutentry_failed_reason = concat(ifnull(concat(a.iutentry_failed_reason,','),''),'Amount not tallied')
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  -- from unit validation
  update recon_trn_tiutentry
  set
    iutentry_status = 'F',
    iutentry_failed_reason = concat(ifnull(concat(iutentry_failed_reason,','),''),'Invalid from unit')
  where scheduler_gid = in_scheduler_gid
  and from_recon_code is null
  and delete_flag = 'N';

  -- to unit validation
  update recon_trn_tiutentry
  set
    iutentry_status = 'F',
    iutentry_failed_reason = concat(ifnull(concat(iutentry_failed_reason,','),''),'Invalid to unit')
  where scheduler_gid = in_scheduler_gid
  and to_recon_code is null
  and delete_flag = 'N';

  -- entry value validation
  update recon_trn_tiutentry
  set
    iutentry_status = 'F',
    iutentry_failed_reason = concat(ifnull(concat(iutentry_failed_reason,','),''),'Zero entry value')
  where scheduler_gid = in_scheduler_gid
  and entry_value = 0
  and delete_flag = 'N';

  -- tran_gid validation
  update recon_trn_tiutentry
  set
    iutentry_status = 'F',
    iutentry_failed_reason = concat(ifnull(concat(iutentry_failed_reason,','),''),'Invalid tran id')
  where scheduler_gid = in_scheduler_gid
  and ref_tran_gid = 0
  and delete_flag = 'N';

  -- duplicate tran_gid,tranbrkp_gid validation
  insert into recon_tmp_trefgid(ref_tran_gid,ref_tranbrkp_gid)
    select ref_tran_gid,ref_tranbrkp_gid from recon_trn_tiutentry
    where iutentry_status in ('C','P')
    and delete_flag = 'N'
    group by ref_tran_gid,ref_tranbrkp_gid
    having count(*) > 1;

  update recon_trn_tiutentry as a
  inner join recon_tmp_trefgid as b on a.ref_tran_gid = b.ref_tran_gid and a.ref_tranbrkp_gid = b.ref_tranbrkp_gid
  set
    a.iutentry_status = 'F',
    a.iutentry_failed_reason = concat(ifnull(concat(a.iutentry_failed_reason,','),''),'Duplicate tran id & supporting tran id')
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  -- tran_gid,tranbrkp_gid validation
  insert into recon_tmp_treftxtgid
    (
      ref_tran_gid,ref_tranbrkp_gid,recon_code,entry_ref_no,
      entry_value,reftxt_tran_gid,reftxt_tranbrkp_gid
    )
    select
      distinct ifnull(ref_tran_gid,0),ifnull(ref_tranbrkp_gid,0),recon_code,entry_ref_no,
      entry_value,cast(ref_tran_gid as nchar),cast(ref_tranbrkp_gid as nchar)
    from recon_trn_tiutentry
    where scheduler_gid = in_scheduler_gid
    and delete_flag = 'N';

  -- validate
  set v_sql = concat("update ",v_tran_table," as a
    inner join recon_tmp_treftxtgid as b on a.recon_code = b.recon_code
      and a.col1 = b.reftxt_tran_gid
      and a.col2 = b.reftxt_tranbrkp_gid
      and a.col51 is null
    set b.valid_flag = 'Y',
        b.tran_gid = a.tran_gid,
        b.bill_no = a.col19,
        b.ipop_no = a.col21
    ");

  call pr_run_sql2(v_sql,@msg,@result);

  update recon_trn_tiutentry as a
  inner join recon_tmp_treftxtgid as b on a.ref_tran_gid = b.ref_tran_gid and a.ref_tranbrkp_gid = b.ref_tranbrkp_gid
    and b.valid_flag = 'N'
  set
    a.iutentry_status = 'F',
    a.iutentry_failed_reason = concat(ifnull(concat(a.iutentry_failed_reason,','),''),'Invalid tran id & supporting tran id')
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N';

  if not exists(select scheduler_gid from recon_trn_tiutentry
    where scheduler_gid = in_scheduler_gid
    and iutentry_status = 'F'
    and delete_flag = 'N') then

    -- update values
    update recon_trn_tiutentry set
      dr_amount = if(entry_value < 0,abs(entry_value),0),
      cr_amount = if(entry_value > 0,entry_value,0),
      tran_value = abs(entry_value),
      tran_acc_mode = if(entry_value > 0,'C','D'),
      tran_mult = if(entry_value > 0,1,-1)
    where scheduler_gid = in_scheduler_gid
    and delete_flag = 'N';

    update recon_trn_tiutentry as a
    inner join recon_tmp_treftxtgid as b on a.ref_tran_gid = b.ref_tran_gid and a.ref_tranbrkp_gid = b.ref_tranbrkp_gid
    set
      a.bill_no = b.bill_no,
      a.ipop_no = b.ipop_no,
      b.from_unit = a.from_unit_name,
      b.to_unit = a.to_unit_name
    where a.scheduler_gid = in_scheduler_gid
    and a.delete_flag = 'N';

		-- cr location
		set v_sql=concat("insert into ",v_tranbrkp_table,"
			(
				scheduler_gid,
				recon_code,
				col4,
				col8,
				col9,
				col11,
				col12,
				col16,
				col17,
				col18,
				col19,
				col20,
				col21,
				col22,
				col23,
				col37,
				col38,
				col45,
				col43,
				col50,
				col46,
				col47,
				col48,
				col49,
				col51,
        col53
			)
			select
				1,
				recon_code,
				cast(entry_date as nchar),
				cast(tran_value as nchar),
				cast(tran_value as nchar),
				tran_acc_mode,
				cast(tran_mult as nchar),
				'Entry',
				cast(dr_amount as nchar),
				cast(cr_amount as nchar),
				bill_no,
				uhid_no,
				ipop_no,
				'Entry',
				'Entry',
				cast(entry_value as nchar),
				from_recon_code,
				to_recon_code,
				from_loc_code,
				to_loc_code,
				cast(entry_value as nchar),
				iut_ipop,
				from_unit_name,
				to_unit_name,
				entry_ref_no,
        '0'
			from recon_trn_tiutentry
			where scheduler_gid = ",cast(in_scheduler_gid as nchar),"
			and delete_flag = 'N'
			");

		call pr_run_sql2(v_sql,@msg,@result);

		-- validate
		set v_sql = concat("update ",v_tran_table," as a
			inner join recon_tmp_treftxtgid as b on a.tran_gid = b.tran_gid
        and a.recon_code = b.recon_code
				and a.col1 = b.reftxt_tran_gid
				and a.col2 = b.reftxt_tranbrkp_gid
        and b.valid_flag = 'Y'
				and a.col51 is null
			set a.col46 = cast(b.entry_value*-1 as nchar),
          a.col47 = 'IUT - MANUAL',
          a.col51 = b.entry_ref_no,
          a.col53 = cast(cast(a.col37 as decimal(15,2))-b.entry_value*-1 as nchar)
			");

		call pr_run_sql2(v_sql,@msg,@result);

    -- recon field
    -- col22 - IUT Type/Flag
    -- col23 - IUT Entry Value
    -- col24 - Closing Balance Value
    -- col25 - From Unit
    -- col26 - To Unit
    -- col27 - Entry Reference No

    -- update in pd tran table
		set v_sql = concat("update ",v_tran_table," as a
			inner join recon_tmp_treftxtgid as b on a.tran_gid = b.ref_tran_gid
				and (b.ref_tranbrkp_gid = '0' or b.ref_tranbrkp_gid is null)
        and b.valid_flag = 'Y'
			set a.col23 = cast(b.entry_value*-1 as nchar),
          a.col22 = 'IUT - MANUAL',
          a.col27 = b.entry_ref_no,
          a.col24 = cast(cast(a.col37 as decimal(15,2))-b.entry_value*-1 as nchar),
          a.col25 = b.from_unit,
          a.col26 = b.to_unit
			");

		call pr_run_sql2(v_sql,@msg,@result);

    -- update in pd tranbrkp table
		set v_sql = concat("update ",v_tranbrkp_table," as a
			inner join recon_tmp_treftxtgid as b on a.tran_gid = b.ref_tran_gid
				and a.tranbrkp_gid = b.ref_tranbrkp_gid
        and b.valid_flag = 'Y'
			set a.col23 = cast(b.entry_value*-1 as nchar),
          a.col22 = 'IUT - MANUAL',
          a.col27 = b.entry_ref_no,
          a.col24 = cast(cast(a.col37 as decimal(15,2))-b.entry_value*-1 as nchar),
          a.col25 = b.from_unit,
          a.col26 = b.to_unit
			");

		call pr_run_sql2(v_sql,@msg,@result);

    -- update values
    update recon_trn_tiutentry set
      iutentry_status = 'C'
    where scheduler_gid = in_scheduler_gid
    and delete_flag = 'N';

    set out_msg = 'Success';
    set out_result = 1;
  else
    call pr_run_tablequery('','','RPT_IUTENTRY','recon_trn_tiutentry',concat('and scheduler_gid = ',cast(in_scheduler_gid as nchar)),v_job_gid,true,'csv','',@msg,@result);

    set out_msg = 'Failed';
    set out_result = 0;
  end if;

  drop temporary table if exists recon_tmp_trefno;
  drop temporary table if exists recon_tmp_trefgid;
  drop temporary table if exists recon_tmp_treftxtgid;
end $$

DELIMITER ;