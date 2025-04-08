DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_undoko` $$
CREATE PROCEDURE `pr_set_undoko`
(
  in in_ko_gid int,
	in in_recon_code varchar(32),
  in in_undo_ko_reason varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_sql text default '';
  declare v_txt text default '';
	declare v_ko_gid int default 0;
	declare v_count int default 0;

  declare v_ko_date date;
  declare v_ko_undo_period int default 0;

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';
	declare v_koroundoff_table text default '';

  declare v_recon_lock_date date;

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

  set out_result = 0;
  set out_msg = 'initiated';

  -- check if any job is running
	if exists(select job_gid from recon_trn_tjob
		where recon_code = in_recon_code
		and jobtype_code in ('A','M','U','T','UJ')
		and job_status in ('I','P')
		and delete_flag = 'N') then

		select group_concat(cast(job_gid as nchar)) into v_txt from recon_trn_tjob
		where recon_code = in_recon_code
		and jobtype_code in ('A','M','U','T','UJ')
		and job_status in ('I','P')
		and delete_flag = 'N';

		set out_msg = concat('KO/Undo KO/Field Update/Theme is already running in the job id ', v_txt ,' ! ');
		set out_result = 0;
    leave me;
	end if;

  drop temporary table if exists recon_tmp_ttranko;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;

  create temporary table recon_tmp_ttranko(
    tran_gid int(10) unsigned NOT NULL,
    ko_value decimal(15,2) not null default 0,
    roundoff_value decimal(15,2) not null default 0,
    PRIMARY KEY (tran_gid)
  );

  CREATE temporary TABLE recon_tmp_ttrangid(
    tran_gid int(10) unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  );

  CREATE temporary TABLE recon_tmp_ttranbrkpgid(
    tranbrkp_gid int(10) unsigned NOT NULL,
    PRIMARY KEY (tranbrkp_gid)
  );

	set v_sql = concat("select ko_gid into @ko_gid from ",v_ko_table,"
    where ko_gid = ",cast(in_ko_gid as nchar),"
    and delete_flag = 'N'");

	call pr_run_sql(v_sql,@msg,@result);

	set v_ko_gid = ifnull(@ko_gid,0);

  if v_ko_gid > 0 then
    -- get recon_lock_date
    select
      ifnull(recon_lock_date,'2000-01-01')
    into
      v_recon_lock_date
    from recon_mst_trecon
    where recon_code = in_recon_code
    and active_status = 'Y'
    and delete_flag = 'N';

    set v_recon_lock_date = ifnull(v_recon_lock_date,'2000-01-01');

    -- get ko date
		set v_sql = concat("
			select ko_date into @ko_date from ",v_ko_table,"
			where ko_gid = ",cast(in_ko_gid as nchar),"
			and delete_flag = 'N'");

		set v_ko_date = ifnull(@ko_date,null);

    set v_txt = fn_get_configvalue('ko_undo_period');
    set v_ko_undo_period = cast(ifnull(v_txt,'0') as unsigned);

    if curdate() > adddate(v_ko_date,interval v_ko_undo_period day) then
      set out_msg = concat('Undo ko failed ! It should be done with in ',cast(v_ko_undo_period as nchar),' day(s) !)');
      leave me;
    end if;

    -- validate recon_lock_date
    if datediff(v_ko_date,v_recon_lock_date) < 0 then
      set out_msg = concat('Undo job failed ! KO date ',cast(v_ko_date as nchar),' ! Recon Lock Date ',
        cast(v_recon_lock_date as nchar),' !');
      leave me;
    end if;

		set v_sql = concat("
			insert into recon_tmp_ttranko (tran_gid,ko_value,roundoff_value)
				select
					b.tran_gid,
					sum(b.ko_value*b.ko_mult) as ko_value,
					sum(ifnull(c.roundoff_value,0)) as roundoff_value
				from ",v_ko_table," as a
				inner join ",v_kodtl_table," as b on a.ko_gid = b.ko_gid and b.delete_flag = 'N'
				left join ",v_koroundoff_table," as c on b.ko_gid = c.ko_gid
					and b.tran_gid = c.tran_gid
					and b.tranbrkp_gid = c.tranbrkp_gid
					and c.delete_flag = 'N'
				where a.ko_gid = ",cast(in_ko_gid as nchar),"
				and a.delete_flag = 'N'
				group by b.tran_gid");

		call pr_run_sql(v_sql,@msg,@count);

    insert into recon_tmp_ttrangid
      select tran_gid from recon_tmp_ttranko;

		set v_sql = concat("
			insert into recon_tmp_ttranbrkpgid
      select distinct tranbrkp_gid from ",v_kodtl_table,"
      where ko_gid = ",cast(in_ko_gid as nchar)," and tranbrkp_gid > 0 and delete_flag = 'N'");

		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			insert ignore into recon_tmp_ttranbrkpgid
      select tranbrkp_gid from ",v_tranbrkpko_table,"
      where ko_gid = ",cast(in_ko_gid as nchar)," and excp_value = 0 and delete_flag = 'N'");

		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			insert into ",v_tran_table,"
      select b.* from recon_tmp_ttrangid as a
      inner join ",v_tranko_table," as b on a.tran_gid = b.tran_gid and b.delete_flag = 'N'");

		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			insert into ",v_tranbrkp_table,"
      select b.* from recon_tmp_ttranbrkpgid as a
      inner join ",v_tranbrkpko_table," as b on a.tranbrkp_gid = b.tranbrkp_gid and b.delete_flag = 'N'");

		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			delete a.* from ",v_tranko_table," as a where a.tran_gid in (
      select b.tran_gid from recon_tmp_ttrangid as b where a.tran_gid = b.tran_gid)
      and a.delete_flag = 'N'");

		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			delete a.* from ",v_tranbrkpko_table," as a where a.tranbrkp_gid in (
      select b.tranbrkp_gid from recon_tmp_ttranbrkpgid as b where a.tranbrkp_gid = b.tranbrkp_gid)
      and a.delete_flag = 'N'");

		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			update recon_tmp_ttranko as a
			inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
			and b.delete_flag = 'N'
			set
				b.excp_value = b.excp_value + a.ko_value*b.tran_mult,
				b.roundoff_value = b.roundoff_value - a.roundoff_value,
				b.ko_gid = 0,
				b.ko_date = null");
				
		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			update ",v_kodtl_table," as a
			inner join ",v_tranbrkp_table," as c on a.tranbrkp_gid = c.tranbrkp_gid
				and c.delete_flag = 'N'
			set
				c.excp_value = c.excp_value + a.ko_value,
				c.ko_gid = 0,
				c.ko_date = null
			where a.ko_gid = ",cast(in_ko_gid as nchar),"
			and a.delete_flag = 'N'");
			
		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			update ",v_tranbrkp_table,"
			set
				excp_value = tran_value,
				ko_gid = 0,
				ko_date = null
			where ko_gid = ",cast(in_ko_gid as nchar),"
			and delete_flag = 'N'");

		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			update ",v_ko_table," set
				undo_ko_reason = '",in_undo_ko_reason,"',
				update_date = sysdate(),
				update_by = '",in_user_code,"',
				delete_flag = 'Y'
			where ko_gid = ",cast(in_ko_gid as nchar),"
			and delete_flag = 'N'");

		call pr_run_sql(v_sql,@msg,@result);

		set v_sql = concat("
			update ",v_kodtl_table," set
				delete_flag = 'Y'
			where ko_gid = ",cast(in_ko_gid as nchar),"
			and delete_flag = 'N'");
			
		call pr_run_sql(v_sql,@msg,@result);

    -- set delete flag 'Y' in roundoff table
		set v_sql = concat("
    update ",v_koroundoff_table," set
      delete_flag = 'Y'
    where ko_gid = ",cast(in_ko_gid as nchar),"
    and delete_flag = 'N'");
		
		call pr_run_sql(v_sql,@msg,@result);

    set out_result = 1;
    set out_msg = 'Ko undo made successfully !';
  else
    set out_msg = 'Invalid ko_gid';
  end if;

  drop temporary table if exists recon_tmp_ttranko;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;
end $$

DELIMITER ;