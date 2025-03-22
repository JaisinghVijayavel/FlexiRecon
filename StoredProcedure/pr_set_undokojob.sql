DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_undokojob` $$
CREATE PROCEDURE `pr_set_undokojob`(
  in in_recon_code varchar(32),
  in in_job_gid int,
  in in_undo_job_reason varchar(255),
  in in_ip_addr varchar(128),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_sql text default '';
  declare v_txt text default '';
  declare v_count int default 0;

  declare v_jobtype_code text default '';
  declare v_ko_gid int default 0;
  declare v_undo_job_gid int default 0;
  declare v_job_date date;
  declare v_job_undo_period int default 0;

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
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

	  set v_tranko_table = 'recon_trn_ttranko';
	  set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';

	  set v_ko_table = 'recon_trn_tko';
	  set v_kodtl_table = 'recon_trn_tkodtl';
  end if;

  set out_result = 0;
  set out_msg = 'initiated';

  drop temporary table if exists recon_tmp_ttranko;
  drop temporary table if exists recon_tmp_tkotrangid;
  drop temporary table if exists recon_tmp_tkotranbrkpgid;

  create temporary table recon_tmp_ttranko(
    tran_gid int(10) unsigned NOT NULL,
    ko_value decimal(15,2) not null default 0,
    roundoff_value decimal(15,2) not null default 0,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tkotrangid(
    tran_gid int(10) unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tkotranbrkpgid(
    tranbrkp_gid int(10) unsigned NOT NULL,
    PRIMARY KEY (tranbrkp_gid)
  ) ENGINE = MyISAM;

  if exists(select job_gid from recon_trn_tjob
    where job_gid = in_job_gid
    and recon_code = in_recon_code
    and jobtype_code in ('A','S','M')
    and job_status in ('C','F')
    and delete_flag = 'N') then
    select
      jobtype_code,cast(start_date as date)
    into
      v_jobtype_code, v_job_date
    from recon_trn_tjob
    where job_gid = in_job_gid
    and recon_code = in_recon_code
    and jobtype_code in ('A','S','M')
    and job_status in ('C','F')
    and delete_flag = 'N';

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

    -- get undo job threshold
    set v_txt = fn_get_configvalue('job_undo_period');
    set v_job_undo_period = cast(ifnull(v_txt,'0') as unsigned);

    -- validate
    if curdate() > adddate(v_job_date,interval v_job_undo_period day) then
      set out_msg = concat('Undo job failed ! It should be done with in ',cast(v_job_undo_period as nchar),' day(s) !)');
      leave me;
    end if;

    -- validate recon_lock_date
    if datediff(v_job_date,v_recon_lock_date) < 0 then
      set out_msg = concat('Undo job failed ! Job date ',cast(v_job_date as nchar),' ! Recon Lock Date ',
        cast(v_recon_lock_date as nchar),' !');
      leave me;
    end if;

    if v_jobtype_code = 'A' or v_jobtype_code = 'M' then
      -- update job status
      if v_jobtype_code = 'A' then
        call pr_ins_job(in_recon_code,'U',in_job_gid,concat('Undo auto match ',ifnull(in_undo_job_reason,'')),'',in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);
      elseif v_jobtype_code = 'M' then
        call pr_ins_job(in_recon_code,'U',in_job_gid,concat('Undo manual match ',ifnull(in_undo_job_reason,'')),'',in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);
      end if;

      set v_undo_job_gid = @out_job_gid;

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
					where a.job_gid = ",cast(in_job_gid as nchar),"
					and a.delete_flag = 'N'
					group by b.tran_gid");
				
			call pr_run_sql(v_sql,@msg,@result);

      insert into recon_tmp_tkotrangid
        select tran_gid from recon_tmp_ttranko;

			set v_sql = concat("
				insert into recon_tmp_tkotranbrkpgid
					select distinct b.tranbrkp_gid from ",v_ko_table," as a
					inner join ",v_kodtl_table," as b on a.ko_gid = b.ko_gid and b.tranbrkp_gid > 0 and b.delete_flag = 'N'
					where a.job_gid = ",cast(in_job_gid as nchar),"
					and a.delete_flag = 'N'");
					
			call pr_run_sql(v_sql,@msg,@result);

			set v_sql = concat("
				insert ignore into recon_tmp_tkotranbrkpgid
					select b.tranbrkp_gid from ",v_ko_table," as a
					inner join ",v_tranbrkpko_table," as b on a.ko_gid = b.ko_gid and b.excp_value = 0 and b.delete_flag = 'N'
					where a.job_gid = ",cast(in_job_gid as nchar),"
					and a.delete_flag = 'N'");

			call pr_run_sql(v_sql,@msg,@result);

			set v_sql = concat("
				insert into ",v_tran_table,"
					select b.* from recon_tmp_tkotrangid as a
					inner join ",v_tranko_table," as b on a.tran_gid = b.tran_gid and b.delete_flag = 'N'");
					
			call pr_run_sql(v_sql,@msg,@result);

			set v_sql = concat("
      insert into ",v_tranbrkp_table,"
        select b.* from recon_tmp_tkotranbrkpgid as a
        inner join ",v_tranbrkpko_table," as b on a.tranbrkp_gid = b.tranbrkp_gid and b.delete_flag = 'N'");

			call pr_run_sql(v_sql,@msg,@result);

			set v_sql = concat("
				delete a.* from ",v_tranko_table," as a where a.tran_gid in (
					select b.tran_gid from recon_tmp_tkotrangid as b where a.tran_gid = b.tran_gid)
					and a.delete_flag = 'N'");

			call pr_run_sql(v_sql,@msg,@result);

			set v_sql = concat("
				delete a.* from ",v_tranbrkpko_table," as a where a.tranbrkp_gid in (
        select b.tranbrkp_gid from recon_tmp_tkotranbrkpgid as b where a.tranbrkp_gid = b.tranbrkp_gid)
        and a.delete_flag = 'N'");

			call pr_run_sql(v_sql,@msg,@result);

      -- add ko amount in exception amount
			set v_sql = concat("
				update recon_tmp_ttranko as a
				inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
					and b.delete_flag = 'N'
				set
					b.excp_value = b.excp_value + a.ko_value * b.tran_mult,
					b.roundoff_value = b.roundoff_value - a.roundoff_value,
					b.ko_gid = 0,
					b.ko_date = null");
					
			call pr_run_sql(v_sql,@msg,@result);

			set v_sql = concat("
				update ",v_ko_table," as k
				inner join ",v_kodtl_table," as a on k.ko_gid = a.ko_gid and a.delete_flag = 'N'
				inner join ",v_tranbrkp_table," as c on a.tranbrkp_gid = c.tranbrkp_gid
					and c.delete_flag = 'N'
				set
					c.excp_value = c.excp_value + a.ko_value,
					c.ko_gid = 0,
					c.ko_date = null
				where k.job_gid = ",cast(in_job_gid as nchar),"
				and k.delete_flag = 'N'");
				
			call pr_run_sql(v_sql,@msg,@result);

			set v_sql = concat("
				update ",v_ko_table," as k
				inner join ",v_tranbrkp_table," as c on k.ko_gid = c.ko_gid
					and c.delete_flag = 'N'
				set
					c.excp_value = c.tran_value,
					c.ko_gid = 0,
					c.ko_date = null
				where k.job_gid = ",cast(in_job_gid as nchar),"
				and k.delete_flag = 'N'");
				
			call pr_run_sql(v_sql,@msg,@result);

			set v_sql = concat("
      update ",v_ko_table," as k
      inner join ",v_kodtl_table," as a on k.ko_gid = a.ko_gid and a.delete_flag = 'N'
      set
        k.undo_ko_reason = '",in_undo_job_reason,"',
        k.update_date = sysdate(),
        k.update_by = '",in_user_code,"',
        k.delete_flag = 'Y',
        a.delete_flag = 'Y'
      where k.job_gid = ",cast(in_job_gid as nchar),"
      and k.delete_flag = 'N'");
			
			call pr_run_sql(v_sql,@msg,@result);

      -- set delete flag 'Y' in roundoff table
			set v_sql = concat("
				update ",v_ko_table," as k
				inner join ",v_koroundoff_table," as a on k.ko_gid = a.ko_gid and a.delete_flag = 'N'
				set
					a.delete_flag = 'Y'
				where k.job_gid = ",cast(in_job_gid as nchar),"
				and k.delete_flag = 'Y'");
				
			call pr_run_sql(v_sql,@msg,@result);

			-- posting undo
			set v_sql = concat("select count(*) into @post_count from ",v_tranbrkpko_table,"
				where posted_job_gid = ",cast(in_job_gid as nchar),"
        and delete_flag = 'N'");
				
			call pr_run_sql(v_sql,@msg,@result);
			
			set v_count = ifnull(@post_count,0);

      if v_count = 0 then
        truncate recon_tmp_tkotrangid;

        set v_sql = concat("insert into recon_tmp_tkotrangid (tran_gid)
            select distinct tran_gid from ",v_tranbrkp_table,"
              where posted_job_gid = ",cast(in_job_gid as nchar),"
              and delete_flag = 'N'
          ");

				call pr_run_sql2(v_sql,@msg,@result);

				set v_sql = concat("
					update ",v_tran_table," as a set
						a.mapped_value = 0
					where a.tran_gid in (select b.tran_gid from recon_tmp_tkotrangid as b
													 where a.tran_gid = b.tran_gid)
					and a.mapped_value = tran_value
					and a.delete_flag = 'N'");

				call pr_run_sql(v_sql,@msg,@result);

				set v_sql = concat("
					update ",v_tranbrkp_table," set
						tran_gid = 0,
						posted_job_gid = 0
					where posted_job_gid = ",cast(in_job_gid as nchar),"
					and delete_flag = 'N'");
					
				call pr_run_sql(v_sql,@msg,@result);
      end if;
    end if;

    call pr_upd_job(in_job_gid,'U','Undo completed...',@msg,@result);
    call pr_upd_job(v_undo_job_gid,'C','Undo completed...',@msg,@result);

    set out_result = 1;
    set out_msg = 'Job undo made successfully !';
  else
    set out_msg = 'Invalid job_gid';
  end if;

  drop temporary table if exists recon_tmp_ttranko;
  drop temporary table if exists recon_tmp_tkotrangid;
  drop temporary table if exists recon_tmp_tkotranbrkpgid;
end $$

DELIMITER ;