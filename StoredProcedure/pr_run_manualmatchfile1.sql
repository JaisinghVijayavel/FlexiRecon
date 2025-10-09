DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_manualmatchfile` $$
CREATE PROCEDURE `pr_run_manualmatchfile`(
  in in_scheduler_gid int,
	-- in in_recon_code varchar(32),
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_match_gid int default 0;
  declare v_recon_code text default '';
  declare v_recontype_code text default '';
  declare v_ko_gid int default 0;
  declare v_ko_value decimal(15,2) default 0;
  declare v_value decimal(15,2) default 0;
  declare v_tran_value decimal(15,2) default 0;
  declare v_ko_reason text default '';
  declare v_tot_count int default 0;
  declare v_succ_count int default 0;
  declare v_job_gid int default 0;
  declare v_txt text default '';
  declare v_mapping_type char(1) default '';
  declare v_file_name text default '';

	declare v_sql text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

	declare v_tranko_table text default '';
	declare v_tranbrkpko_table text default '';

  declare v_concurrent_ko_flag text default '';

	declare v_ko_table text default '';
	declare v_kodtl_table text default '';
	declare v_koroundoff_table text default '';

  /*
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    ROLLBACK;

    call pr_upd_job(v_job_gid,'F',@full_error,@msg,@result);

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;
  */

  drop temporary table if exists recon_tmp_tmanualtrangid;
  drop temporary table if exists recon_tmp_tmanualmatchgid;
  drop temporary table if exists recon_tmp_tmanualkodtl;
  drop temporary table if exists recon_tmp_tmanualtrankodtl;

  CREATE temporary TABLE recon_tmp_tmanualtrangid(
    tran_gid int(10) unsigned NOT NULL,
    tranbrkp_gid int(10) unsigned NOT NULL,
    dataset_code varchar(32) default null,
    dataset_type varchar(32) default null,
    excp_value decimal(15,2) not null default 0,
    ko_value decimal(15,2) not null default 0,
    roundoff_value decimal(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tmanualmatchgid(
    match_gid int(10) unsigned NOT NULL,
    ko_source_value decimal(15,2) not null default 0,
    ko_comparison_value decimal(15,2) not null default 0,
    ko_diff_value decimal(15,2) not null default 0,
    ko_reason text default null,
    PRIMARY KEY (match_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmanualkodtl(
    kodtl_gid int unsigned NOT NULL AUTO_INCREMENT,
    ko_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    ko_value decimal(15,2) not null default 0,
    roundoff_value decimal(15,2) not null default 0,
    ko_mult tinyint not null default 0,
    PRIMARY KEY (kodtl_gid),
    key idx_ko_gid(ko_gid),
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tmanualtrankodtl(
    trankodtl_gid int unsigned NOT NULL AUTO_INCREMENT,
    ko_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL,
    ko_value decimal(15,2) not null default 0,
    roundoff_value decimal(15,2) not null default 0,
    PRIMARY KEY (trankodtl_gid),
    key idx_ko_gid(ko_gid),
    key idx_tran_gid(tran_gid)
  ) ENGINE = MyISAM;


  if not exists(select scheduler_gid from con_trn_tscheduler
     where scheduler_gid = in_scheduler_gid and delete_flag = 'N') then
    set out_msg = 'File not found !';
    set out_result = 0;
    leave me;
  end if;

  select
    file_name into v_file_name
  from con_trn_tscheduler
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  select
    a.recon_code,
    b.recontype_code
  into
    v_recon_code,
    v_recontype_code
  from recon_trn_tmanualtran as a
  inner join recon_mst_trecon as b on a.recon_code = b.recon_code
    and b.delete_flag = 'N'
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N'
  limit 0,1;

  set v_recon_code = ifnull(v_recon_code,'');
  set v_recontype_code = ifnull(v_recontype_code,'');

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
	  set v_tran_table = concat(v_recon_code,'_tran');
	  set v_tranbrkp_table = concat(v_recon_code,'_tranbrkp');

	  set v_tranko_table = concat(v_recon_code,'_tranko');
	  set v_tranbrkpko_table = concat(v_recon_code,'_tranbrkpko');

	  set v_ko_table = concat(v_recon_code,'_ko');
	  set v_kodtl_table = concat(v_recon_code,'_kodtl');
    set v_koroundoff_table = concat(v_recon_code,'_koroundoff');
  else
	  set v_tran_table = 'recon_trn_ttran';
	  set v_tranbrkp_table = 'recon_trn_ttranbrkp';

	  set v_tranko_table = 'recon_trn_ttranko';
	  set v_tranbrkpko_table = 'recon_trn_ttranbrkpko';

	  set v_ko_table = 'recon_trn_tko';
	  set v_kodtl_table = 'recon_trn_tkodtl';
    set v_koroundoff_table = 'recon_trn_tkoroundoff';
  end if;

  select
    count(distinct match_gid) into v_tot_count
  from recon_trn_tmanualtran
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  set v_tot_count = ifnull(v_tot_count,0);

  -- Match validation
  if v_tot_count = 0 then
    set out_msg = 'No record found !';
    set out_result = 0;
    leave me;
  end if;

	if exists(select job_gid from recon_trn_tjob
	  where jobtype_code = 'M'
	  and job_status in ('I','P')
	  and delete_flag = 'N') then

	  select group_concat(cast(job_gid as nchar)) into v_txt from recon_trn_tjob
	  where jobtype_code = 'M'
	  and job_status in ('I','P')
	  and delete_flag = 'N';

	  set out_msg = concat('Manual match is already running in the job id ', v_txt ,' ! ');
	  set out_result = 0;

	  set v_job_gid = 0;

	  leave me;
	else
	  call pr_ins_job(v_recon_code,'M',in_scheduler_gid,concat('Manual match - ',v_file_name),v_file_name,
      in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);
	end if;

  set v_job_gid = @out_job_gid;

  -- Wiith in A/C, Between A/C Validation
  if v_recontype_code = 'W' or v_recontype_code = 'B' then
    if fn_get_chkbalance(v_recon_code,curdate()) = false then
      set out_msg = 'Recon was not tallied !';
      set out_result = 0;

      SIGNAL SQLSTATE '99999' SET MESSAGE_TEXT = 'Recon was not tallied';
      leave me;
    end if;
  end if;

  -- update ko_mult in manualtran table
  set v_sql = concat("update recon_trn_tmanualtran as a
    inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
      and b.delete_flag = 'N'
    set
      a.ko_mult = b.tran_mult,
      a.ko_acc_mode = b.tran_acc_mode
    where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and a.tranbrkp_gid = 0
    and a.delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("update recon_trn_tmanualtran as a
    inner join ",v_tranbrkp_table," as b on a.tran_gid = b.tran_gid
      and a.tranbrkp_gid = b.tranbrkp_gid
      and b.delete_flag = 'N'
    set
      a.ko_mult = b.tran_mult,
      a.ko_acc_mode = b.tran_acc_mode
    where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
    and a.tranbrkp_gid > 0
    and a.delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  /*
  update recon_trn_tmanualtran
  set ko_mult = -1
  where scheduler_gid = in_scheduler_gid
  and ko_acc_mode = 'D'
  and delete_flag = 'N';

  update recon_trn_tmanualtran
  set ko_mult = 1
  where scheduler_gid = in_scheduler_gid
  and ko_acc_mode = 'C'
  and delete_flag = 'N';

  update recon_trn_tmanualtran
  set ko_mult = 1
  where scheduler_gid = in_scheduler_gid
  and ko_acc_mode = 'V'
  and delete_flag = 'N';
  */
  -- validate tran_gid
  truncate recon_tmp_tmanualtrangid;

  if v_recontype_code <> 'N' then
    -- insert in trangid table where tranbrkp_gid = 0
		set v_sql = concat("
			insert into recon_tmp_tmanualtrangid
				select
					a.tran_gid,
					0 as tranbrkp_gid,
					b.dataset_code,
					c.dataset_type,
					b.excp_value,
					sum(a.ko_value) as ko_value,
          sum(a.roundoff_value) as roundoff_value,
					b.tran_mult
				from recon_trn_tmanualtran as a
				inner join ",v_tran_table," as b
					on a.tran_gid = b.tran_gid
					and b.recon_code = a.recon_code
					and a.ko_value <= b.excp_value
					and b.excp_value <> 0
					-- and b.mapped_value = 0
					and b.delete_flag = 'N'
				inner join recon_mst_trecondataset as c on b.dataset_code = c.dataset_code
          and b.recon_code = c.recon_code
          and c.active_status = 'Y'
          and c.delete_flag = 'N'
				where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
				and a.tranbrkp_gid = 0
				and a.delete_flag = 'N'
				group by a.tran_gid,b.dataset_code,b.excp_value,b.tran_mult");

		call pr_run_sql(v_sql,@msg,@result);

    -- insert in trangid table where tranbrkp_gid > 0
		set v_sql = concat("
			insert into recon_tmp_tmanualtrangid
				select
					a.tran_gid,
					a.tranbrkp_gid,
					b.dataset_code,
					c.dataset_type,
					b.excp_value,
					a.ko_value,
          a.roundoff_value,
					b.tran_mult
				from recon_trn_tmanualtran as a
				inner join ",v_tranbrkp_table," as b
					on a.tran_gid = b.tran_gid
					and a.tranbrkp_gid = b.tranbrkp_gid
					and b.recon_code = a.recon_code
					and a.ko_value <= b.excp_value
					and b.excp_value > 0
					and b.delete_flag = 'N'
				inner join recon_mst_trecondataset as c on b.dataset_code = c.dataset_code
          and b.recon_code = c.recon_code
          and c.active_status = 'Y'
          and c.delete_flag = 'N'
				where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
				and a.delete_flag = 'N'");

		call pr_run_sql(v_sql,@msg,@result);
  else
		set v_sql = concat("
			insert into recon_tmp_tmanualtrangid
				select
					a.tran_gid,
					b.dataset_code,
					c.dataset_type,
					0,
					0,
					0,
          0
				from recon_trn_tmanualtran as a
				inner join ",v_tran_table," as b
					on a.tran_gid = b.tran_gid
					and b.recon_code = a.recon_code
					and b.ko_gid = 0
					and b.delete_flag = 'N'
				inner join recon_mst_trecondataset as c on b.dataset_code = c.dataset_code
          and b.recon_code = c.recon_code
          and c.active_status = 'Y'
          and c.delete_flag = 'N'
				where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
				and a.delete_flag = 'N'
				group by a.tran_gid,b.dataset_code,c.dataset_type");

		call pr_run_sql(v_sql,@msg,@result);
  end if;

  -- validate match_gid
  truncate recon_tmp_tmanualmatchgid;

  if v_recontype_code = 'W' or v_recontype_code = 'B' then
    insert into recon_tmp_tmanualmatchgid
      select
        a.match_gid,
        sum(if(a.ko_mult = -1,a.ko_value,0)) as ko_source_value,
        sum(if(a.ko_mult = 1,a.ko_value,0)) as ko_comparison_value,
        sum(a.ko_value*a.ko_mult) as ko_diff_value,
        max(a.ko_reason) as ko_reason
      from recon_trn_tmanualtran as a
      inner join recon_tmp_tmanualtrangid as b
        on a.tran_gid = b.tran_gid
        and a.tranbrkp_gid = b.tranbrkp_gid
        and b.excp_value <> 0
        and b.excp_value >= (a.ko_value+a.roundoff_value)
      where a.scheduler_gid = in_scheduler_gid
      and a.delete_flag = 'N'
      group by a.match_gid
      having sum(a.ko_value*b.tran_mult) = 0;
  elseif v_recontype_code = 'I' or v_recontype_code = 'V' then
    insert into recon_tmp_tmanualmatchgid
      select
        a.match_gid,
        sum(if(b.dataset_type='B',a.ko_value*b.tran_mult,0)) as ko_source_value,
        sum(if(b.dataset_type='T',a.ko_value*b.tran_mult,0)) as ko_comparison_value,
        sum(a.ko_value*b.tran_mult) as ko_diff_value,
        max(a.ko_reason) as ko_reason
      from recon_trn_tmanualtran as a
      inner join recon_tmp_tmanualtrangid as b
        on a.tran_gid = b.tran_gid
        and a.tranbrkp_gid = b.tranbrkp_gid
        and b.excp_value <> 0
        and b.excp_value >= (a.ko_value+a.roundoff_value)
      where a.scheduler_gid = in_scheduler_gid
      and a.delete_flag = 'N'
      group by a.match_gid
      having sum(if(b.dataset_type='B',a.ko_value*b.tran_mult,0)) = sum(if(b.dataset_type='T',a.ko_value*b.tran_mult,0));
  elseif v_recontype_code = 'N' then
    insert into recon_tmp_tmanualmatchgid
      select
        a.match_gid,
        0 as ko_source_value,
        0 as ko_comparison_value,
        0 as ko_diff_value,
        max(a.ko_reason) as ko_reason
      from recon_trn_tmanualtran as a
      inner join recon_tmp_tmanualtrangid as b
        on a.tran_gid = b.tran_gid
      where a.scheduler_gid = in_scheduler_gid
      and a.delete_flag = 'N'
      group by a.match_gid;
  end if;

  ko_block:begin
    declare ko_done int default 0;
    declare ko_cursor cursor for
      select match_gid,ko_source_value,ko_reason from recon_tmp_tmanualmatchgid;
    declare continue handler for not found set ko_done=1;

    open ko_cursor;

    ko_loop: loop
      fetch ko_cursor into v_match_gid,v_ko_value,v_ko_reason;

      if ko_done = 1 then leave ko_loop; end if;

      call pr_ins_ko(v_recon_code,v_job_gid,'','Y','N',v_ko_value,v_ko_reason,'',in_user_code,@ko_gid,@msg,@result);

      set v_ko_gid = @ko_gid;

      -- call pr_set_ko_bulk(v_job_gid,v_recon_gid,v_ko_amount,v_ko_tran,v_ko_reason,in_user_code,@ko_gid,@msg,@result);

      -- select @msg,@result,v_match_gid,v_ko_tran,v_ko_amount,v_ko_reason;

      if v_ko_gid > 0 then
        truncate recon_tmp_tmanualkodtl;
        truncate recon_tmp_tmanualtrankodtl;

        if v_recontype_code <> 'N' then
					set v_sql = concat("
						insert into recon_tmp_tmanualkodtl (ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult,roundoff_value)
							select
								",cast(v_ko_gid as nchar),",
								a.tran_gid,
								a.tranbrkp_gid,
								a.ko_value,
								a.ko_mult,
                a.roundoff_value
							from recon_trn_tmanualtran as a
							inner join ",v_tran_table," as b
								on a.tran_gid = b.tran_gid
								and b.excp_value <> 0
								/*and b.excp_value >= ((a.ko_value*a.ko_mult)*b.tran_mult)*/
								and b.delete_flag = 'N'
							where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
							and a.match_gid = ",cast(v_match_gid as nchar),"
							and a.tranbrkp_gid = 0
							and a.ko_gid = 0
							and a.delete_flag = 'N'");

					call pr_run_sql(v_sql,@msg,@result);

					set v_sql = concat("
						insert into recon_tmp_tmanualkodtl (ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult,roundoff_value)
							select
								",cast(v_ko_gid as nchar),",
								a.tran_gid,
								a.tranbrkp_gid,
								a.ko_value,
								b.tran_mult,
                a.roundoff_value
							from recon_trn_tmanualtran as a
							inner join ",v_tranbrkp_table," as b
								on a.tran_gid = b.tran_gid
								and a.tranbrkp_gid = b.tranbrkp_gid
								and b.excp_value > 0
								and b.excp_value >= a.ko_value
								and b.delete_flag = 'N'
							where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
							and a.match_gid = ",cast(v_match_gid as nchar),"
							and a.ko_gid = 0
							and a.delete_flag = 'N'");

					call pr_run_sql(v_sql,@msg,@result);

					set v_sql = concat("
						insert into recon_tmp_tmanualtrankodtl (ko_gid,tran_gid,ko_value,roundoff_value)
							select
								",cast(v_ko_gid as nchar),",
								a.tran_gid,
								sum(a.ko_value*a.ko_mult),
								sum(a.roundoff_value*a.ko_mult)
							from recon_trn_tmanualtran as a
							inner join ",v_tran_table," as b
								on a.tran_gid = b.tran_gid
								and b.excp_value <> 0
								/*and b.excp_value >= a.ko_value*/
								and b.delete_flag = 'N'
							where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
							and a.match_gid = ",cast(v_match_gid as nchar),"
							and a.ko_gid = 0
							and a.delete_flag = 'N'
							group by a.tran_gid");

					call pr_run_sql(v_sql,@msg,@result);

          select abs(sum(ko_value*ko_mult)) into v_value from recon_tmp_tmanualkodtl;

          if (v_value > 0 and v_recontype_code = 'I') or v_value = 0 then
						set v_sql = concat("
							insert into ",v_kodtl_table," (ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult)
              select ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult from recon_tmp_tmanualkodtl");

						call pr_run_sql(v_sql,@msg,@result);

            -- knockoff roundoff value
						set v_sql = concat("
							insert into ",v_koroundoff_table," (ko_gid,tran_gid,tranbrkp_gid,roundoff_value)
              select ko_gid,tran_gid,tranbrkp_gid,roundoff_value from recon_tmp_tmanualkodtl
              where roundoff_value <> 0");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("
							update ",v_tran_table," as a
							inner join recon_tmp_tmanualtrankodtl as b on a.tran_gid = b.tran_gid
							set a.excp_value = a.excp_value - (b.ko_value)*a.tran_mult,
                a.roundoff_value = a.roundoff_value + b.roundoff_value,
								a.ko_gid = b.ko_gid,
								a.ko_date = curdate(),
                a.theme_code = ''
							where a.excp_value <> 0
							and a.delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);

						set v_sql = concat("
							update ",v_tranbrkp_table," as a
							inner join recon_tmp_tmanualkodtl as b on a.tran_gid = b.tran_gid
								and a.tranbrkp_gid = b.tranbrkp_gid
							set a.excp_value = 0,
								a.ko_gid = b.ko_gid,
								a.ko_date = curdate(),
                a.theme_code = ''
							where a.excp_value > 0
							and a.delete_flag = 'N'");

						call pr_run_sql(v_sql,@msg,@result);
          end if;
        else
					set v_sql = concat("
						insert into recon_tmp_tmanualkodtl (ko_gid,tran_gid,ko_value)
							select
								",cast(v_ko_gid as nchar),",
								a.tran_gid,
								a.ko_value
							from recon_trn_tmanualtran as a
							inner join ",v_tran_table," as b
								on a.tran_gid = b.tran_gid
								and b.delete_flag = 'N'
							where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
							and a.match_gid = ",cast(v_match_gid as nchar),"
							and a.ko_gid = 0
							and a.delete_flag = 'N'");

					call pr_run_sql(v_sql,@msg,@result);

					set v_sql = concat("
						insert into ",v_kodtl_table," (ko_gid,tran_gid,ko_value)
							select a.ko_gid,a.tran_gid,a.ko_value from recon_tmp_tmanualkodtl as a
							inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
							and b.delete_flag = 'N'");

					call pr_run_sql(v_sql,@msg,@result);

					set v_sql = concat("
						update ",v_tran_table," as a
						inner join recon_tmp_tmanualkodtl as b on a.tran_gid = b.tran_gid
						set a.ko_gid = b.ko_gid,
								a.ko_date = curdate(),
                a.theme_code = ''
						where a.delete_flag = 'N'");

					call pr_run_sql(v_sql,@msg,@result);
        end if;

        update recon_trn_tmanualtran set
          ko_status = 'C',
          ko_gid = v_ko_gid,
          ko_date = curdate()
        where scheduler_gid = in_scheduler_gid
        and match_gid = v_match_gid
        and delete_flag = 'N';

        insert into recon_trn_tmanualtranmatch
          select * from recon_trn_tmanualtran
          where scheduler_gid = in_scheduler_gid
          and match_gid = v_match_gid
          and delete_flag = 'N';

        delete from recon_trn_tmanualtran
        where scheduler_gid = in_scheduler_gid
        and match_gid = v_match_gid
        and delete_flag = 'N';

        -- get tran_gid
        truncate recon_tmp_tmanualtrangid;

				set v_sql = concat("
					insert into recon_tmp_tmanualtrangid (tran_gid)
						select a.tran_gid from recon_tmp_tmanualkodtl as a
						inner join ",v_tran_table," as b on a.tran_gid = b.tran_gid
							and b.excp_value = 0
							and b.delete_flag = 'N'
						where a.tranbrkp_gid = 0
						group by a.tran_gid");

				call pr_run_sql(v_sql,@msg,@result);

        -- move record(s) to tranko table
				set v_sql = concat("
					insert into ",v_tranko_table,"
						select a.* from ",v_tran_table," as a
						inner join recon_tmp_tmanualtrangid as b on a.tran_gid = b.tran_gid");

				call pr_run_sql(v_sql,@msg,@result);

				set v_sql = concat("
					delete a.* from ",v_tran_table," as a
					where a.tran_gid in
					(
						select b.tran_gid from recon_tmp_tmanualtrangid as b where a.tran_gid = b.tran_gid
					)
					and a.excp_value = 0
					and a.delete_flag = 'N'");

				call pr_run_sql(v_sql,@msg,@result);

        -- get tranbrkp_gid
        truncate recon_tmp_tmanualtrangid;

				set v_sql = concat("
					insert into recon_tmp_tmanualtrangid (tran_gid,tranbrkp_gid)
          select a.tran_gid,a.tranbrkp_gid from recon_tmp_tmanualkodtl as a
          inner join ",v_tranbrkp_table," as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
            and b.excp_value = 0
            and b.delete_flag = 'N'");

				call pr_run_sql(v_sql,@msg,@result);

        -- move record(s) to tranbrkpko table
				set v_sql = concat("
					insert into ",v_tranbrkpko_table,"
          select a.* from ",v_tranbrkp_table," as a
          inner join recon_tmp_tmanualtrangid as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
          where a.excp_value = 0
          and a.delete_flag = 'N'");

				call pr_run_sql(v_sql,@msg,@result);

				set v_sql = concat("
					delete a.* from ",v_tranbrkp_table," as a
					where a.tranbrkp_gid in
					(
						select b.tranbrkp_gid from recon_tmp_tmanualtrangid as b where a.tranbrkp_gid = b.tranbrkp_gid
					)
					and a.excp_value = 0
					and a.delete_flag = 'N'");

				call pr_run_sql(v_sql,@msg,@result);

        -- find tran_gid zero cases for breakup line
        truncate recon_tmp_tmanualtrangid;

				set v_sql = concat("
					insert into recon_tmp_tmanualtrangid (tran_gid)
						select a.tran_gid from ",v_tran_table," as a
						inner join
						(
							select a.tran_gid from recon_tmp_tmanualkodtl as a
							left join ",v_tranbrkp_table," as b on a.tran_gid = b.tran_gid
								and b.delete_flag = 'N'
							where b.tran_gid is null
							and a.tranbrkp_gid > 0
							group by a.tran_gid
						) as b on b.tran_gid = a.tran_gid
						where a.excp_value = 0
						and a.delete_flag = 'N'");

				call pr_run_sql(v_sql,@msg,@result);

				set v_sql = concat("
					insert into ",v_tranko_table,"
          select a.* from ",v_tran_table," as a
          inner join recon_tmp_tmanualtrangid as b on a.tran_gid = b.tran_gid
          where a.excp_value = 0
          and a.delete_flag = 'N'");

				call pr_run_sql(v_sql,@msg,@result);

				set v_sql = concat("
					delete a.* from ",v_tran_table," as a
					where a.tran_gid in
					(
						select b.tran_gid from recon_tmp_tmanualtrangid as b where a.tran_gid = b.tran_gid
					)
					and a.excp_value = 0
					and a.delete_flag = 'N'");

				call pr_run_sql(v_sql,@msg,@result);

        set v_succ_count = v_succ_count + 1;
      end if;
    end loop ko_loop;

    close ko_cursor;
  end ko_block;

  -- move failed record(s) to recon_trn_tmanualtranfailed table
	update recon_trn_tmanualtran set
	  ko_status = 'F'
	where scheduler_gid = in_scheduler_gid
	and delete_flag = 'N';

	insert into recon_trn_tmanualtranmatch
	  select * from recon_trn_tmanualtran
	  where scheduler_gid = in_scheduler_gid
	  and delete_flag = 'N';

	delete from recon_trn_tmanualtran
	where scheduler_gid = in_scheduler_gid
	and delete_flag = 'N';

  drop temporary table if exists recon_tmp_tmanualtrangid;
  drop temporary table if exists recon_tmp_tmanualmatchgid;
  drop temporary table if exists recon_tmp_tmanualkodtl;
  drop temporary table if exists recon_tmp_tmanualtrankodtl;

  if v_succ_count > 0 then
    set out_msg = concat('Out of ',cast(v_tot_count as nchar),' pair(s) ,',
                    cast(v_succ_count as nchar),' pair(s) KO successfully !');
    set out_result = 1;

    call pr_upd_job(v_job_gid,'C','Completed',@msg,@result);
  else
    call pr_upd_job(v_job_gid,'F','Failed',@msg,@result);

    set out_msg = 'Failed';
    set out_result = 0;
  end if;
end $$

DELIMITER ;