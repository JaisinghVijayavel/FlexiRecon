DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_manualmatchfile` $$
CREATE PROCEDURE `pr_run_manualmatchfile`
(
  in in_scheduler_gid int,
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

  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_tmatchgid;
  drop temporary table if exists recon_tmp_tkodtl;

  CREATE temporary TABLE recon_tmp_ttrangid(
    tran_gid int(10) unsigned NOT NULL,
    tranbrkp_gid int(10) unsigned NOT NULL,
    dataset_code varchar(32) default null,
    dataset_type varchar(32) default null,
    excp_value decimal(15,2) not null default 0,
    ko_value decimal(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    PRIMARY KEY (tran_gid,tranbrkp_gid)
  ) ENGINE = MyISAM;

  CREATE temporary TABLE recon_tmp_tmatchgid(
    match_gid int(10) unsigned NOT NULL,
    ko_source_value decimal(15,2) not null default 0,
    ko_comparison_value decimal(15,2) not null default 0,
    ko_diff_value decimal(15,2) not null default 0,
    ko_reason text default null,
    PRIMARY KEY (match_gid)
  ) ENGINE = MyISAM;

  create temporary table recon_tmp_tkodtl(
    kodtl_gid int unsigned NOT NULL AUTO_INCREMENT,
    ko_gid int unsigned NOT NULL,
    tran_gid int unsigned NOT NULL,
    tranbrkp_gid int unsigned not null default 0,
    ko_value decimal(15,2) not null default 0,
    ko_mult tinyint not null default 0,
    PRIMARY KEY (kodtl_gid),
    key idx_ko_gid(ko_gid),
    key idx_tran_gid(tran_gid),
    key idx_tranbrkp_gid(tranbrkp_gid)
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

  -- update ko_mult in manualtran table
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

  -- validate tran_gid
  truncate recon_tmp_ttrangid;

  if v_recontype_code <> 'N' then
    -- insert in trangid table where tranbrkp_gid = 0
    insert into recon_tmp_ttrangid
      select
        a.tran_gid,
        0 as tranbrkp_gid,
        b.dataset_code,
        c.dataset_type,
        b.excp_value,
        sum(a.ko_value) as ko_value,
        b.tran_mult
      from recon_trn_tmanualtran as a
      inner join recon_trn_ttran as b
        on a.tran_gid = b.tran_gid
        and b.recon_code = a.recon_code
        and a.ko_value <= b.excp_value
        and b.excp_value <> 0
        -- and b.mapped_value = 0
        and b.delete_flag = 'N'
      inner join recon_mst_trecondataset as c on b.dataset_code = c.dataset_code and c.delete_flag = 'N'
      where a.scheduler_gid = in_scheduler_gid
      and a.tranbrkp_gid = 0
      and a.delete_flag = 'N'
      group by a.tran_gid,b.dataset_code,b.excp_value,b.tran_mult;

    -- insert in trangid table where tranbrkp_gid > 0
    insert into recon_tmp_ttrangid
      select
        a.tran_gid,
        a.tranbrkp_gid,
        b.dataset_code,
        c.dataset_type,
        b.excp_value,
        a.ko_value,
        b.tran_mult
      from recon_trn_tmanualtran as a
      inner join recon_trn_ttranbrkp as b
        on a.tran_gid = b.tran_gid
        and a.tranbrkp_gid = b.tranbrkp_gid
        and b.recon_code = a.recon_code
        and a.ko_value <= b.excp_value
        and b.excp_value > 0
        and b.delete_flag = 'N'
      inner join recon_mst_trecondataset as c on b.dataset_code = c.dataset_code and c.delete_flag = 'N'
      where a.scheduler_gid = in_scheduler_gid
      and a.delete_flag = 'N';
  else
    insert into recon_tmp_ttrangid
      select
        a.tran_gid,
        b.dataset_code,
        c.dataset_type,
        0,
        0,
        0
      from recon_trn_tmanualtran as a
      inner join recon_trn_ttran as b
        on a.tran_gid = b.tran_gid
        and b.recon_code = a.recon_code
        and b.ko_gid = 0
        and b.delete_flag = 'N'
      inner join recon_mst_trecondataset as c on b.dataset_code = c.dataset_code and c.delete_flag = 'N'
      where a.scheduler_gid = in_scheduler_gid
      and a.delete_flag = 'N'
      group by a.tran_gid,b.dataset_code,c.dataset_type;
  end if;

  -- validate match_gid
  truncate recon_tmp_tmatchgid;

  if v_recontype_code = 'W' or v_recontype_code = 'B' then
    insert into recon_tmp_tmatchgid
      select
        a.match_gid,
        sum(if(a.ko_mult = -1,a.ko_value,0)) as ko_source_value,
        sum(if(a.ko_mult = 1,a.ko_value,0)) as ko_comparison_value,
        sum(a.ko_value*a.ko_mult) as ko_diff_value,
        max(a.ko_reason) as ko_reason
      from recon_trn_tmanualtran as a
      inner join recon_tmp_ttrangid as b
        on a.tran_gid = b.tran_gid
        and a.tranbrkp_gid = b.tranbrkp_gid
        and b.excp_value <> 0
        and b.excp_value >= a.ko_value
      where a.scheduler_gid = in_scheduler_gid
      and a.delete_flag = 'N'
      group by a.match_gid
      having sum(a.ko_value*b.tran_mult) = 0;
  elseif v_recontype_code = 'I' or v_recontype_code = 'V' then
    insert into recon_tmp_tmatchgid
      select
        a.match_gid,
        sum(if(b.dataset_type='B',a.ko_value*b.tran_mult,0)) as ko_source_value,
        sum(if(b.dataset_type='T',a.ko_value*b.tran_mult,0)) as ko_comparison_value,
        sum(a.ko_value*b.tran_mult) as ko_diff_value,
        max(a.ko_reason) as ko_reason
      from recon_trn_tmanualtran as a
      inner join recon_tmp_ttrangid as b
        on a.tran_gid = b.tran_gid
        and a.tranbrkp_gid = b.tranbrkp_gid
        and b.excp_value <> 0
        and b.excp_value >= a.ko_value
      where a.scheduler_gid = in_scheduler_gid
      and a.delete_flag = 'N'
      group by a.match_gid
      having sum(if(b.dataset_type='B',a.ko_value*b.tran_mult,0)) = sum(if(b.dataset_type='T',a.ko_value*b.tran_mult,0));
  elseif v_recontype_code = 'N' then
    insert into recon_tmp_tmatchgid
      select
        a.match_gid,
        0 as ko_source_value,
        0 as ko_comparison_value,
        0 as ko_diff_value,
        max(a.ko_reason) as ko_reason
      from recon_trn_tmanualtran as a
      inner join recon_tmp_ttrangid as b
        on a.tran_gid = b.tran_gid
      where a.scheduler_gid = in_scheduler_gid
      and a.delete_flag = 'N'
      group by a.match_gid;
  end if;

  ko_block:begin
    declare ko_done int default 0;
    declare ko_cursor cursor for
      select match_gid,ko_source_value,ko_reason from recon_tmp_tmatchgid;
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
        truncate recon_tmp_tkodtl;

        if v_recontype_code <> 'N' then
          insert into recon_tmp_tkodtl (ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult)
            select
              v_ko_gid,
              a.tran_gid,
              a.tranbrkp_gid,
              a.ko_value,
              b.tran_mult
            from recon_trn_tmanualtran as a
            inner join recon_trn_ttran as b
              on a.tran_gid = b.tran_gid
              and b.excp_value <> 0
              and b.excp_value >= a.ko_value
              and b.delete_flag = 'N'
            where a.scheduler_gid = in_scheduler_gid
            and a.match_gid = v_match_gid
            and a.tranbrkp_gid = 0
            and a.ko_gid = 0
            and a.delete_flag = 'N';

          insert into recon_tmp_tkodtl (ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult)
            select
              v_ko_gid,
              a.tran_gid,
              a.tranbrkp_gid,
              a.ko_value,
              b.tran_mult
            from recon_trn_tmanualtran as a
            inner join recon_trn_ttranbrkp as b
              on a.tran_gid = b.tran_gid
              and a.tranbrkp_gid = b.tranbrkp_gid
              and b.excp_value > 0
              and b.excp_value >= a.ko_value
              and b.delete_flag = 'N'
            where a.scheduler_gid = in_scheduler_gid
            and a.match_gid = v_match_gid
            and a.ko_gid = 0
            and a.delete_flag = 'N';

          select abs(sum(ko_value*ko_mult)) into v_value from recon_tmp_tkodtl;

          if (v_value > 0 and v_recontype_code = 'I') or v_value = 0 then
            insert into recon_trn_tkodtl (ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult)
              select ko_gid,tran_gid,tranbrkp_gid,ko_value,ko_mult from recon_tmp_tkodtl;
            /*
              select a.ko_gid,a.tran_gid,a.ko_value,a.tran_mult from recon_tmp_tkodtl as a
              inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid
                and b.excp_value > 0
                and b.delete_flag = 'N';
            */

            update recon_trn_ttran as a
            inner join recon_tmp_tkodtl as b on a.tran_gid = b.tran_gid
            set a.excp_value = a.excp_value - (b.ko_value*b.ko_mult)*a.tran_mult,
              a.ko_gid = b.ko_gid,
              a.ko_date = curdate()
            where a.excp_value <> 0
            and a.delete_flag = 'N';

            update recon_trn_ttranbrkp as a
            inner join recon_tmp_tkodtl as b on a.tran_gid = b.tran_gid
              and a.tranbrkp_gid = b.tranbrkp_gid
            set a.excp_value = 0,
              a.ko_gid = b.ko_gid,
              a.ko_date = curdate()
            where a.excp_value > 0

            and a.delete_flag = 'N';
          end if;
        else
          insert into recon_tmp_tkodtl (ko_gid,tran_gid,ko_value)
            select
              v_ko_gid,
              a.tran_gid,
              a.ko_value
            from recon_trn_tmanualtran as a
            inner join recon_trn_ttran as b
              on a.tran_gid = b.tran_gid
              and b.delete_flag = 'N'
            where a.scheduler_gid = in_scheduler_gid
            and a.match_gid = v_match_gid
            and a.ko_gid = 0
            and a.delete_flag = 'N';

          insert into recon_trn_tkodtl (ko_gid,tran_gid,ko_value)
            select a.ko_gid,a.tran_gid,a.ko_value from recon_tmp_tkodtl as a
            inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid
            and b.delete_flag = 'N';

          update recon_trn_ttran as a
          inner join recon_tmp_tkodtl as b on a.tran_gid = b.tran_gid
          set a.ko_gid = b.ko_gid,
              a.ko_date = curdate()
          where a.delete_flag = 'N';
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
        truncate recon_tmp_ttrangid;

        insert into recon_tmp_ttrangid (tran_gid)
          select a.tran_gid from recon_tmp_tkodtl as a
          inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid
            and b.excp_value = 0
            and b.delete_flag = 'N'
          where a.tranbrkp_gid = 0
          group by a.tran_gid;

        -- move record(s) to tranko table
        insert into recon_trn_ttranko
          select a.* from recon_trn_ttran as a
          inner join recon_tmp_ttrangid as b on a.tran_gid = b.tran_gid;

        delete from recon_trn_ttran
        where tran_gid in
        (
          select tran_gid from recon_tmp_ttrangid
        )
        and excp_value = 0
        and delete_flag = 'N';

        -- get tranbrkp_gid
        truncate recon_tmp_ttrangid;

        insert into recon_tmp_ttrangid (tran_gid,tranbrkp_gid)
          select a.tran_gid,a.tranbrkp_gid from recon_tmp_tkodtl as a
          inner join recon_trn_ttranbrkp as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
            and b.excp_value = 0
            and b.delete_flag = 'N';

        -- move record(s) to tranbrkpko table
        insert into recon_trn_ttranbrkpko
          select a.* from recon_trn_ttranbrkp as a
          inner join recon_tmp_ttrangid as b on a.tran_gid = b.tran_gid and a.tranbrkp_gid = b.tranbrkp_gid
          where a.excp_value = 0
          and a.delete_flag = 'N';

        delete from recon_trn_ttranbrkp
        where tranbrkp_gid in
        (
          select tranbrkp_gid from recon_tmp_ttrangid
        )
        and excp_value = 0
        and delete_flag = 'N';

        -- find tran_gid zero cases for breakup line
        truncate recon_tmp_ttrangid;

        insert into recon_tmp_ttrangid (tran_gid)
          select a.tran_gid from recon_trn_ttran as a
          inner join
          (
            select a.tran_gid from recon_tmp_tkodtl as a
            left join recon_trn_ttranbrkp as b on a.tran_gid = b.tran_gid
              and b.delete_flag = 'N'
            where b.tran_gid is null
            and a.tranbrkp_gid > 0
            group by a.tran_gid
          ) as b on b.tran_gid = a.tran_gid
          where a.excp_value = 0
          and a.delete_flag = 'N';

        insert into recon_trn_ttranko
          select a.* from recon_trn_ttran as a
          inner join recon_tmp_ttrangid as b on a.tran_gid = b.tran_gid
          where a.excp_value = 0
          and a.delete_flag = 'N';

        delete from recon_trn_ttran
        where tran_gid in
        (
          select tran_gid from recon_tmp_ttrangid
        )
        and excp_value = 0
        and delete_flag = 'N';

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

  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_tmatchgid;
  drop temporary table if exists recon_tmp_tkodtl;

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