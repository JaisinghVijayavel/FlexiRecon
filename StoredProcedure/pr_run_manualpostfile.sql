DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_manualpostfile` $$
CREATE PROCEDURE `pr_run_manualpostfile`
(
  in in_scheduler_gid int,
	in in_recon_code varchar(32),
  in in_ip_addr varchar(255),
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_recon_code text default '';
  declare v_recontype_code text default '';
  declare v_dataset_code text default '';
  declare v_rec_count int default 0;
  declare v_tot_count int default 0;
  declare v_succ_count int default 0;
  declare v_txt text default '';
  declare v_sql text default '';
  declare v_job_gid int default 0;
  declare v_file_name text default '';
	
	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

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

	-- set tran table
  /*
	set v_tran_table = concat(in_recon_code,'_tran');
	set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  */

	set v_tran_table = 'recon_trn_ttran';
	set v_tranbrkp_table = 'recon_trn_ttranbrkp';

  drop temporary table if exists recon_tmp_ttrangid;

  CREATE temporary TABLE recon_tmp_ttrangid(
    tran_gid int(10) unsigned NOT NULL,
    excp_value decimal(15,2) not null default 0,
    match_value decimal(15,2) not null default 0,
    tran_mult tinyint not null default 0,
    PRIMARY KEY (tran_gid)
  ) ENGINE = MyISAM;

  if not exists(select scheduler_gid from con_trn_tscheduler
     where scheduler_gid = in_scheduler_gid and delete_flag = 'N') then
    set out_msg = 'Invalid scheduler !';
    set out_result = 0;
    leave me;
  end if;

  select
    a.recon_code,
    b.recontype_code,
    a.dataset_code
  into
    v_recon_code,
    v_recontype_code,
    v_dataset_code
  from recon_trn_tmanualtranbrkp as a
  inner join recon_mst_trecon as b on a.recon_code = b.recon_code
    and b.delete_flag = 'N'
  where a.scheduler_gid = in_scheduler_gid
  and a.delete_flag = 'N'
  limit 0,1;

  set v_recon_code = ifnull(v_recon_code,'');
  set v_recontype_code = ifnull(v_recontype_code,'');

  -- get file name
  select
    file_name into v_file_name
  from con_trn_tscheduler
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  -- get record count
  select
    count(*),
    count(distinct tran_gid)
  into
    v_rec_count,
    v_tot_count
  from recon_trn_tmanualtranbrkp
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N';

  set v_rec_count = ifnull(v_rec_count,0);
  set v_tot_count = ifnull(v_tot_count,0);

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
	  call pr_ins_job(v_recon_code,'M',in_scheduler_gid,concat('Manual posting - ',v_file_name),v_file_name,
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

  truncate recon_tmp_ttrangid;

	set v_sql = concat("
		insert into recon_tmp_ttrangid
			select
				a.tran_gid,
				b.excp_value,
				sum(a.tranbrkp_value*c.tran_mult) as match_value,
				b.tran_mult
			from recon_trn_tmanualtranbrkp as a
			inner join ",v_tran_table," as b
				on a.tran_gid = b.tran_gid
				and b.recon_code = a.recon_code
				and b.excp_value >= a.tranbrkp_value
				and b.excp_value = b.tran_value
				and b.mapped_value = 0
				and b.delete_flag = 'N'
			inner join ",v_tranbrkp_table," as c
				on a.tranbrkp_gid = c.tranbrkp_gid
				and c.excp_value > 0
				and c.excp_value = a.tranbrkp_value
				and c.tran_gid = 0
				and c.delete_flag = 'N'
			where a.scheduler_gid = ",cast(in_scheduler_gid as nchar),"
			and a.dataset_code = '",v_dataset_code,"'
			and a.delete_flag = 'N'
			group by a.tran_gid,b.excp_value,b.tran_mult
			having sum(a.tranbrkp_value*c.tran_mult) = (b.excp_value*b.tran_mult)");
	
	call pr_run_sql(v_sql,@msg,@result);

  update recon_trn_tmanualtranbrkp as a set
    a.tranbrkp_status = 'M'
  where a.scheduler_gid = in_scheduler_gid
  and a.tran_gid in (select b.tran_gid from recon_tmp_ttrangid as b where a.tran_gid = b.tran_gid)
  and a.tranbrkp_status = 'P'
  and a.delete_flag = 'N';

	set v_sql = concat("
		update ",v_tran_table," as a set
			a.mapped_value = b.tran_value
		where a.tran_gid in (select b.tran_gid from recon_tmp_ttrangid as b where a.tran_gid = b.tran_gid)
		and a.excp_value = tran_value
		and a.mapped_value = 0
		and a.delete_flag = 'N'");
	
	call pr_run_sql(v_sql,@msg,@result);

	set v_sql = concat("
		update recon_trn_tmanualtranbrkp as a
		inner join ",v_tranbrkp_table," as b
			on a.tranbrkp_gid = b.tranbrkp_gid
			and b.excp_value > 0
			and b.excp_value = a.tranbrkp_value
			and b.tran_gid = 0
			and b.delete_flag = 'N'
		set
			b.tran_gid = a.tran_gid
		where a.scheduler_gid = in_scheduler_gid
		and a.tranbrkp_status = 'M'
		and a.delete_flag = 'N'");

	call pr_run_sql(v_sql,@msg,@result);

  update recon_trn_tmanualtranbrkp set
    tranbrkp_status = 'C'
  where scheduler_gid = in_scheduler_gid
  and tranbrkp_status = 'M'
  and delete_flag = 'N';

  update recon_trn_tmanualtranbrkp set
    tranbrkp_status = 'F'
  where scheduler_gid = in_scheduler_gid
  and tranbrkp_status = 'P'
  and delete_flag = 'N';

  select
    count(*) into v_succ_count
  from recon_trn_tmanualtranbrkp
  where scheduler_gid = in_scheduler_gid
  and tranbrkp_status = 'C'
  and delete_flag = 'N';

  set v_succ_count = ifnull(v_succ_count,0);

	insert into recon_trn_tmanualtranbrkppost
	  select * from recon_trn_tmanualtranbrkp
	  where scheduler_gid = in_scheduler_gid
	  and delete_flag = 'N';

	delete from recon_trn_tmanualtranbrkp
	where scheduler_gid = in_scheduler_gid
	and delete_flag = 'N';

  drop temporary table if exists recon_tmp_ttrangid;

  call pr_upd_job(v_job_gid,'C','Completed',@msg,@result);

  set out_msg = concat(cast(v_succ_count as nchar),' record(s) posted successfully !');
  set out_result = 1;
end $$

DELIMITER ;