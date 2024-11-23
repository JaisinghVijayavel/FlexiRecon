DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_manualtranbrkpfile` $$
CREATE PROCEDURE `pr_run_manualtranbrkpfile`
(
  in in_scheduler_gid int,
  in in_ip_addr varchar(255),
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_recon_code text default '';
  declare v_dataset_code text default '';
  declare v_rec_count int default 0;
  declare v_tot_count int default 0;
  declare v_succ_count int default 0;
  declare v_txt text default '';
  declare v_job_gid int default 0;

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
    recon_code,
    dataset_code
  into
    v_recon_code,
    v_dataset_code
  from recon_trn_tmanualtranbrkp
  where scheduler_gid = in_scheduler_gid
  and delete_flag = 'N'
  limit 0,1;

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
	  call pr_ins_job(v_recon_code,'M',in_scheduler_gid,concat('Manual posting...'),'',
      in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);
	end if;

  set v_job_gid = @out_job_gid;

  truncate recon_tmp_ttrangid;

  insert into recon_tmp_ttrangid
    select
      a.tran_gid,
      b.excp_value,
      sum(c.excp_value*c.tran_mult) as match_value,
      b.tran_mult
    from recon_trn_tmanualtranbrkp as a
    inner join recon_trn_ttran as b
      on a.tran_gid = b.tran_gid
      and b.excp_value = b.tran_value
      and b.mapped_value = 0
      and b.delete_flag = 'N'
    inner join recon_trn_ttranbrkp as c
      on a.tranbrkp_gid = c.tranbrkp_gid
      and c.recon_code = b.recon_code
      and c.dataset_code = b.dataset_code
      and c.excp_value > 0
      and c.excp_value = c.tran_value
      and c.tran_gid = 0
      and c.delete_flag = 'N'
    where a.scheduler_gid = in_scheduler_gid
    and a.delete_flag = 'N'
    group by a.tran_gid,b.excp_value,b.tran_mult
    having sum(c.excp_value*c.tran_mult) = (b.excp_value*b.tran_mult);

    -- and a.dataset_code = v_dataset_code

  /*
  update recon_trn_tmanualtranbrkp set
    tranbrkp_status = 'M'
  where scheduler_gid = in_scheduler_gid
  and tran_gid in (select tran_gid from recon_tmp_ttrangid)
  and tranbrkp_status = 'P'
  and delete_flag = 'N';

  update recon_trn_ttran set
    mapped_value = tran_value
  where tran_gid in (select tran_gid from recon_tmp_ttrangid)
  and excp_value = tran_value
  and mapped_value = 0
  and delete_flag = 'N';
  */

  update recon_trn_tmanualtranbrkp as a
  inner join recon_tmp_ttrangid as b on a.tran_gid = b.tran_gid
  set
    a.tranbrkp_status = 'M'
  where a.scheduler_gid = in_scheduler_gid
  and a.tranbrkp_status = 'P'
  and a.delete_flag = 'N';

  update recon_trn_ttran as a
  inner join recon_tmp_ttrangid as b on a.tran_gid = b.tran_gid
  set
    a.mapped_value = a.tran_value
  where a.excp_value = a.tran_value
  and a.mapped_value = 0
  and a.excp_value > 0
  and a.delete_flag = 'N';

  update recon_trn_tmanualtranbrkp as a
  inner join recon_trn_ttranbrkp as b
    on a.tranbrkp_gid = b.tranbrkp_gid
    and b.excp_value > 0
    and b.excp_value = b.tran_value
    and b.tran_gid = 0
    and b.delete_flag = 'N'
  set
    b.tran_gid = a.tran_gid
  where a.scheduler_gid = in_scheduler_gid
  and a.tranbrkp_status = 'M'
  and a.delete_flag = 'N';

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