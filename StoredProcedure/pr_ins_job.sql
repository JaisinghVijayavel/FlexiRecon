DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_ins_job` $$
CREATE PROCEDURE `pr_ins_job`(
  in in_recon_code varchar(32),
  in in_jobtype_code varchar(32),
  in in_job_ref_gid int,
  in in_job_name varchar(255),
  in in_job_input_param text,
  in in_job_initiated_by varchar(255),
  in in_ip_addr varchar (255),
  in in_job_status varchar(32),
  in in_job_remark varchar(255),
  out out_job_gid int,
  out out_msg text,
  out out_result int(10)
)
me:BEGIN
  if in_job_ref_gid > 0 and in_jobtype_code = 'S' then
    if exists(select job_gid from recon_trn_tjob
      where jobtype_code = in_jobtype_code
      and job_ref_gid = in_job_ref_gid
      and delete_flag = 'N') then
      leave me;
    end if;
  end if;

  insert into recon_trn_tjob
  (
    recon_code,
    jobtype_code,
    job_ref_gid,
    job_input_param,
    job_name,
    job_initiated_by,
    start_date,
    ip_addr,
    job_status,
    job_remark
  )
  values
  (
    in_recon_code,
    in_jobtype_code,
    in_job_ref_gid,
    in_job_input_param,
    in_job_name,
    in_job_initiated_by,
    SYSDATE(),
    in_ip_addr,
    in_job_status,
    in_job_remark
  );

  select last_insert_id() into @jobId;

  if(in_jobtype_code = 'F') then
    update recon_trn_tscheduler
    set job_gid = @jobId
    where scheduler_gid = in_job_ref_gid
    and delete_flag = 'N';
  end if;

  select @jobId into out_job_gid;
  set out_result = 1;
  set out_msg = 'Record saved successfully !';
END $$

DELIMITER ;