DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_upd_job` $$
CREATE PROCEDURE `pr_upd_job`(
  in in_job_gid int,
  in in_job_status varchar(32),
  in in_job_remark varchar(255),
  out out_msg text,
  out out_result int(10)
)
BEGIN
  IF in_job_status = 'C' or in_job_status = 'F' then
		update recon_trn_tjob set
      end_date = sysdate(),
      job_status = in_job_status,
      job_remark = in_job_remark,
      update_date = sysdate()
    where job_gid = in_job_gid
    and delete_flag = 'N';

	ELSE
		update recon_trn_tjob set
      job_status = in_job_status,
      job_remark = in_job_remark,
      update_date = sysdate()
    where job_gid = in_job_gid
    and delete_flag = 'N';
	END IF;

  set out_result = 1;
  set out_msg = 'Record updated successfully !';
END $$

DELIMITER ;