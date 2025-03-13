DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_reconrule` $$
CREATE PROCEDURE `pr_run_reconrule`
(
  in in_recon_code text,
  in in_period_from date,
  in in_period_to date,
  in in_automatch_flag char(1),
  in in_ip_addr varchar(255),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By - Muthu
    Created Date - 2025-02-19

    Updated By - Vijayavel
    Updated Date - 2025-03-10

	  Version - 002
	*/

  declare v_query text default '';
  declare err_msg text default '';
  declare err_flag varchar(10) default false;
  declare v_txt text default '';
  declare v_count integer default 0;
  declare v_recon_koqueue_max integer default 0;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text,' ',err_msg);

    ROLLBACK;

    -- call pr_upd_koqueue(v_job_gid,'F',@full_error,@msg,@result);

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;

  set v_txt = fn_get_configvalue('recon_koqueue_max');

  if v_txt = '' then
    set v_txt = '0';
  end if;

  set v_recon_koqueue_max = cast(v_txt as unsigned);

  if v_recon_koqueue_max <= 0 then
    set v_recon_koqueue_max = 1;
  end if;

  select
    count(*)
  into
    v_count
  from recon_trn_tkoqueue
  where recon_code = in_recon_code
  and koqueue_status in ('I','P')
  and delete_flag = 'N';

  if (v_count < v_recon_koqueue_max)then
	  set v_query = Concat("call pr_run_reconrulerecursion
    (
      '",in_recon_code,"',
      '",in_period_from,"',
      '",in_period_to,"',
      '",in_automatch_flag,"',
      '",in_ip_addr,"',
      '",in_user_code,"',
      @out_msg,
      @out_result
    );");

    START TRANSACTION;

	  insert into recon_trn_tkoqueue(recon_code,ko_query,koqueue_status,scheduled_date,scheduled_by,delete_flag)
	  values (in_recon_code,v_query,'I',now(),in_user_code,'N');

    COMMIT;

    set out_result = 1;
    set out_msg = 'Success';
  else
    set out_result = 0;
    set out_msg = concat('Falied ! Reached max limit ',cast(v_recon_koqueue_max as nchar),' !');
  end if;

end $$

DELIMITER ;