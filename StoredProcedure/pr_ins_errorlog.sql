DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_ins_errorlog` $$
CREATE PROCEDURE `pr_ins_errorlog`(
  in in_user_code varchar(32),
  in in_ip_addr varchar(255),
  in in_source_name varchar(128),
  in in_proc_name varchar(128),
  in in_errorlog_text text,
  out out_msg text,
  out out_result int(10)
)
me:BEGIN
  declare err_msg text default '';
  declare err_flag boolean default false;

  insert into recon_trn_terrorlog
  (
    entry_date,
    user_code,
    ip_addr,
    source_name,
    proc_name,
    errorlog_text
  )
  values
  (
    sysdate(),
    in_user_code,
    in_ip_addr,
    in_source_name,
    in_proc_name,
    in_errorlog_text
  );

  set out_result = 1;
  set out_msg = 'Record saved successfully !';
 END $$

DELIMITER ;