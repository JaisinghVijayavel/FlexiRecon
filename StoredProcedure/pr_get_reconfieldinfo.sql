DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_reconfieldinfo` $$
CREATE PROCEDURE `pr_get_reconfieldinfo`
(
  in in_recon_code varchar(32),
  out out_msg text,
  out out_result int
)
BEGIN
  /*
    Created By : Vijayavel
    Created Date: 10-01-2026

    Updated By : Vijayavel
    updated Date : 13-01-2026

	  Version - 2
  */
  set out_result = 0;
  set out_msg = 'Failed';

  -- get reconfield info
  select fn_get_reconconfigvalue(in_recon_code,'Field Update Report') as report_code,
         fn_get_reconconfigvalue(in_recon_code,'Field Update Report Template') as reporttemplate_code,
         fn_get_reconconfigvalue(in_recon_code,'Field Update Report Resultset') as reporttemplateresultset_code,
         fn_get_reconconfigvalue(in_recon_code,'Field Update Report Column Position tran_gid') as position_tran_gid,
         fn_get_reconconfigvalue(in_recon_code,'Field Update Report Column Position tranbrkp_gid') as position_tranbrkp_gid,
         fn_get_reconconfigvalue(in_recon_code,'Field Update URL') as field_update_url;

  set out_result = 1;
  set out_msg = 'Success';
END $$

DELIMITER ;