DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_iutsummary` $$
CREATE PROCEDURE `pr_run_iutsummary`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  /*
    Created By : Vijayavel
    Created Date : 12-06-2025

    Updated By : Vijayavel
    updated Date :

    Version : 1
  */

  declare v_report_code_cb text;
  declare v_reporttemplate_code_cb text;
	declare v_report_code_summary text;
  declare v_reporttemplate_code_summary text;

  declare v_recon_cycle_date text;

  set v_report_code_summary = 'RPT_EXCP_WITHBRKP';
  set v_reporttemplate_code_summary = 'RT413';

  set v_report_code_cb = 'DS426';
  set v_reporttemplate_code_cb = 'RT431';

  -- get recon details
  select
    date_format(recon_cycle_date,'%Y-%m-%d') as cycle_date
  into
    v_recon_cycle_date
  from recon_mst_trecon
  where recon_code = in_recon_code
  and active_status = 'Y'
  and delete_flag = 'N';

  -- IUT Summary
  call pr_run_dynamicreport(v_reporttemplate_code_summary, in_recon_code,v_report_code_summary,
    'IUT Summary','and a.scheduler_gid > 0 ', false, '', '', in_user_code, @out_msg, @out_result);

	-- Closing Balance
	call pr_run_dynamicreport(v_reporttemplate_code_cb, in_recon_code,v_report_code_cb,
        'CB',
        concat("and col12 = '",v_recon_cycle_date,"' and col45 <> '' "),
        false, '', '', in_user_code, @out_msg, @out_result);
end $$

DELIMITER ;