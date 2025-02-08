DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_brsnbfcsummary` $$
CREATE PROCEDURE `pr_run_brsnbfcsummary`
(
  in in_recon_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_reporttemplate_code_es text;
	declare v_report_code_es text;

  -- fetching reportcode and report template code for Custom report Formatted Exception
	select
		report_code,reporttemplate_code
	into
		v_report_code_es,v_reporttemplate_code_es
  from recon_mst_treporttemplate
  where recon_code = in_recon_code
  and reporttemplate_name = 'Formatted Exception'
  and active_status = 'Y'
	and delete_flag = 'N';

  call pr_run_dynamicreport(v_reporttemplate_code_es, in_recon_code,v_report_code_es,
    'Transaction Exception With Breakp','and a.scheduler_gid > 0 ', false, '', '', in_user_code, @out_msg, @out_result);


  call pr_get_datasetclosingbal(in_recon_code,sysdate());
end $$

DELIMITER ;