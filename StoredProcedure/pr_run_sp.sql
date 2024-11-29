DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_sp` $$
CREATE PROCEDURE `pr_run_sp`(
  in in_recon_code text,
  in in_sp_name text,
  in in_job_gid int,
  in in_rptsession_gid int,
  in in_condition text,
  in in_sorting_order text,
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
begin
  if in_sp_name = 'pr_run_koreport' then
    call pr_run_koreport(in_job_gid,in_rptsession_gid,in_condition,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_koheadreport' then
    call pr_run_koheadreport(in_job_gid,in_rptsession_gid,in_condition,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_kobrkpreport' then
    call pr_run_kobrkpreport(in_job_gid,in_rptsession_gid,in_condition,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_tranbrkpreport' then
    call pr_run_tranbrkpreport(in_recon_code,in_job_gid,in_rptsession_gid,in_condition,in_sorting_order,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_amountmatchedmultiple' then
    call pr_run_amountmatchedmultiple(in_recon_code,in_job_gid,in_rptsession_gid,in_condition,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_tranreport' then
    call pr_run_tranreport(in_recon_code,in_job_gid,in_rptsession_gid,in_condition,in_sorting_order,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_manualmatchreport' then
    call pr_run_manualmatchreport(in_job_gid,in_rptsession_gid,in_condition,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_manualpostreport' then
    call pr_run_manualpostreport(in_job_gid,in_rptsession_gid,in_condition,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_kotranreport' then
    call pr_run_kotranreport(in_job_gid,in_rptsession_gid,in_condition,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_tranwithbrkpreport' then
    call pr_run_tranwithbrkpreport(in_recon_code,in_job_gid,in_rptsession_gid,in_condition,in_sorting_order,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_errorlogreport' then
    call pr_run_errorlogreport(in_job_gid,in_rptsession_gid,in_condition,in_sorting_order,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_accbalreport' then
    call pr_run_accbalreport(in_job_gid,in_rptsession_gid,in_condition,in_sorting_order,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_pdtranreport' then
    call pr_run_pdtranreport(in_recon_code,in_job_gid,in_rptsession_gid,in_condition,in_sorting_order,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_pdtranbrkpreport' then
    call pr_run_pdtranbrkpreport(in_recon_code,in_job_gid,in_rptsession_gid,in_condition,in_sorting_order,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_pdtranwithbrkpreport' then
    call pr_run_pdtranwithbrkpreport(in_recon_code,in_job_gid,in_rptsession_gid,in_condition,in_sorting_order,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_pdtranwithbrkpexcprpt' then
    call pr_run_pdtranwithbrkpexcprpt(in_recon_code,in_job_gid,in_rptsession_gid,in_condition,in_sorting_order,in_user_code,@msg,@result);
  elseif in_sp_name = 'pr_run_pdkoreport' then
    call pr_run_pdkoreport(in_job_gid,in_rptsession_gid,in_condition,in_user_code,@msg,@result);
  end if;

  set out_msg = @msg;
  set out_result = @result;
end $$

DELIMITER ;