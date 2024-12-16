DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_undoiutentry` $$
CREATE PROCEDURE `pr_run_undoiutentry`
(
  in in_recon_code varchar(32),
  in in_iutentryref_no text,
  out out_msg text,
  out out_result int
)
me:begin
end $$

DELIMITER ;