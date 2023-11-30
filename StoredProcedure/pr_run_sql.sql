DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_sql` $$
CREATE PROCEDURE `pr_run_sql`
(
  in in_sql text,
  out out_msg text,
  out out_result int
)
begin
  set @sql = in_sql;
  prepare stmt from @sql;
  execute stmt;
  deallocate prepare stmt;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;