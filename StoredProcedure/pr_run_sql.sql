﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_run_sql` $$
CREATE PROCEDURE `pr_run_sql`
(
  in in_sql text,
  out out_msg text,
  out out_result int
)
begin
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    ROLLBACK;

    call pr_ins_errorlog('system','localhost','sp','pr_run_sql',in_sql,@msg,@result);

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;

  set @sql = in_sql;
  prepare sql_stmt from @sql;
  execute sql_stmt;
  deallocate prepare sql_stmt;

  set out_msg = 'Success';
  set out_result = 1;
end $$

DELIMITER ;