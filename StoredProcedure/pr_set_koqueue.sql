DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_koqueue` $$
CREATE PROCEDURE `pr_set_koqueue`()
me:BEGIN
  /*
    Created By : Muthu
    Created Date : 19-02-2025

    Updated By : Vijayavel
    updated Date : 26-03-2025

    Version : 3
  */

	DECLARE v_query TEXT;
	DECLARE v_koqueue_gid INT;
	DECLARE err_msg text default '';
	DECLARE err_flag varchar(10) default false;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
		@errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

		SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text,' ',err_msg);

		call pr_upd_koqueue(v_koqueue_gid,'F',@full_error,@msg,@result);

		set @out_msg = @full_error;
		set @out_result = 0;

		SIGNAL SQLSTATE '99999' SET
		MYSQL_ERRNO = @errno,
		MESSAGE_TEXT = @text;
	END;

  /*
	SELECT
		kq.ko_query, kq.koqueue_gid
	INTO
		@vquery, v_koqueue_gid
	FROM recon_trn_tkoqueue as kq
	left JOIN recon_trn_tjob AS b ON kq.recon_code = b.recon_code
		AND b.jobtype_code = 'A'
		AND b.job_status <> 'C'
		AND b.job_status <> 'F'
		AND b.job_status <> 'U'
		AND b.job_status <> 'R'
		AND b.delete_flag = 'N'
	WHERE kq.koqueue_status = 'I'
	AND kq.delete_flag = 'N'
	ORDER BY kq.koqueue_gid
	LIMIT 1;
  */

	SELECT
		a.ko_query, a.koqueue_gid
	INTO
		@vquery, v_koqueue_gid
	FROM recon_trn_tkoqueue as a
	left JOIN recon_trn_tkoqueue AS b ON a.recon_code = b.recon_code
    and b.koqueue_status = 'P'
		AND b.delete_flag = 'N'
	WHERE a.koqueue_status = 'I'
  and b.recon_code is null
	AND a.delete_flag = 'N'
	ORDER BY a.koqueue_gid
	LIMIT 1;

	set v_koqueue_gid = ifnull(v_koqueue_gid,0);

	if v_koqueue_gid > 0 then
		call pr_upd_koqueue(v_koqueue_gid,'P',"",@msg,@result);

		PREPARE stmt100 FROM @vquery;
		EXECUTE stmt100;
		DEALLOCATE PREPARE stmt100;
	end if;
END $$

DELIMITER ;