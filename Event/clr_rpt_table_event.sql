DELIMITER $$

CREATE EVENT `clr_rpt_table_event`
	ON SCHEDULE EVERY 1 DAY STARTS '2024-10-29 23:50:00'
	DO BEGIN

    call pr_set_clearrpttable();

	END $$

DELIMITER ;