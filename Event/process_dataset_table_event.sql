DELIMITER $$

CREATE EVENT `process_dataset_table_event`
	ON SCHEDULE EVERY 1 SECOND
	DO BEGIN
    call pr_set_move_dataset();
	END $$

DELIMITER ;