DELIMITER $$

CREATE EVENT `run_koqueue_event` ON SCHEDULE EVERY 59 SECOND STARTS '2024-11-25 15:37:22' ON COMPLETION NOT PRESERVE ENABLE
DO BEGIN
    -- Call the stored procedure
    CALL pr_set_koqueue();

    -- Log each execution
    INSERT INTO event_log (event_name) VALUES ('run_koqueue_event');
END $$
DELIMITER ;