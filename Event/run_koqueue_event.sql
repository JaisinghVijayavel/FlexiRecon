DELIMITER $$
DROP EVENT IF EXISTS run_koqueue_event;
CREATE EVENT `run_koqueue_event`
  ON SCHEDULE EVERY 5 Second
DO BEGIN
    -- Call the stored procedure
    CALL pr_set_koqueue();

    -- Log each execution
    INSERT INTO event_log (event_name) VALUES ('run_koqueue_event');
END $$
DELIMITER ;