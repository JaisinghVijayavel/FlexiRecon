DELIMITER $$
CREATE EVENT `run_koqueue_event` ON SCHEDULE EVERY 5 SECOND
  DO BEGIN
    -- Call the stored procedure
    CALL pr_set_koqueue();

    -- Log each execution
    -- INSERT INTO event_log (event_name) VALUES ('run_koqueue_event');
  END $$
DELIMITER ;