DELIMITER $$
DROP PROCEDURE IF EXISTS `pr_set_move_dataset` $$
CREATE procedure `pr_set_move_dataset`()
me:begin
  /*
    Created By : Vijayavel
    Created Date : 26-12-2023

    Updated By :
    Updated Date :

    Version : 1
  */

  declare v_scheduler_gid int default 0;
  declare v_insert_by text default '';

	scheduler_block:begin
		declare scheduler_done int default 0;
		declare scheduler_cursor cursor for
			select scheduler_gid,insert_by from recon_trn_tscheduler
			where scheduler_status = 'S'
			and delete_flag = 'N';
		declare continue handler for not found set scheduler_done=1;

		open scheduler_cursor;

		scheduler_loop: loop
			fetch scheduler_cursor into v_scheduler_gid,v_insert_by;

			if scheduler_done = 1 then leave scheduler_loop; end if;

			call pr_set_process_dataset(v_scheduler_gid,'localhost',v_insert_by,'','',@msg,@result);
      call pr_get_dsinvalidrecord(v_scheduler_gid,@msg,@result);
		end loop scheduler_loop;

		close scheduler_cursor;
	end scheduler_block;
end $$

DELIMITER ;