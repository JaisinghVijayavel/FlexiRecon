DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_trulerecorder` $$
CREATE PROCEDURE `pr_recon_mst_trulerecorder`
(
	inout in_rulerecorder_gid int,
	in in_rule_code varchar(32),
	in in_recorder_applied_on varchar(32),
	in in_recorder_seqno decimal(7,3),
	in in_recorder_field varchar(128),
	in in_active_status char(1),
	in in_action varchar(32),
	in in_action_by varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_rulerecorder_gid int default 0;
	declare v_msg text default '';
	
  if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if in_recorder_field = '' or in_recorder_field is null then
			set err_msg := concat(err_msg,'field cannot be empty,');
			set err_flag := true;
		end if;     
  end if;
	
  if in_action = "UPDATE"  or in_action = "DELETE" then
		if in_rulerecorder_gid = '' or in_rulerecorder_gid is null or in_rulerecorder_gid = 0 then
			set err_msg := concat(err_msg,'Invalid rule order gid,');
			set err_flag := true;
		end if;		
  end if;
	
	if err_flag = true then
		set out_result = 0;
		set out_msg = err_msg;
    leave me;
  end if;
	
  start transaction;
	
	if (in_action = 'INSERT') then 
		set in_rulerecorder_gid = 0;        
		
    INSERT INTO recon_mst_trulerecorder
    (
			rule_code,
			recorder_applied_on,
			recorder_seqno,
			recorder_field,
			active_status,
			insert_date,
			insert_by
		)
		VALUES
		(
			in_rule_code,
			in_recorder_applied_on,
			in_recorder_seqno,
			in_recorder_field,
			in_active_status,
			sysdate(),
			in_action_by
		);
        
		select 
			max(rulerecorder_gid) 
		into 
			v_rulerecorder_gid 
		from recon_mst_trulerecorder;
		
		set in_rulerecorder_gid = v_rulerecorder_gid;
		set v_msg = 'Record saved successfully.. !';
  elseif(in_action = 'UPDATE') then
		UPDATE recon_mst_trulerecorder SET
			rule_code =in_rule_code,
			recorder_applied_on = in_recorder_applied_on,
			recorder_seqno = in_recorder_seqno,
			recorder_field = in_recorder_field,
			active_status =in_active_status,
			update_date = sysdate(),
			update_by =in_action_by
		WHERE rulerecorder_gid =in_rulerecorder_gid
		and active_status = 'Y'
		and delete_flag = 'N';
		
		set v_rulerecorder_gid = in_rulerecorder_gid;
		set v_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then 
		update recon_mst_trulerecorder set
			update_date = sysdate(),
			update_by = in_action_by,
			active_status = 'N'  
		where  rulerecorder_gid =in_rulerecorder_gid
		and delete_flag = 'N';
		
		set v_rulerecorder_gid = in_rulerecorder_gid;
		set v_msg = 'Record deleted successfully.. !';
  end if;
	
	commit;
	
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;