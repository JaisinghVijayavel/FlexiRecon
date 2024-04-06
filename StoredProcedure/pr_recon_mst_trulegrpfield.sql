DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_trulegrpfield` $$
CREATE PROCEDURE `pr_recon_mst_trulegrpfield`
(
	inout in_rulegrpfield_gid int(10),
	in in_grp_field varchar(32),
	in in_rulegrpfield_seqno decimal(7,3),
	in in_rule_code varchar(32),
	in in_active_status char(1),
	in in_action varchar(32),
	in in_action_by varchar(10),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_rulegrpfield_gid int default 0;
	declare v_msg text default '';
  declare v_rule_group text default '';
  
  set v_rule_group = (select group_flag from recon_mst_trule 
											where rule_code = in_rule_code
											and delete_flag = 'N');
  
  if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if in_grp_field = '' or in_grp_field is null then
			set err_msg := concat(err_msg,'Group field cannot be empty,');
			set err_flag := true;
		end if;       
		
    if (v_rule_group ='OTO') then 
      set err_msg := concat(err_msg,'Group Flag One to One not Allowed to Add Grouping Field');
			set err_flag := true;
    end if;
  end if;
	
  if in_action = "UPDATE"  or in_action = "DELETE" then
		if in_rulegrpfield_gid = '' or in_rulegrpfield_gid is null or in_rulegrpfield_gid = 0 then
			set err_msg := concat(err_msg,'Invalid group gid,');
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
		insert into recon_mst_trulegrpfield
		(
			rule_code,
			rulegrpfield_seqno,
			grp_field,
			active_status,
			insert_date,
			insert_by
		)
		value
		(
			in_rule_code,
			in_rulegrpfield_seqno,
			in_grp_field,
			in_active_status,
			sysdate(),
			in_action_by
		);
		
		select 
			max(rulegrpfield_gid) 
		into 
			v_rulegrpfield_gid 
		from recon_mst_trulegrpfield;
		
		set in_rulegrpfield_gid = v_rulegrpfield_gid;
		
		set v_msg = 'Record saved successfully.. !';
  elseif(in_action = 'UPDATE') then		
		update recon_mst_trulegrpfield set
			rule_code = in_rule_code,
			rulegrpfield_seqno= in_rulegrpfield_seqno,
			grp_field = in_grp_field,
			update_date = sysdate(),
			update_by = in_action_by
		where rulegrpfield_gid = in_rulegrpfield_gid
		and active_status = 'Y'
		and delete_flag = 'N';
		
    set v_rulegrpfield_gid = in_rulegrpfield_gid;
		set v_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then 
		update recon_mst_trulegrpfield set
			update_date = sysdate(),
			update_by = in_action_by,
			delete_flag = 'Y'  
		where rulegrpfield_gid = in_rulegrpfield_gid
		and delete_flag = 'N';
		
		set v_rulegrpfield_gid = in_rulegrpfield_gid;
		set v_msg = 'Record deleted successfully.. !';
  end if;
	
	commit;
	
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;