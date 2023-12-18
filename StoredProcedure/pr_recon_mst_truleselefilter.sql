DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_truleselefilter` $$
CREATE PROCEDURE `pr_recon_mst_truleselefilter`
(
	inout in_ruleselefilter_gid int(10),
	in in_rule_code varchar(32),
	in in_filter_applied_on varchar(32),
	in in_filter_field varchar(255),
	in in_filter_criteria varchar(255),
	in in_ident_criteria varchar(255),
	in in_ident_value varchar(255),
	in in_open_flag varchar(32),
	in in_close_flag varchar(32),
	in in_join_condition varchar(32),
	in in_active_status char(1),
	in in_action varchar(32),
	in in_action_by varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN

	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_ruleselefilter_gid int default 0;
	declare v_msg text default '';
  declare v_recon_code text default '';
	
	if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if not exists(select rule_gid from recon_mst_trule
			where rule_code = in_rule_code 
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid recon code');
			set err_flag := true;
		end if;
		
		if in_filter_applied_on = '' or in_filter_applied_on is null then
			set err_msg := concat(err_msg,'Filter applied on cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_filter_criteria = '' or in_filter_criteria is null then
			set err_msg := concat(err_msg,'Filter criteria on cannot be empty,');
			set err_flag := true;
		end if;

		if in_ident_criteria = '' or in_ident_criteria is null then
			set err_msg := concat(err_msg,'Identifier criteria on cannot be empty,');
			set err_flag := true;
		end if;

		if in_ident_value = '' or in_ident_value is null then
			set err_msg := concat(err_msg,'Identifier value on cannot be empty,');
			set err_flag := true;
		end if;

		if in_active_status <> 'Y' and in_active_status <> 'N' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;

    -- rule code
		if not exists(select rule_gid from recon_mst_trule
			where rule_code = in_rule_code
      and active_status <> 'N'
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid rule code');
			set err_flag := true;
    else
      select recon_code into v_recon_code from recon_mst_trule
			where rule_code = in_rule_code
      and active_status = 'Y'
			and delete_flag = 'N';
		end if;

    -- recon field
    if not exists(select recon_field_name from recon_mst_treconfield
      where recon_code = v_recon_code
      and recon_field_name = in_filter_field
      and active_status = 'Y'
      and delete_flag = 'N') then
      select
        recon_field_name into in_filter_field
      from recon_mst_treconfield
      where recon_code = v_recon_code
      and recon_field_desc = in_filter_field
      and active_status = 'Y'
      and delete_flag = 'N';
    end if;

    set in_filter_field = ifnull(in_filter_field,'');

		if in_filter_field = '' then
			set err_msg := concat(err_msg,'Invalid filter field,');
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
		set in_ruleselefilter_gid = 0;
		
		insert into recon_mst_truleselefilter
		(
			rule_code,
			filter_applied_on,
			filter_field,
			filter_criteria,
			ident_criteria,
			ident_value,
			open_parentheses_flag,
			close_parentheses_flag,
			join_condition,
			active_status,
			insert_date,
			insert_by
		)
		value
		(
			in_rule_code,
			in_filter_applied_on,
			in_filter_field,
			in_filter_criteria,
			in_ident_criteria,
			in_ident_value,
			in_open_flag,
			in_close_flag,
			in_join_condition,
			in_active_status,
			sysdate(),
			in_action_by
		);
		
		select max(ruleselefilter_gid) into v_ruleselefilter_gid from recon_mst_truleselefilter;
		set in_ruleselefilter_gid = v_ruleselefilter_gid;
		set v_msg = 'Record saved successfully.. !';  
	elseif(in_action = 'UPDATE') then
		update recon_mst_truleselefilter set
			rule_code = in_rule_code,
			filter_applied_on = in_filter_applied_on,
			filter_field = in_filter_field,
			filter_criteria = in_filter_criteria,
			ident_criteria = in_ident_criteria,
			ident_value = in_ident_value,
			open_parentheses_flag = in_open_flag,
			close_parentheses_flag = in_close_flag,
			join_condition = in_join_condition,
			update_date = sysdate(),
			update_by = in_action_by
		where ruleselefilter_gid = in_ruleselefilter_gid
		and delete_flag = 'N';     
		
		set in_ruleselefilter_gid = v_ruleselefilter_gid;
		set v_msg = 'Record updated successfully.. !';  
	elseif(in_action = 'DELETE') then
		update recon_mst_truleselefilter set
			update_date = sysdate(),
			update_by = in_action_by,
			delete_flag = 'Y'  
		where ruleselefilter_gid = in_ruleselefilter_gid
		and delete_flag = 'N';   
		set in_ruleselefilter_gid = v_ruleselefilter_gid;
		set v_msg = 'Record deleted successfully.. !';  
	end if;
	
	commit;
	
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;