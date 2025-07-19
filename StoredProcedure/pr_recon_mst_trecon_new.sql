DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_trecon_new` $$
CREATE PROCEDURE `pr_recon_mst_trecon_new`
(
	inout in_recon_gid int(10),
	inout in_recon_code varchar(32),
	in in_recon_name varchar(255),
	in in_recontype_code char(1),
	in in_recon_automatch_partial char(1),
	in in_period_from date,
	in in_period_to date,
  in in_closure_date date,
  in in_cycle_date date,
	in in_until_active_flag char(1),
	in in_active_status char(1),
	in in_recon_date_flag char(1),
	in in_recon_date_field varchar(128),
	in in_recon_value_flag char(1),
	in in_recon_value_field varchar(128),
  in in_threshold_code varchar(32),
	in in_threshold_plus_value double(15,2),
	in in_threshold_minus_value double(15,2),
  in in_processing_method varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	in in_action varchar(16),
	in in_action_by varchar(32),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN

	declare v_recon_gid int default 0;
	declare v_result int default 0;
	declare v_msg text default '';
	declare err_msg text default '';
	declare v_master_syscode text default '';
	declare err_flag boolean default false;

	set in_recon_value_flag = ifnull(in_recon_value_flag,'');
	set in_recon_value_field = ifnull(in_recon_value_field,'');
	set in_recon_date_flag = ifnull(in_recon_date_flag,'');
	set in_recon_date_field = ifnull(in_recon_date_field,'');
	set in_recon_automatch_partial = ifnull(in_recon_automatch_partial,'');
	SET v_master_syscode= ifnull((select master_syscode from admin_mst_tusercontext where user_code=in_user_code and parent_master_syscode ='QCD_L3'),'');

	if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if v_master_syscode = '' or v_master_syscode is null then
			set err_msg := concat(err_msg,'Please set Recon level in config,');
			set err_flag := true;
		end if;
		
		if in_recon_name = '' or in_recon_name is null then
			set err_msg := concat(err_msg,'Recon Name cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_recontype_code = '' or in_recontype_code is null then
			set err_msg := concat(err_msg,'Type Code cannot be empty,');
			set err_flag := true;
		end if;
		
    if in_period_from = '' or in_period_from is null then
			set err_msg := concat(err_msg,'Preiod from cannot be empty,');
			set err_flag := true;
		end if;
		
    if in_until_active_flag <> 'Y' and in_until_active_flag <> 'N' then
			set err_msg := concat(err_msg,'Invalid until active flag,');
			set err_flag := true;
		end if;
		
		if in_until_active_flag = 'Y' then
			set in_period_to = null;
		end if;
		
    if in_closure_date = '1901-01-01' then
			set in_closure_date = null;
		end if;
		
    if in_cycle_date = '1901-01-01' then
			set in_cycle_date = null;
		end if;
		
		if in_until_active_flag = 'N' then
			if in_period_to is null or in_period_to='1901-01-02' then
				set err_msg := concat(err_msg,'Invalid period to,');
				set err_flag := true;
			end if;
			
      if STR_TO_DATE(in_period_from, '%Y-%m-%d') > STR_TO_DATE(in_period_to, '%Y-%m-%d') then
				set err_msg := concat(err_msg,'Period to not lesser than period from,');
				set err_flag := true;
		 end if;        
		end if;
		
    if in_active_status <> 'Y' and in_active_status <> 'N' and in_active_status <> 'D' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;
		
		if in_recon_automatch_partial ='Y' THEN
			if in_threshold_plus_value = 0 or in_threshold_plus_value is null then
				set err_msg := concat(err_msg,'Knockoff threshold value cannot be zero,');
				set err_flag := true;
			end if;
    end if;
	end if;

  if in_action = "UPDATE"  or in_action = "DELETE" then
		if not exists(select recon_gid from recon_mst_trecon
			where recon_gid = in_recon_gid
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid recon,');
			set err_flag := true;
		end if;
  end if;

  if in_action = "INSERT" then
		if exists(select recon_gid from recon_mst_trecon
			where recon_name = in_recon_name and active_status='Y'
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate record,');
			set err_flag := true;
		end if;
  end if;
	
  if in_action = "UPDATE" then
		if exists(select recon_gid from recon_mst_trecon where recon_name = in_recon_name and recon_gid <> in_recon_gid and active_status='Y'
							and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate record,');
			set err_flag := true;
		end if;
  end if;
	
	if err_flag = true then
		set out_result = 0;
		set out_msg = err_msg;
    leave me;
  end if;

  if in_action = "INSERT"  or in_action = "UPDATE" then
    if in_recontype_code = 'W' or in_recontype_code = 'B' or in_recontype_code = 'I' then
			if in_recon_value_flag = 'Y' then
				set in_recon_value_flag = 'N';
				set in_recon_value_field = '';
      end if;
				
      if in_recon_date_flag = 'N' then
				set in_recon_date_flag = 'Y';
      end if;      
    end if;

		if in_recontype_code = 'V' then
			if in_recon_value_flag = 'N' then
				set in_recon_value_flag = 'Y';
        set in_recon_value_field = '';
			end if;
    end if;
		
    if in_recontype_code = 'N' then     
			set in_recon_value_flag = 'N';
			set in_recon_automatch_partial = 'N';
			set in_threshold_plus_value = 0;
			set in_threshold_minus_value = 0;
		end if;
  end if;
   
	select	count(*) into v_result from recon_mst_treconfield where recon_code = in_recon_code and active_status = 'Y' and delete_flag = 'N';
	set v_result = ifnull(v_result,0);

	if (in_active_status = 'Y' and v_result = 0) or (in_recon_value_flag = 'Y' and in_recon_value_field = '') or (in_recon_date_flag = 'Y' and in_recon_date_field = '') then
		set in_active_status = 'D';
	end if;	
 
  if in_action ="DELETE" then 
		if exists(select theme_gid from recon_mst_ttheme where recon_code = in_recon_code and active_status != 'N' and delete_flag = 'N') then
			set out_result = 0;
			set out_msg = 'Access Denied';
			leave me;
		 end if;
		 
		if exists(select rule_gid from recon_mst_trule	where recon_code = in_recon_code and active_status != 'N' and delete_flag = 'N') then
			set out_result = 0;
			set out_msg = 'Access Denied';
			leave me;
		end if;
		 
    if exists(select preprocess_gid from recon_mst_tpreprocess where recon_code = in_recon_code and active_status != 'N' and delete_flag = 'N') then
			set out_result = 0;
			set out_msg = 'Access Denied';
			leave me;
		end if;
		
		if exists(select reporttemplate_gid from recon_mst_treporttemplate where recon_code = in_recon_code and active_status != 'N' and delete_flag = 'N') then
			set out_result = 0;
			set out_msg = 'Access Denied';
			leave me;
		end if;
  end if;
	
	if(in_until_active_flag = 'Y') then
		set in_period_to = null;
	end if;

  start transaction;
  if (in_action = 'INSERT') then
		set in_recon_gid = 0;
		set in_recon_code = fn_get_autocode('RECON');
		set in_active_status = 'D';

		insert into recon_mst_trecon  
		(
			recon_code,
			recon_name,
			recontype_code,
			period_from,
			period_to,
			until_active_flag,
      recon_closure_date,
      recon_cycle_date,
			recon_date_flag,
			recon_date_field,
			recon_value_flag,
			recon_value_field,
			recon_automatch_partial,
      threshold_code,
			threshold_plus_value,
			threshold_minus_value,
      processing_method,
			active_status,
			insert_date,
			insert_by 
		)
		value
		(
			in_recon_code,
			in_recon_name,
			in_recontype_code,
			in_period_from,
			in_period_to,
			in_until_active_flag,
      in_closure_date,
      in_cycle_date,
			in_recon_date_flag,
			in_recon_date_field,
			in_recon_value_flag,
			in_recon_value_field,
			in_recon_automatch_partial,
      in_threshold_code,
			in_threshold_plus_value,
			concat('-',in_threshold_minus_value),
      in_processing_method,
			in_active_status,
			sysdate(),
			in_user_code
		);

		call pr_recon_mst_treconcontext (in_recon_code,in_user_code,in_role_code,in_lang_code);

		select max(recon_gid) into v_recon_gid from recon_mst_trecon;
		set in_recon_gid = v_recon_gid;

		INSERT INTO recon_mst_tmaster(master_gid, master_syscode, master_code,  master_name,  parent_master_syscode, depend_flag, depend_master_syscode,depend_parent_master_syscode,system_flag, active_status, insert_date, insert_by)
		VALUES(0,in_recon_code,in_recon_code,in_recon_name,'QCD_L4','Y',v_master_syscode,'QCD_L3','N',in_active_status,NOW(),in_user_code);

    if in_recontype_code = 'W' or in_recontype_code = 'B' or in_recontype_code = 'I' then
      insert into recon_mst_treconfield (
        recon_code,
        recon_field_name,
        recon_field_desc,
        display_flag,
        display_order,
        recon_field_type,
        recon_field_length,
        system_field_flag,
        active_status,
        insert_date,
        insert_by )
      select
        in_recon_code,
        'value_debit',
        'Debit',
        'Y',
        2,
        'NUMERIC',
        '14,2',
        'Y',
        'Y',
        sysdate(),
        in_user_code;

      insert into recon_mst_treconfield (
        recon_code,
        recon_field_name,
        recon_field_desc,
        display_flag,
        display_order,
        recon_field_type,
        recon_field_length,
        system_field_flag,
        active_status,
        insert_date,
        insert_by )
      select
        in_recon_code,
        'value_credit',
        'Credit',
        'Y',
        3,
        'NUMERIC',
        '14,2',
        'Y',
        'Y',
        sysdate(),
        in_user_code;

      insert into recon_mst_treconfield (
        recon_code,
        recon_field_name,
        recon_field_desc,
        display_flag,
        display_order,
        recon_field_type,
        recon_field_length,
        system_field_flag,
        active_status,
        insert_date,
        insert_by )
      select
        in_recon_code,
        'tran_date',
        'Tran Date',
        'Y',
        1,
        'DATE',
        '',
        'Y',
        'Y',
        sysdate(),
        in_user_code;

      insert into recon_mst_treconfield (
        recon_code,
        recon_field_name,
        recon_field_desc,
        display_flag,
        display_order,
        recon_field_type,
        recon_field_length,
        system_field_flag,
        active_status,
        insert_date,
        insert_by )
      select
        in_recon_code,
        'bal_value_debit',
        'Balance Debit',
        'Y',
        4,
        'NUMERIC',
        '14,2',
        'N',
        'Y',
        sysdate(),
        in_user_code;


      insert into recon_mst_treconfield (
        recon_code,
        recon_field_name,
        recon_field_desc,
        display_flag,
        display_order,
        recon_field_type,
        recon_field_length,
        system_field_flag,
        active_status,
        insert_date,
        insert_by )
      select
        in_recon_code,
        'bal_value_credit',
        'Balance Credit',
        'Y',
        5,
        'NUMERIC',
        '14,2',
        'N',
        'Y',
        sysdate(),
        in_user_code;
		end if;

    call pr_set_createrecontables(in_recon_code,@msg,@result);

		set out_result = 1;
		set out_msg = 'Record saved successfully.. !';
  elseif(in_action = 'UPDATE') then
		update recon_mst_trecon set
			recon_name = in_recon_name,
			recontype_code = in_recontype_code,
			period_from = in_period_from,
			period_to = in_period_to,
			until_active_flag = in_until_active_flag,
			recon_closure_date = in_closure_date,
      recon_cycle_date = in_cycle_date,
			active_status=in_active_status,
			recon_date_flag = in_recon_date_flag,
			recon_date_field = in_recon_date_field,
			recon_value_flag = in_recon_value_flag,
			recon_value_field = in_recon_value_field,
			recon_automatch_partial = in_recon_automatch_partial,
      threshold_code = in_threshold_code,
			threshold_plus_value = in_threshold_plus_value,
			threshold_minus_value = concat('-',in_threshold_minus_value),
      processing_method = in_processing_method,
			update_date = sysdate(),
			update_by = in_user_code
		where recon_gid = in_recon_gid
		and delete_flag = 'N';

		update recon_mst_tmaster set active_status = 'Y' where master_syscode = in_recon_code and delete_flag = 'N';

	  set out_result = 1;
	  set out_msg = 'Record Updated Successfully.. !';
  elseif(in_action = 'DELETE') then
		update recon_mst_trecon set 
			active_status = 'N',
			update_date = sysdate(),
			update_by = in_action_by 
		where recon_gid = in_recon_gid	
		and delete_flag = 'N';
		
		update recon_mst_trecondataset set 
			active_status = 'N',
			update_date = sysdate(),
			update_by = in_action_by 
		where recon_code = in_recon_code 
		and delete_flag = 'N';
		
		update recon_mst_treconfield set 
			active_status = 'N',
			update_date = sysdate(),
			update_by = in_action_by 
		where recon_code = in_recon_code 
		and delete_flag = 'N';
		
		update recon_mst_treconfieldmapping set 
			active_status = 'N',
			update_date = sysdate(),
			update_by = in_action_by 
		where recon_code = in_recon_code 
		and delete_flag = 'N';
			
    update admin_mst_treconcontext set 
			active_status = 'N' 
		where recon_code = in_recon_code 
		and delete_flag = 'N';
		
    set out_result = 1;
		set out_msg = 'Record deleted successfully.. !';
  else
		set out_result = 0;
		set out_msg = 'Failed !';
	end if;
	
	commit;
END $$

DELIMITER ;