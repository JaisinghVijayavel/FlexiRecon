DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_trulesetup` $$
CREATE PROCEDURE `pr_recon_mst_trulesetup`
(
	inout in_rule_gid int(10),
	in in_rule_code varchar(32),
	in in_rule_name varchar(255),
	in in_recon_code varchar(32),
	in in_rule_order decimal(9,2),
	in in_period_from date,
	in in_period_to date,
	in in_until_active_flag char(1),
	in in_applyrule_on varchar(32),
	in in_group_flag varchar(32),
	in in_source_dataset_code varchar(32),
	in in_comparison_dataset_code varchar(32),
	in in_source_acc_mode varchar(32),
	in in_comparison_acc_mode varchar(32),
	in in_parent_dataset_code varchar(32),
	in in_parent_acc_mode varchar(32),
	in in_active_status char(1),
	in in_action varchar(16),
	in in_action_by varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN
	/*
		Created By : Hema
    Created Date :

    Updated By : Vijayavel J
    Updated Date : Dec-08-2023

    Version No : 3
  */

	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_rule_gid int default 0;
  declare v_result int default 0;
	declare v_msg text default '';
    
  if(in_action = 'INSERT' or in_action = 'UPDATE') then
    if in_rule_name = '' or in_rule_name is null then
			set err_msg := concat(err_msg,'Rule name cannot be empty,');
			set err_flag := true;
		end if;
		
		if not exists(select recon_gid from recon_mst_trecon 
			where recon_code = in_recon_code
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid recon code');
			set err_flag := true;
		end if;
		
		if in_applyrule_on <> 'T' and in_applyrule_on <> 'S' or in_applyrule_on is null then
			set err_msg := concat(err_msg,'Invalid applyrule value,');
			set err_flag := true;
		end if;

    if not exists (select dataset_code from recon_mst_trecondataset
      where recon_code = in_recon_code
      and dataset_code = in_source_dataset_code
      and dataset_type <> 'S'
      and active_status = 'Y'
      and delete_flag = 'N') then
      set err_msg := concat(err_msg,'Invalid source dataset,');
      set err_flag := true;
    end if;

    if in_applyrule_on = 'T' then
      if not exists (select dataset_code from recon_mst_trecondataset
        where recon_code = in_recon_code
        and dataset_code = in_comparison_dataset_code
        and dataset_type <> 'S'
        and active_status = 'Y'
        and delete_flag = 'N') then
        set err_msg := concat(err_msg,'Invalid comparison dataset,');
        set err_flag := true;
      end if;
    elseif in_applyrule_on = 'S' then
      if not exists (select dataset_code from recon_mst_trecondataset
        where recon_code = in_recon_code
        and dataset_code = in_comparison_dataset_code
        and parent_dataset_code = in_source_dataset_code
        and dataset_type = 'S'
        and active_status = 'Y'
        and delete_flag = 'N') then
        set err_msg := concat(err_msg,'Invalid supporting dataset,');
        set err_flag := true;
      end if;
    end if;

    if in_period_from = '' or in_period_from is null then
			set err_msg := concat(err_msg,'Period from cannot be empty,');
			set err_flag := true;
		end if;

    if in_until_active_flag <> 'Y' and in_until_active_flag <> 'N' then
			set err_msg := concat(err_msg,'Invalid until active flag,');
			set err_flag := true;
		elseif in_until_active_flag = 'Y' then
			set in_period_to = null;
		else
			if in_period_to is null then
				set err_msg := concat(err_msg,'Invalid period to,');
				set err_flag := true;
			end if;
		end if;

		if in_active_status <> 'Y' and in_active_status <> 'N' and in_active_status <> 'D' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;

    if in_group_flag is null then
			set err_msg := concat(err_msg,'Invalid group flag value,');
			set err_flag := true;
		end if;
	end if;

  -- check duplicate rule name
	if in_action ='INSERT' then
		if exists(select rule_gid from recon_mst_trule
			where rule_name = in_rule_name
      and recon_code = in_recon_code
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate record,');
			set err_flag := true;
		end if;
  elseif in_action ='UPDATE' then
		if exists(select rule_gid from recon_mst_trule
			where rule_name = in_rule_name
      and recon_code = in_recon_code
      and rule_gid <> in_rule_gid
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate record,');
			set err_flag := true;
		end if;
	end if;

  -- duplicate rule order
	if in_action ='INSERT' then
		if exists(select rule_gid from recon_mst_trule
			where recon_code = in_recon_code
      and rule_order = in_rule_order
      and active_status <> 'N'
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate rule order no.,');
			set err_flag := true;
		end if;
  elseif in_action ='UPDATE' then
		if exists(select rule_gid from recon_mst_trule
			where recon_code = in_recon_code
      and rule_order = in_rule_order
      and active_status <> 'N'
      and rule_gid <> in_rule_gid
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate rule order no.,');
			set err_flag := true;
		end if;
	end if;

  if in_action = "UPDATE"  or in_action = "DELETE" then
		if not exists(select rule_gid from recon_mst_trule
			where rule_gid = in_rule_gid
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid record,');
			set err_flag := true;
		end if;
  end if;

	if err_flag = true then
		set out_result = 0;
		set out_msg = err_msg;
    leave me;
  end if;

  if in_action = 'UPDATE' then
    select
      count(*) into v_result
    from recon_mst_trulecondition
    where rule_code = in_rule_code
    and delete_flag = 'N';

    set v_result = ifnull(v_result,0);

    if in_active_status = 'Y' and v_result = 0 then
      set in_active_status = 'D';
    end if;
  end if;

  start transaction;

	if (in_action = 'INSERT') then
    set in_rule_code = fn_get_autocode('RULE');
    set in_active_status = 'D';

    insert into recon_mst_trule
		(
			rule_code,
			rule_name,
			recon_code,
			rule_order,
			rule_apply_on,
			period_from,
			period_to,
			until_active_flag,
			source_dataset_code,
			source_acc_mode,
			comparison_dataset_code,
			comparison_acc_mode,
			group_flag,
			active_status,
			insert_date,
			insert_by
		)
		value
		(
			in_rule_code,
			in_rule_name,
			in_recon_code,
			in_rule_order,
			in_applyrule_on,
			in_period_from,
			in_period_to,
			in_until_active_flag,
			in_source_dataset_code,
			in_source_acc_mode,
			in_comparison_dataset_code,
			in_comparison_acc_mode,
			in_group_flag,
			in_active_status,
			sysdate(),
			in_action_by
		);

		select max(rule_gid) into v_rule_gid from recon_mst_trule;

		set in_rule_gid = v_rule_gid;

		set out_result = 1;
		set out_msg = 'Record saved successfully.. !';
	elseif(in_action = 'UPDATE') then
		update recon_mst_trule set
			rule_name = in_rule_name,
			recon_code = in_recon_code,
			rule_order = in_rule_order,
			rule_apply_on = in_applyrule_on,
			period_from = in_period_from,
			period_to = in_period_to,
			until_active_flag = in_until_active_flag,
			source_dataset_code = in_source_dataset_code,
			source_acc_mode = in_source_acc_mode,
			comparison_dataset_code = in_comparison_dataset_code,
			comparison_acc_mode = in_comparison_acc_mode,
			group_flag = in_group_flag,
			active_status = in_active_status,
			update_date = sysdate(),
			update_by = in_action_by
		where rule_gid = in_rule_gid
		and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record updated successfully.. !';
	elseif(in_action = 'DELETE') then
		update recon_mst_trule set
      active_status = 'N',
			update_date = sysdate(),
			update_by = in_action_by
		where rule_gid = in_rule_gid
		and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record deleted successfully.. !';
	end if;

  commit;
END $$

DELIMITER ;