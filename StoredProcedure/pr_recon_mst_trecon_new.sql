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
	in in_until_active_flag char(1),
	in in_active_status char(1),
	in in_recon_date_flag char(1),
	in in_recon_date_field varchar(128),
	in in_recon_value_flag char(1),
	in in_recon_value_field varchar(128),
	in in_threshold_plus_value double(15,2),
	in in_threshold_minus_value double(15,2),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	in in_action varchar(32),
	in in_action_by varchar(32),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN
	/*
		Created By : Hema
    Created Date :

    Updated By : Vijayavel J
    Updated Date : Mar-04-2024

    Version No : 6
  */

	declare v_recon_gid int default 0;
  declare v_result int default 0;
	declare v_msg text default '';

	declare err_msg text default '';
	declare err_flag boolean default false;

  set in_recon_value_flag = ifnull(in_recon_value_flag,'');
  set in_recon_value_field = ifnull(in_recon_value_field,'');

  set in_recon_date_flag = ifnull(in_recon_date_flag,'');
  set in_recon_date_field = ifnull(in_recon_date_field,'');

  set in_recon_automatch_partial = ifnull(in_recon_automatch_partial,'');

	if in_action = "UPDATE"  or in_action = "INSERT" then
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
			where recon_name = in_recon_name
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

  -- validation
  if in_action = "INSERT"  or in_action = "UPDATE" then
    if in_recontype_code = 'W'
      or in_recontype_code = 'B'
      or in_recontype_code = 'I' then

      -- value field flag
      if in_recon_value_flag = 'Y' then
        set in_recon_value_flag = 'N';
        set in_recon_value_field = '';
      end if;

      if in_recon_date_flag = 'N' then
        set in_recon_date_flag = 'Y';
      end if;
    end if;

    -- recon value based
    if in_recontype_code = 'V' then
      -- value field flag
      if in_recon_value_flag = 'N' then
        set in_recon_value_flag = 'Y';
        set in_recon_value_field = '';
      end if;
    end if;

    -- recon non-value based
    if in_recontype_code = 'N' then
      -- value field flag
      set in_recon_value_flag = 'N';

      -- partial match
      set in_recon_automatch_partial = 'N';
      set in_threshold_plus_value = 0;
      set in_threshold_minus_value = 0;
    end if;


		if in_action = "UPDATE" then
			if exists(select recon_gid from recon_mst_trecon
				where recon_name = in_recon_name
							and recon_gid <> in_recon_gid
							and delete_flag = 'N') then
				set err_msg := concat(err_msg,'Duplicate record,');
				set err_flag := true;
			end if;

      -- check recon field
			select
				count(*) into v_result
			from recon_mst_treconfield
			where recon_code = in_recon_code
			and active_status = 'Y'
			and delete_flag = 'N';

			set v_result = ifnull(v_result,0);

			if (in_active_status = 'Y' and v_result = 0)
        or (in_recon_value_flag = 'Y' and in_recon_value_field = '')
        or (in_recon_date_flag = 'Y' and in_recon_date_field = '') then
				set in_active_status = 'D';
			end if;
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
			recon_date_flag,
			recon_date_field,
			recon_value_flag,
			recon_value_field,
			recon_automatch_partial,
			threshold_plus_value,
			threshold_minus_value,
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
			in_recon_date_flag,
			in_recon_date_field,
			in_recon_value_flag,
			in_recon_value_field,
			in_recon_automatch_partial,
			in_threshold_plus_value,
			in_threshold_minus_value,
			in_active_status,
			sysdate(),
			in_action_by
		);

		select max(recon_gid) into v_recon_gid from recon_mst_trecon;

		set in_recon_gid = v_recon_gid;

    -- insert in reconcontext
    call pr_recon_mst_treconcontext (in_recon_code,in_user_code,in_role_code,in_lang_code);
    set in_recon_code=in_recon_code;

    -- insert system field debit,credit,tran date
    if in_recontype_code = 'W'
      or in_recontype_code = 'B'
      or in_recontype_code = 'I' then
      -- tran_date
      insert into recon_mst_treconfield
      (
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
        insert_by
      )
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
        in_action_by;

      -- balance value debit
      insert into recon_mst_treconfield
      (
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
        insert_by
      )
      select
        in_recon_code,
        'col1',
        'Particulars',
        'Y',
        2,
        'TEXT',
        '255',
        'N',
        'Y',
        sysdate(),
        in_action_by;

      -- debit
      insert into recon_mst_treconfield
      (
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
        insert_by
      )
      select
        in_recon_code,
        'value_debit',
        'Debit',
        'Y',
        3,
        'NUMERIC',
        '14,2',
        'Y',
        'Y',
        sysdate(),
        in_action_by;

      -- credit
      insert into recon_mst_treconfield
      (
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
        insert_by
      )
      select
        in_recon_code,
        'value_credit',
        'Credit',
        'Y',
        4,
        'NUMERIC',
        '14,2',
        'Y',
        'Y',
        sysdate(),
        in_action_by;

      -- balance value debit
      insert into recon_mst_treconfield
      (
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
        insert_by
      )
      select
        in_recon_code,
        'bal_value_debit',
        'Balance Debit',
        'N',
        5,
        'NUMERIC',
        '14,2',
        'Y',
        'Y',
        sysdate(),
        in_action_by;

      -- balance value credit
      insert into recon_mst_treconfield
      (
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
        insert_by
      )
      select
        in_recon_code,
        'bal_value_credit',
        'Balance Credit',
        'N',
        6,
        'NUMERIC',
        '14,2',
        'Y',
        'Y',
        sysdate(),
        in_action_by;
    end if;

	  set out_result = 1;
	  set out_msg = 'Record saved successfully.. !';
	elseif(in_action = 'UPDATE') then
		update recon_mst_trecon set
			recon_name = in_recon_name,
			recontype_code = in_recontype_code,
			period_from = in_period_from,
			period_to = in_period_to,
			until_active_flag = in_until_active_flag,
			active_status=in_active_status,
			recon_date_flag = in_recon_date_flag,
			recon_date_field = in_recon_date_field,
			recon_value_flag = in_recon_value_flag,
			recon_value_field = in_recon_value_field,
			recon_automatch_partial = in_recon_automatch_partial,
			threshold_plus_value = in_threshold_plus_value,
			threshold_minus_value = in_threshold_minus_value,
			update_date = sysdate(),
			update_by = in_action_by
		where recon_gid = in_recon_gid
		and delete_flag = 'N';

	  set out_result = 1;
	  set out_msg = 'Record Updated Successfully.. !';
  elseif(in_action = 'DELETE') then
		update recon_mst_trecon set
      active_status = 'N',
			update_date = sysdate(),
			update_by = in_action_by
		where recon_gid = in_recon_gid
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