DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_tpreprocess` $$
CREATE PROCEDURE `pr_recon_mst_tpreprocess`
(
  inout in_preprocess_gid int,
  inout in_preprocess_code varchar(32),
  in in_preprocess_desc varchar(32),
  in in_recon_code varchar(32),
  in in_hold_flag varchar(32),
  in in_get_recon_field varchar(32),
  in in_set_recon_field varchar(32),
  in in_process_method varchar(32),
  in in_process_query varchar(32),
  in in_process_function varchar(32),
  in in_lookup_dataset_code varchar(32),
  in in_lookup_return_field varchar(32),
  in in_preprocess_order varchar(32),
  in in_active_status varchar(32),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32),
  in in_action varchar(32),
  in in_action_by varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN
	declare err_msg text default '';
	declare err_flag boolean default false;
  declare v_preprocess_gid int default 0;

	if(in_action = 'INSERT' or in_action = 'UPDATE') then
    if in_preprocess_desc = '' or in_preprocess_desc is null then
			set err_msg := concat(err_msg,'Preprocess Name cannot be empty,');
			set err_flag := true;
		end if;

    if in_active_status <> 'Y' and in_active_status <> 'N' and in_active_status <> 'D' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;

	  if in_hold_flag <> 'Y' and in_hold_flag <> 'N'  or in_hold_flag is null then
			set err_msg := concat(err_msg,'Invalid Hold Flag value,');
			set err_flag := true;
		end if;

    if in_active_status <> 'Y' and in_active_status <> 'N' and in_active_status <> 'D' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;
	end if;

	if in_action = "UPDATE"  or in_action = "DELETE" then
		if not exists (select preprocess_gid from recon_mst_tpreprocess
      where preprocess_gid = in_preprocess_gid
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid Preprocess,');
			set err_flag := true;
		end if;
	end if;

  -- Duplicate validation
  if in_action = 'INSERT' then
		if exists (select preprocess_gid from recon_mst_tpreprocess
      where preprocess_desc = in_preprocess_desc
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate Preprocess,');
			set err_flag := true;
		end if;
  elseif in_action = 'UPDATE' then
		if exists (select preprocess_gid from recon_mst_tpreprocess
      where preprocess_desc = in_preprocess_desc
      and preprocess_gid <> in_preprocess_gid
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate Preprocess,');
			set err_flag := true;
		end if;
  end if;

	if err_flag = true then
		set out_result = 0;
		set out_msg = err_msg;
    leave me;
	end if;

  if (in_action = 'INSERT') then
    set in_preprocess_code = fn_get_autocode('PP');

		INSERT INTO recon_mst_tpreprocess
		(
			preprocess_code,
			preprocess_desc,
			recon_code,
			get_recon_field,
			set_recon_field,
			process_method,
			process_query,
			process_function,
			lookup_dataset_code,
			lookup_return_field,
			preprocess_order,
			hold_flag,
			active_status,
			insert_date,
			insert_by
		)
		VALUES
		(
			in_preprocess_code,
			in_preprocess_desc,
			in_recon_code,
			in_get_recon_field,
			in_set_recon_field,
			in_process_method,
			in_process_query,
			in_process_function,
			in_lookup_dataset_code,
			in_lookup_return_field,
			in_preprocess_order,
			in_hold_flag,
			in_active_status,
			sysdate(),
			in_user_code
		);

		select max(preprocess_gid) into v_preprocess_gid from recon_mst_tpreprocess;
		set in_preprocess_gid = v_preprocess_gid;
		Set in_preprocess_code = in_preprocess_code;
		set out_result = 1;

		set out_msg ='Record saved successfully.. !';
  elseif(in_action = 'UPDATE') then
		UPDATE recon_mst_tpreprocess SET
			preprocess_code = in_preprocess_code,
			preprocess_desc = in_preprocess_desc,
			recon_code = in_recon_code,
			get_recon_field = in_get_recon_field,
			set_recon_field = in_set_recon_field,
			process_method = in_process_method,
			process_query = in_process_query,
			process_function = in_process_function,
			lookup_dataset_code = in_lookup_dataset_code,
			lookup_return_field = in_lookup_return_field,
			preprocess_order = in_preprocess_order,
			hold_flag = in_hold_flag,
			active_status = in_active_status,
			update_date = sysdate(),
			update_by = in_user_code
		WHERE preprocess_gid = in_preprocess_gid
		and delete_flag = 'N';

    set out_result = 1;
		set out_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then
		update recon_mst_tpreprocess set
      active_status='N',
			update_date = sysdate(),
			update_by = in_user_code
		where preprocess_gid = in_preprocess_gid
		and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record deleted successfully.. !';
	end if;
END $$

DELIMITER ;