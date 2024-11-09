DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_tpreprocess` $$
CREATE PROCEDURE `pr_recon_mst_tpreprocess`(
  inout in_preprocess_gid int(10),
  inout in_preprocess_code varchar(32),
  in in_preprocess_desc text,
  in in_recon_code varchar(32), 
  in in_hold_flag varchar(32), 
  in in_get_recon_field varchar(32),
  in in_set_recon_field varchar(32), 
  in in_process_method varchar(32),
  in in_process_query text, 
  in in_expression text, 
  in in_process_function text,
  in in_lookup_dataset_code varchar(32), 
  in in_lookup_multi_return_flag varchar(32), 
  in in_lookup_return_field varchar(32),
  in in_returnflag varchar(32),
  in in_preprocess_order varchar(32),
  in in_postprocessflag varchar(32),
  in in_cumulative_flag varchar(32),
  in in_active_status char(1),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32),
  in in_action varchar(32),
  in in_action_by varchar(10),
  out out_msg text,
  out out_result int(10)
)
me:BEGIN
	declare err_msg text default '';
	declare err_flag boolean default false;
  declare v_preprocess_gid int default 0;
	declare v_process_method text default '';

  set in_cumulative_flag = ifnull(in_cumulative_flag,'N');

  if in_cumulative_flag = '' then
    set in_cumulative_flag = 'N';
  end if;

	if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if in_preprocess_desc = '' or in_preprocess_desc is null then
			set err_msg := concat(err_msg,'Preprocess Name cannot be empty,');
			set err_flag := true;
		end if;
		if in_process_method = '' or in_process_method is null then
			set err_msg := concat(err_msg,'Preprocess Method cannot be empty,');
			set err_flag := true;
		end if;
		if in_preprocess_order = '' or in_preprocess_order is null or in_preprocess_order ='0' then
			set err_msg := concat(err_msg,'Preprocess Order cannot be empty,');
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
		if in_lookup_multi_return_flag ='Y' THEN
			set in_returnflag ='N';
		end if;
      --  if in_process_method ='QCD_EXPRESSION' THEN
		-- set in_process_function =fn_get_expressionformat(in_recon_code,in_expression);
		-- end if;
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
		if exists (select preprocess_gid from recon_mst_tpreprocess where preprocess_desc = in_preprocess_desc  and recon_code=in_recon_code and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate Preprocess,');
			set err_flag := true;
		end if;
        if exists(select preprocess_gid from recon_mst_tpreprocess	where preprocess_order = in_preprocess_order and recon_code = in_recon_code
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate Preprocess Order,');
			set err_flag := true;
		end if;
	elseif in_action = 'UPDATE' then
		if exists (select preprocess_gid from recon_mst_tpreprocess where preprocess_desc = in_preprocess_desc and recon_code=in_recon_code and preprocess_gid <> in_preprocess_gid
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate Preprocess,');
			set err_flag := true;
		end if;
        if exists(select preprocess_gid from recon_mst_tpreprocess where preprocess_order = in_preprocess_order and recon_code = in_recon_code
            and preprocess_gid <> in_preprocess_gid and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate Preprocess Order,');
			set err_flag := true;
		end if;
    end if;
	if err_flag = true then
		set out_result = 0;
		set out_msg = err_msg;
		leave me;
	end if;

  if in_process_method ='QCD_EXPRESSION' THEN
    set in_process_function =fn_get_expressionformat(in_recon_code,in_set_recon_field,in_expression,false);
  elseif in_process_method ='QCD_CUMULATIVEXP' THEN
    set in_process_function =fn_get_expressionformat(in_recon_code,in_set_recon_field,in_expression,true);
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
      process_expression,
			process_function,
      cumulative_flag,
			lookup_dataset_code,
      lookup_multi_return_flag,
			lookup_return_field,
      lookup_group_flag,
			preprocess_order,
      postprocess_flag,
			hold_flag,
			active_status,
			insert_date,
			insert_by)
		VALUES
		(
      in_preprocess_code,
			in_preprocess_desc,
			in_recon_code,
			in_get_recon_field,
			in_set_recon_field,
			in_process_method,
			in_process_query,
      in_expression,
			in_process_function,
      in_cumulative_flag,
			in_lookup_dataset_code,
      in_lookup_multi_return_flag,
			in_lookup_return_field,
      in_returnflag,
			in_preprocess_order,
      in_postprocessflag,
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
        process_expression=in_expression,
		    process_function = in_process_function,
        cumulative_flag = in_cumulative_flag,
		    lookup_dataset_code = in_lookup_dataset_code,
        lookup_multi_return_flag=in_lookup_multi_return_flag,
		    lookup_return_field = in_lookup_return_field,
        lookup_group_flag = in_returnflag,
        postprocess_flag = in_postprocessflag,
		    preprocess_order = in_preprocess_order,
		    hold_flag = in_hold_flag,
		    active_status = in_active_status,
		    update_date = sysdate(),
		    update_by = in_user_code
		  WHERE preprocess_gid = in_preprocess_gid;

      set out_result = 1;
		  set out_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then
		update recon_mst_tpreprocess set
			active_status='N',delete_flag = 'Y',
			update_date = sysdate(),
			update_by = in_user_code
		where preprocess_gid = in_preprocess_gid
		and delete_flag = 'N';
		set out_result = 1;
		set out_msg = 'Record deleted successfully.. !';
	elseif(in_action = 'DELETEPREPROCESS') then
	  set v_process_method = (select process_method from recon_mst_tpreprocess
                            where preprocess_code=in_preprocess_code and delete_flag = 'N');

		if(v_process_method='QCD_FUNCTION') then
			update recon_mst_tpreprocessfilter set
				update_date = sysdate(),
				update_by = in_action_by,
				active_status='N'	,delete_flag = 'Y'
			where preprocess_code=in_preprocess_code and delete_flag = 'N';

			update recon_mst_tpreprocess set
				process_function = '',
        get_recon_field = '',
        set_recon_field = ''
			where preprocess_code=in_preprocess_code
      and delete_flag = 'N';

		elseif(v_process_method='QCD_LOOKUP') then
			update recon_mst_tpreprocesscondition set
				update_date = sysdate(),
				update_by = in_action_by,
				active_status = 'N'  ,
				delete_flag = 'Y'
			where preprocess_code=in_preprocess_code
      and delete_flag = 'N';

			update recon_mst_tpreprocess set
				lookup_dataset_code = '',
        set_recon_field='',
				lookup_return_field = ''
			where preprocess_code=in_preprocess_code
      and delete_flag = 'N';

		elseif(v_process_method='QCD_QUERY') then
			update recon_mst_tpreprocess set
				process_query = ''
			where preprocess_code=in_preprocess_code
      and delete_flag = 'N';
		end if;

    set out_result = 1;
		set out_msg = 'Record deleted successfully.. !';
	end if;
END $$

DELIMITER ;