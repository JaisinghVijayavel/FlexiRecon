DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_tdataset` $$
CREATE PROCEDURE `pr_recon_mst_tdataset`
(
  inout in_dataset_gid int(10),
  in in_dataset_code varchar(32),
  in in_dataset_name varchar(32),
  in in_dataset_category varchar(32),
  in in_clone_dataset varchar(32),
  in in_active_status char(1),
  in in_active_reason text,
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32),
  in in_action varchar(16),
  in in_action_by varchar(10),
  out out_msg text,
  out out_result int(10)
)
me:BEGIN
	/*
		Created By : Hema
    Created Date : Sep-29-2023

    Updated By : Vijayavel J
    Updated Date : Nov-23-2023

    Version No : 2
  */

  declare v_dataset_db_name text default '';
  declare v_dataset_table_name text default '';
	declare v_dataset_gid int default 0;
	declare v_msg text default '';

	declare err_msg text default '';
	declare err_flag boolean default false;

	if(in_action = 'INSERT' or in_action = 'UPDATE') then
    if in_dataset_name = '' or in_dataset_name is null then
			set err_msg := concat(err_msg,'Dataset Name cannot be empty,');
			set err_flag := true;
		end if;

		if in_dataset_category = '' or in_dataset_category is null then
			set err_msg := concat(err_msg,'Dataset Category cannot be empty,');
			set err_flag := true;
		end if;

    if in_active_status <> 'Y' and in_active_status <> 'N' and in_active_status <> 'D' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;
	end if;

	if in_action = "UPDATE"  or in_action = "DELETE" then
		if not exists (select dataset_gid from recon_mst_tdataset
      where dataset_gid = in_dataset_gid
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid dataset,');
			set err_flag := true;
		end if;
	end if;

  -- Duplicate validation
  if in_action = 'INSERT' then
		if exists (select dataset_gid from recon_mst_tdataset
      where dataset_name = in_dataset_name
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate dataset,');
			set err_flag := true;
		end if;
  elseif in_action = 'UPDATE' then
		if exists (select dataset_gid from recon_mst_tdataset
      where dataset_name = in_dataset_name
      and dataset_gid <> in_dataset_gid
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate dataset,');
			set err_flag := true;
		end if;
  end if;

	if err_flag = true then
		set out_result = 0;
		set out_msg = err_msg;
    leave me;
	end if;

  if (in_action = 'INSERT') then
    set in_dataset_code = fn_get_autocode('DS');

    set v_dataset_db_name = fn_get_configvalue('dataset_db_name');
    set v_dataset_db_name = ifnull(v_dataset_db_name,'');

    if v_dataset_db_name <> '' then
      set v_dataset_table_name = concat(v_dataset_db_name,'.',in_dataset_code);
    else
      set v_dataset_table_name = in_dataset_code;
    end if;

    if exists(select dataset_gid from recon_mst_tdataset
			where dataset_code = in_dataset_code
      and delete_flag = 'N') then

      set out_result = 0;
      set out_msg = 'Duplicate record !';
      leave me;
		end if;

		set in_dataset_gid = 0;

		insert into recon_mst_tdataset
    (
      dataset_code, dataset_name, dataset_category,dataset_table_name,
			active_status,insert_date,insert_by
    )
    value
    (
      in_dataset_code, in_dataset_name, in_dataset_category,v_dataset_table_name,
      in_active_status, sysdate(),in_action_by
    );

    if in_clone_dataset is not null then
			call pr_recon_mst_dataset_clone(in_clone_dataset,in_dataset_code, in_action,in_action_by, @out_msg, @out_result);
    end if;

		select max(dataset_gid) into v_dataset_gid from recon_mst_tdataset;

		set in_dataset_gid = v_dataset_gid;

    -- create dataset table
    call pr_create_datasettable(v_dataset_db_name,in_dataset_code,@msg,@result);

		set out_result = 1;
		set out_msg = 'Record saved successfully.. !';

	elseif(in_action = 'UPDATE') then
		update recon_mst_tdataset set
			dataset_name = in_dataset_name,
			dataset_category = in_dataset_category,
			active_status = in_active_status,
			update_date = sysdate(),
			update_by = in_action_by
		where dataset_gid = in_dataset_gid
		and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then
		update recon_mst_tdataset set
			inactive_reason=in_active_reason,
      active_status='N',
			update_date = sysdate(),
			update_by = in_action_by
		where dataset_gid = in_dataset_gid
		and delete_flag = 'N';
            
		set out_result = 0;           
		set out_msg = 'Record deleted successfully.. !';
	end if;
END $$

DELIMITER ;