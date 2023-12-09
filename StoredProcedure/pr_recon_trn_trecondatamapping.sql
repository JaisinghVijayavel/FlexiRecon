DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_trn_trecondatamapping` $$
CREATE PROCEDURE `pr_recon_trn_trecondatamapping`
(
	inout in_reconfield_gid int(10),
	in in_reconfieldmapping_gid int(10),
	in in_recon_code varchar(32),
	in in_recon_field_name varchar(32),
	in in_display_order decimal(6,2),
	in in_dataset_code varchar(32),
	in in_dataset_field_name varchar(255),
	in in_active_status char(1),
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
		Created Date : Oct-06-2023

		Updated By : Vijayavel J
		Updated Date : Nov-08-2023
		
		Version No : 2
	*/
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_reconfield_gid int default 0;
	declare v_reconfield_gid1 int default 0;   
	declare v_reconfieldmapping_gid int default 0;
	declare v_msg text default '';
	declare v_field_type text default '';
	declare v_fieldtype_id text default '';

  set in_reconfield_gid = ifnull(in_reconfield_gid,0);

  if(in_action = 'INSERT' or in_action = 'UPDATE') then
	if in_recon_field_name = '' or in_recon_field_name is null then
		set err_msg := concat(err_msg,'Recon fieldname cannot be empty,');
		set err_flag := true;
	end if;

	if in_display_order = '' or in_display_order is null then
		set err_msg := concat(err_msg,'Display order cannot be empty,');
		set err_flag := true;
	end if;

		if not exists(select recon_gid from recon_mst_trecon
			where recon_code = in_recon_code 
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid recon code');
			set err_flag := true;
		end if;
		
		if in_recon_field_name = '' or in_recon_field_name is null then
			set err_msg := concat(err_msg,'Recon fieldname cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_display_order = '' or in_display_order is null then
			set err_msg := concat(err_msg,'Display order cannot be empty,');
			set err_flag := true;
		end if;
		
		if(in_action = 'UPDATE' and in_reconfieldmapping_gid > 0) then
			if in_dataset_code = '' or in_dataset_code is null then
				set err_msg := concat(err_msg,'Dataset code cannot be empty,');
				set err_flag := true;
			end if;
			
			if not exists(select dataset_gid from recon_mst_tdataset 
				where dataset_code = in_dataset_code 
				and delete_flag = 'N') then
				set err_msg := concat(err_msg,'Invalid dataset code,');
				set err_flag := true;
			end if;

			if in_dataset_field_name = '' or in_dataset_field_name is null then
				set err_msg := concat(err_msg,'Dataset fieldname cannot be empty,');
				set err_flag := true;
			end if;
		end if;

    if not exists(select * from recon_mst_tdatasetfield
        where dataset_code = in_dataset_code
        and dataset_table_field = in_dataset_field_name
        and active_status = 'Y'
        and delete_flag = 'N')
        and in_dataset_code <> ''
        and in_dataset_field_name <> '' then
				set err_msg := concat(err_msg,'Invalid dataset field,');
				set err_flag := true;
    end if;
  end if;

  if err_flag = true then
		set out_result = 0;
		set out_msg = err_msg;
		leave me;
  end if;

  -- get dataset field type
  select
    field_type into v_field_type
  from recon_mst_tdatasetfield
  where dataset_code = in_dataset_code
  and dataset_table_field = in_dataset_field_name
  and delete_flag = 'N';

  if not exists(select * from recon_mst_treconfield
    where recon_code = in_recon_code
    and (recon_field_name = in_recon_field_name
    or recon_field_desc = in_recon_field_name)
    and delete_flag = 'N') then
    if in_action = 'INSERT' then
      call pr_recon_mst_treconfield(in_reconfield_gid,in_recon_code,in_recon_field_name,in_recon_field_name,
                                  v_field_type,in_display_order,'Y','INSERT',in_action_by,@msg,@result);
    end if;
  end if;

  if exists(select * from recon_mst_treconfield
    where reconfield_gid = in_reconfield_gid
    and delete_flag = 'N') then
    if in_action = 'UPDATE' then
      call pr_recon_mst_treconfield(in_reconfield_gid,in_recon_code,in_recon_field_name,in_recon_field_name,
                                  v_field_type,in_display_order,'Y','UPDATE',in_action_by,@msg,@result);
    end if;
  end if;

  select recon_field_name into in_recon_field_name from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_desc = in_recon_field_name
  and active_status = 'Y'
  and delete_flag = 'N';

  set in_recon_field_name = ifnull(in_recon_field_name,'');

	if (in_action = 'INSERT' or in_action = 'UPDATE') and in_recon_field_name <> '' then
		if in_reconfieldmapping_gid = 0 then
			if not exists(select * from recon_mst_treconfieldmapping
				where recon_code = in_recon_code
				and recon_field_name = in_recon_field_name
				and dataset_code = in_dataset_code
				and dataset_field_name = in_dataset_field_name
				and delete_flag = 'N') then
				insert into recon_mst_treconfieldmapping
				(
					recon_code,
					recon_field_name,
					dataset_code,
					dataset_field_name,
					active_status,
					insert_date,
					insert_by
				) value
				(
					in_recon_code,
					in_recon_field_name,
					in_dataset_code,
					in_dataset_field_name,
					in_active_status,
					sysdate(),
					in_action_by
				);
			else
				update recon_mst_treconfieldmapping set
					recon_code = in_recon_code,
					recon_field_name = in_recon_field_name,
					dataset_code = in_dataset_code,
					dataset_field_name = in_dataset_field_name,
					active_status = in_active_status,
					update_date = sysdate(),
					update_by = in_action_by
				where reconfieldmapping_gid = in_reconfieldmapping_gid
				and delete_flag = 'N';
			end if;
		end if;
	elseif in_action = 'DELETE' then
		update recon_mst_treconfield set
			active_status = 'N',
			update_date = sysdate(),
			update_by = in_action_by
		where reconfield_gid = in_reconfield_gid
		and delete_flag = 'N';

		update recon_mst_treconfieldmapping set
			active_status = 'N',
			update_date = sysdate(),
			update_by = in_action_by
		where reconfieldmapping_gid = in_reconfieldmapping_gid
		and delete_flag = 'N';
	end if;

	set out_result = 1;
	set out_msg = 'Record updated successfully !';

  -- check record is in recon_mst_treconfield table

  /*
  if (in_action = 'INSERT' and in_reconfield_gid = 0 and in_reconfieldmapping_gid = 0) then
		insert into recon_mst_treconfield
		(
			recon_code,
			recon_field_name,
			recon_field_desc,
			display_order,
      active_status,
			insert_date,
			insert_by
		)
		value
    (
			in_recon_code,
			in_recon_field_name,
			in_recon_field_name,
			in_display_order,
      in_active_status,
			sysdate(),  
			in_action_by  
		);
		
		insert into recon_mst_treconfieldmapping
    ( 
			recon_code, 
			recon_field_name, 
			dataset_code, 
			dataset_field_name,
      active_status, 
			insert_date,  
			insert_by  
		)  
		value
    (
			in_recon_code,  
			in_recon_field_name,
			in_dataset_code,
			in_dataset_field_name,  
			in_active_status,
			sysdate(),  
			in_action_by  
		);
		
    select max(reconfield_gid) into v_reconfield_gid from recon_mst_treconfield;
		set in_reconfield_gid = v_reconfield_gid;
		
		set v_msg = 'Record inserted successfully.. !';
		
    set v_field_type = (select field_type from recon_mst_tdatasetfield 
			where dataset_code = in_dataset_code 
			and dataset_table_field = in_dataset_field_name
			and delete_flag = 'N');
			
    update recon_mst_treconfield set
			recon_field_type = v_field_type
    where recon_code = in_recon_code 
    and recon_field_name = in_recon_field_name
    and delete_flag = 'N';
		
	elseif(in_action = 'UPDATE' and in_reconfield_gid > 0 and in_reconfieldmapping_gid > 0) then
		update recon_mst_treconfield set
			recon_code = in_recon_code,
			recon_field_name = in_recon_field_name,
			recon_field_desc = in_recon_field_name,
			display_order = in_display_order,
			update_date = sysdate(),
			update_by = in_action_by
		where reconfield_gid = in_reconfield_gid
		and delete_flag = 'N';
			
		update recon_mst_treconfieldmapping set
			recon_code = in_recon_code,
			recon_field_name = in_recon_field_name,
			dataset_code = in_dataset_code,
			dataset_field_name = in_dataset_field_name,
			update_date = sysdate(),
			update_by = in_action_by
		where reconfieldmapping_gid = in_reconfieldmapping_gid
		and delete_flag = 'N';

    set v_reconfield_gid = in_reconfield_gid;
		set v_msg = 'Record updated successfully.. !';
	elseif(in_action = 'INSERT' and in_reconfield_gid > 0 and in_reconfieldmapping_gid = 0 ) then
		update recon_mst_treconfield set
				recon_code = in_recon_code,
				recon_field_name = in_recon_field_name,
				recon_field_desc = in_recon_field_name,
				display_order = in_display_order,
				update_date = sysdate(),
				update_by = in_action_by
			where reconfield_gid = in_reconfield_gid
			and delete_flag = 'N';
		
      set v_field_type = (select field_type from recon_mst_tdatasetfield where dataset_code = in_dataset_code and dataset_table_field = in_dataset_field_name);
			set v_fieldtype_id = (select recon_field_type from recon_mst_treconfield where reconfield_gid = in_reconfield_gid);
			
      if(v_field_type = v_fieldtype_id) then
				insert into recon_mst_treconfieldmapping
        (
					recon_code,
					recon_field_name,
					dataset_code,
					dataset_field_name,
          active_status,
					insert_date,
					insert_by
				) value
        (
					in_reconfieldmapping_gid,
					in_recon_code,
					in_recon_field_name,
					in_dataset_code,
          in_dataset_field_name,
					in_active_status,
					sysdate(),
					in_action_by
				);

        set v_reconfield_gid = in_reconfield_gid;
				set v_msg = 'Record updated successfully.. !';
      else
				set v_msg = 'Invalid field type';
			end if;
    elseif(in_action = 'UPDATE' and in_reconfield_gid > 0 and in_reconfieldmapping_gid = 0 and in_dataset_code='') then
			update recon_mst_treconfield set
				recon_code = in_recon_code,
				recon_field_name = in_recon_field_name,
				recon_field_desc = in_recon_field_name,
				display_order = in_display_order,
				update_date = sysdate(),
				update_by = in_action_by
			where reconfield_gid = in_reconfield_gid
			and delete_flag = 'N';
		set v_msg = 'Record updated successfully.. !';
         
		elseif(in_action = 'DELETE' and in_reconfield_gid > 0 and in_reconfieldmapping_gid = 0 ) then
			update recon_mst_treconfield set
				update_date = sysdate(),
				update_by = in_action_by,
				delete_flag = 'Y'  
			where reconfield_gid = in_reconfield_gid
			and delete_flag = 'N';
            
            update recon_mst_treconfieldmapping set
				update_date = sysdate(),
				update_by = in_action_by,
				delete_flag = 'Y'  
			where recon_code = in_recon_code
			and delete_flag = 'N';
             set v_reconfield_gid = in_reconfield_gid;
			set v_msg = 'Record deleted successfully.. !';
		elseif(in_action = 'DELETE' and in_reconfield_gid = 0 and in_reconfieldmapping_gid > 0 ) then
            update recon_mst_treconfieldmapping set
				update_date = sysdate(),
				update_by = in_action_by,
				delete_flag = 'Y'  
			where reconfieldmapping_gid = in_reconfieldmapping_gid
			and delete_flag = 'N';   
            set v_reconfield_gid = in_reconfield_gid;
			set v_msg = 'Record deleted successfully.. !';
   end if;
  set in_reconfield_gid=v_reconfield_gid;
    set out_result = 1;
	set out_msg = v_msg;
  */
END $$

DELIMITER ;