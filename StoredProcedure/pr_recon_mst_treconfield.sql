DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treconfield` $$
CREATE PROCEDURE `pr_recon_mst_treconfield`
(
	inout in_reconfield_gid int(10),
	in in_recon_code varchar(32),
	in in_field_name varchar(255),
	in in_field_alias_name varchar(255),
	in in_fieldtype_code varchar(32),
	in in_display_order decimal(6,2),
  in in_active_status char(1),
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
		Updated Date : Nov-08-2023

		Version No : 2
	*/
	
	declare err_msg text default '';
	declare err_flag boolean default false;
  declare v_col text default '';
	declare v_reconfield_gid int default 0;
  declare v_result int default 0;
	declare v_msg text default '';
  
	if in_action = "UPDATE"  or in_action = "INSERT" then
		if in_recon_code = '' or in_recon_code is null then
			set err_msg := concat(err_msg,'Recon code cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_field_name = '' or in_field_name is null then
			set err_msg := concat(err_msg,'Field name cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_field_alias_name = '' or in_field_alias_name is null then
			set err_msg := concat(err_msg,'Field alies name cannot be empty,');
			set err_flag := true;
		end if;

    /*
		if in_fieldtype_code = '' or in_fieldtype_code is null then
			set err_msg := concat(err_msg,'Field typecode cannot be empty,');
			set err_flag := true;
		end if;
    */

		if in_active_status <> 'Y' and in_active_status <> 'N' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;

		if not exists(select recon_gid from recon_mst_trecon
			where recon_code = in_recon_code
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid recon code');
			set err_flag := true;
		end if;

    if in_action = "INSERT" then
		  if exists(select reconfield_gid from recon_mst_treconfield
			  where display_order = in_display_order
        and recon_code = in_recon_code
        and active_status = 'Y'
        and delete_flag = 'N') then
			  set err_msg := concat(err_msg,'Duplicate display order,');
			  set err_flag := true;
		  end if;
    elseif in_action = "UPDATE" then
		  if exists(select reconfield_gid from recon_mst_treconfield
			  where display_order = in_display_order
        and recon_code = in_recon_code
        and reconfield_gid <> in_reconfield_gid
        and active_status = 'Y'
        and delete_flag = 'N') then
			  set err_msg := concat(err_msg,'Duplicate display order,');
			  set err_flag := true;
		  end if;
    end if;
	end if;

  -- duplicate field name
  if in_action = "INSERT" then
		if exists(select reconfield_gid from recon_mst_treconfield
			where recon_field_name = in_field_name
      and recon_code = in_recon_code
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

  start transaction;

  if (in_action = 'INSERT') then
		set in_reconfield_gid = 0;

    -- get col
    select
      max(cast(replace(recon_field_name,'col','') as unsigned)) into v_result
    from recon_mst_treconfield
    where recon_code = in_recon_code
    and recon_field_name like 'col%'
    and delete_flag = 'N';

    set v_result = ifnull(v_result,0)+1;

    set v_col = concat('col',cast(v_result as nchar));

		insert into recon_mst_treconfield
		(
			recon_code,
			recon_field_name,
      recon_field_desc,
      recon_field_sno,
			recon_field_type,
			display_order,
      active_status,
			insert_date,
			insert_by
		)
		value
		(
			in_recon_code,
			v_col,
			in_field_alias_name,
      v_result,
			in_fieldtype_code,
			in_display_order,
      in_active_status,
			sysdate(),
			in_action_by
		);

		select max(reconfield_gid) into v_reconfield_gid from recon_mst_treconfield;
		set in_reconfield_gid = v_reconfield_gid;
		set v_msg = 'Record saved successfully.. !';
	elseif(in_action = 'UPDATE') then
		update recon_mst_treconfield set
			recon_field_desc = in_field_alias_name,
			recon_field_type = in_fieldtype_code,
			display_order = in_display_order,
      active_status = in_active_status,
			update_date = sysdate(),
			update_by = in_action_by
		where reconfield_gid = in_reconfield_gid
			and delete_flag = 'N';
        
		set v_reconfield_gid = in_reconfield_gid;
		set v_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then 
		update recon_mst_treconfield set
			active_status = 'N',
      delete_flag = 'Y',
			update_date = sysdate(),
			update_by = in_action_by,
			delete_flag = 'Y'  
		where reconfield_gid = in_reconfield_gid
    and delete_flag = 'N';
        
		set v_reconfield_gid = in_reconfield_gid;
		set v_msg = 'Record deleted successfully.. !';
  end if;
    
  commit;
    
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;