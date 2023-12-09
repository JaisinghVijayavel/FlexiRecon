DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treconfieldmapping` $$
CREATE PROCEDURE `pr_recon_mst_treconfieldmapping`
(
	inout in_reconfieldmapping_gid int(10),
	in in_recon_code varchar(32),
	in in_filetemplate_code varchar(32),
	in in_acc_code varchar(32),
	in in_recon_field_name varchar(255),
	in in_filetemplate_field_name varchar(255),
	in in_action varchar(16),
	in in_action_by varchar(32),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN
	
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_reconfieldmapping_gid int default 0;
	declare v_msg text default '';
  
	if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if not exists(select recon_gid from recon_mst_trecon 
			where recon_code = in_recon_code 
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid recon code');
			set err_flag := true;
		end if;
		
    if in_filetemplate_code = '' or in_filetemplate_code is null then
			set err_msg := concat(err_msg,'File templete cannot be empty,');
			set err_flag := true;
		end if;
		
		if not exists(select acc_gid from recon_mst_tacc 
			where acc_code = in_acc_code
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid account code');
			set err_flag := true;
		end if;
		
		if in_recon_field_name = '' or in_recon_field_name is null then
			set err_msg := concat(err_msg,'Recon field name cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_filetemplate_field_name = '' or in_filetemplate_field_name is null then
			set err_msg := concat(err_msg,'File templete cannot be empty,');
			set err_flag := true;
		end if;
		
    if in_active_status <> 'Y' and in_active_status <> 'N' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;
	end if;
	
  if in_action = "INSERT" then
		if exists(select reconfieldmapping_gid from recon_mst_treconfieldmapping 
			where recon_field_name = in_recon_field_name
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
		set in_reconfieldmapping_gid = 0;  
		
    insert into recon_mst_treconfieldmapping
		(
			reconfieldmapping_gid,
			recon_code,
			filetemplate_code,
			acc_code,
			recon_field_name,
			filetemplate_field_name,
			insert_date,
			insert_by
		)
		value
		(
			in_reconfieldmapping_gid,
			in_recon_code,
			in_filetemplate_code,
			in_acc_code,
			in_recon_field_name,
			in_filetemplate_field_name,
			sysdate(),
			in_action_by
		);
                
				select max(reconfieldmapping_gid) into v_reconfieldmapping_gid from recon_mst_treconfieldmapping;
				set in_reconfieldmapping_gid = v_reconfieldmapping_gid;
				set v_msg = 'Record saved successfully.. !';
			elseif(in_action = 'UPDATE') then
				update recon_mst_treconfieldmapping set
					recon_code = in_recon_code,
                    filetemplate_code = in_filetemplate_code,
                    acc_code = in_acc_code,
                    recon_field_name = in_recon_field_name,
                    filetemplate_field_name = in_filetemplate_field_name,
                    update_date = sysdate(),
                    update_by = in_action_by
				where reconfieldmapping_gid = in_reconfieldmapping_gid
                and delete_flag = 'N';
                
				set v_reconfieldmapping_gid = in_reconfieldmapping_gid;
				set v_msg = 'Record Updated Successfully.. !';
			elseif(in_action = 'DELETE') then 
				update recon_mst_treconfieldmapping set
					update_date = sysdate(),
                    update_by = in_action_by,
                    delete_flag = 'Y'  
				where reconfieldmapping_gid = in_reconfieldmapping_gid
                and delete_flag = 'N';
                
				set v_reconfieldmapping_gid = in_reconfieldmapping_gid;
				set v_msg = 'Record deleted successfully.. !';
        end if;
    commit;
    
    	set out_result = 1;
		set out_msg = v_msg;
END $$

DELIMITER ;