DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_tdatasetfield` $$
CREATE PROCEDURE `pr_recon_mst_tdatasetfield`
(
	inout in_datasetfield_gid int(10),
	in in_dataset_code varchar(32),
	in in_field_name varchar(32),
	in in_field_type varchar(32),
	in in_field_length varchar(16),
	in in_precision_length int(10),
	in in_scale_length int(10),
	in in_field_mandatory char(1),
	in in_action varchar(16),
	in in_action_by varchar(10),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN
	/*
		Created By : Hema
		Created Date : Sep-29-2023

		Updated By : Vijayavel J
		Updated Date : Dec-02-2023

		Version No : 3
	*/

	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_datasetfield_gid int default 0;
	declare v_msg text default '';
  declare v_count int default 0;
	
	if(in_action = 'INSERT' or in_action = 'UPDATE') then
    if (in_action = 'INSERT') then set in_datasetfield_gid = 0; end if;

		if not exists(select dataset_code from recon_mst_tdataset
			where dataset_code = in_dataset_code
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid dataset code');
			set err_flag := true;
		end if;

    if in_field_name = '' or in_field_name is null then
			set err_msg := concat(err_msg,'Field name cannot be empty,');
			set err_flag := true;
		end if;

		if in_field_type = '' or in_field_type is null then
			set err_msg := concat(err_msg,'Field type cannot be empty,');
			set err_flag := true;
		end if;

		if in_field_type = "TEXT" then
			if in_field_length = '' or in_field_length is null then
				set err_msg := concat(err_msg,'Field length cannot be empty,');
				set err_flag := true;
			end if;
		end if;

		if in_field_mandatory = '' or in_field_mandatory is null then
			set err_msg := concat(err_msg,'Field mandatory cannot be empty,');
			set err_flag := true;
		end if;

		if in_field_type = "DATE" or in_field_type = "DATETIME" then
			set in_field_length = '';
			set in_precision_length = null;
			set in_scale_length = null;
		elseif in_field_type = "TEXT" then
			set in_precision_length = null;
			set in_scale_length = null;
		elseif in_field_type = "INTEGER" then
			set in_precision_length = null;
			set in_scale_length = null;
		elseif in_field_type = "NUMERIC" then
			set in_precision_length = ifnull(in_precision_length,0);
			set in_scale_length = ifnull(in_scale_length,0);

			if in_precision_length = 0 then
				set err_msg := concat(err_msg,'Precision length cannot be zero,');
				set err_flag := true;
			end if;

			if in_scale_length = 0 then
				set err_msg := concat(err_msg,'Scale length cannot be zero,');
				set err_flag := true;
			end if;

			set in_field_length = concat(cast(in_precision_length+in_scale_length as nchar),',',cast(in_scale_length as nchar));
		end if;

    -- duplicate validate
    if exists(select * from recon_mst_tdatasetfield
      where dataset_code = in_dataset_code
      and field_name = in_field_name
      and datasetfield_gid <> in_datasetfield_gid
      and delete_flag = 'N') then
      set err_msg := concat(err_msg,'Duplicate dataset field,');
      set err_flag := true;
    end if;
	end if;

	if(in_action = 'UPDATE' or in_action = 'DELETE') then
		if in_datasetfield_gid = '' or in_datasetfield_gid is null then
			set err_msg := concat(err_msg,'Invalid datasetfield gid,');
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
		set in_datasetfield_gid = 0;

		select count(*)+1 into v_count from recon_mst_tdatasetfield
		where dataset_code = in_dataset_code;

		insert into recon_mst_tdatasetfield
		(
			dataset_code,
			field_name,
			field_type,
			field_length,
			precision_length,
			scale_length,
			field_mandatory,
			dataset_field_sno,
			dataset_table_field,
			insert_date,
			insert_by
		)
		value
		(
			in_dataset_code,
			in_field_name,
			in_field_type,
			in_field_length,
			in_precision_length,
			in_scale_length,
			in_field_mandatory,
			v_count,
			concat('col',v_count),
			sysdate(),
			in_action_by
		);
		
		select max(datasetfield_gid) into v_datasetfield_gid from recon_mst_tdatasetfield;
		
		set in_datasetfield_gid = v_datasetfield_gid;
		set v_msg = 'Record saved successfully.. !';
	elseif(in_action = 'UPDATE') then
		update recon_mst_tdatasetfield set
			datasetfield_gid = in_datasetfield_gid,
			dataset_code = in_dataset_code,
			field_name = in_field_name,
			field_type = in_field_type,
			field_length = in_field_length,
			precision_length = in_precision_length,       
			scale_length = in_scale_length,
			field_mandatory = in_field_mandatory,
			update_date = sysdate(),
			update_by = in_action_by
		where datasetfield_gid = in_datasetfield_gid
		and delete_flag = 'N';
        
		set v_datasetfield_gid = in_datasetfield_gid;
		set v_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then 
		update recon_mst_tdatasetfield set
			update_date = sysdate(),
			update_by = in_action_by,
			delete_flag = 'Y'  
		where datasetfield_gid = in_datasetfield_gid
		and delete_flag = 'N';
		set v_msg = 'Record deleted successfully.. !';
	end if;
	
	commit;
	
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;