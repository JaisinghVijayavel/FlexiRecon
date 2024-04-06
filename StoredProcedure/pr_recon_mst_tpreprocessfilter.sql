DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_tpreprocessfilter` $$
CREATE PROCEDURE `pr_recon_mst_tpreprocessfilter`
(
	inout in_preprocessfilter_gid int,
	in in_preprocess_code varchar(32),	
  in in_recon_code varchar(32),
	in in_filter_seqno double,
	in in_filter_field varchar(255),
	in in_filter_criteria varchar(255),	
	in in_filter_value varchar(255),
	in in_open_flag varchar(32),
	in in_close_flag varchar(32),
	in in_join_condition varchar(32),
	in in_active_status char(1),
	in in_action varchar(32),
	in in_action_by varchar(32),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN

	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_preprocessfilter_gid int default 0;
	declare v_msg text default '';
  declare v_recon_code text default '';
  declare v_count int default 0;
	
	if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if not exists(select in_preprocess_code from recon_mst_tpreprocess
			where preprocess_code = in_preprocess_code 
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid preprocess code');
			set err_flag := true;
		end if;	
				
		if in_filter_criteria = '' or in_filter_criteria is null then
			set err_msg := concat(err_msg,'Filter criteria on cannot be empty,');
			set err_flag := true;
		end if;

		if in_filter_value = '' or in_filter_value is null then
			set err_msg := concat(err_msg,'Identifier value on cannot be empty,');
			set err_flag := true;
		end if;

		if in_active_status <> 'Y' and in_active_status <> 'N' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;
 

    -- recon field
    if not exists(select recon_field_name from recon_mst_treconfield
      where recon_code = in_recon_code
      and recon_field_name = in_filter_field
      and active_status = 'Y'
      and delete_flag = 'N') then
      select
        recon_field_name into in_filter_field
      from recon_mst_treconfield
      where recon_code = in_recon_code
      and recon_field_desc = in_filter_field
      and active_status = 'Y'
      and delete_flag = 'N';
    end if;

    set in_filter_field = ifnull(in_filter_field,'');

		if in_filter_field = '' then
			set err_msg := concat(err_msg,'Invalid filter field,');
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
		set in_preprocessfilter_gid = 0;
		
		select count(*)+1 into v_count from recon_mst_tpreprocessfilter 
		where preprocess_code=in_preprocess_code;
		
		INSERT INTO recon_mst_tpreprocessfilter
		(
			preprocess_code,
			filter_seqno,
			filter_field,
			filter_criteria,
			filter_value,
			open_parentheses_flag,
			close_parentheses_flag,
			join_condition,
			active_status,
			insert_date,
			insert_by
		)
		VALUES
		(
			in_preprocess_code,
			in_filter_seqno,
			in_filter_field,
			in_filter_criteria,
			in_filter_value,
			in_open_flag,
			in_close_flag,
			in_join_condition,
			in_active_status,
			sysdate(),
			in_user_code
		);
		
		select max(preprocessfilter_gid) into v_preprocessfilter_gid from recon_mst_tpreprocessfilter;
		set in_preprocessfilter_gid = v_preprocessfilter_gid;
		set v_msg = 'Record saved successfully.. !';  
	elseif(in_action = 'UPDATE') then
		UPDATE recon_mst_tpreprocessfilter SET
			preprocess_code =in_preprocess_code,
			filter_seqno =in_filter_seqno,
			filter_field = in_filter_field,
			filter_criteria = in_filter_criteria,
			filter_value = in_filter_value,
			open_parentheses_flag = in_open_flag,
			close_parentheses_flag = in_close_flag,
			join_condition = in_join_condition,
			active_status = in_active_status,
			update_date = sysdate(),
			update_by = in_user_code
		WHERE preprocessfilter_gid = in_preprocessfilter_gid
		and delete_flag = 'N';     
		
		set in_preprocessfilter_gid = in_preprocessfilter_gid;
		set v_msg = 'Record updated successfully.. !';  
	elseif(in_action = 'DELETE') then
		update recon_mst_tpreprocessfilter set
			update_date = sysdate(),
			update_by = in_action_by,
			active_status='N'		
		where preprocessfilter_gid = in_preprocessfilter_gid
		and delete_flag = 'N';   
		
		set in_preprocessfilter_gid = in_preprocessfilter_gid;
		set v_msg = 'Record deleted successfully.. !';  
	end if;
	
	commit;
	
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;