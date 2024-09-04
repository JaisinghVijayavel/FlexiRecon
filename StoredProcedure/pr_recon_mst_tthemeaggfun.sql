DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_tthemeaggfun` $$
CREATE PROCEDURE `pr_recon_mst_tthemeaggfun`(
	inout in_themeaggfield_gid int(10),
	in in_theme_code varchar(32),
	in in_recon_field varchar(32),
	in in_themeaggfield_seqno decimal(6,2),
	in in_themeaggfield_applied_on varchar(32),
	in in_themeaggfield_name varchar(255),
	in in_themeagg_function varchar(255),
	in in_active_status char(1),
	in in_action varchar(16),
	in in_action_by varchar(10),
	in in_user_code varchar(32),
	in in_role_code varchar(32),
	in in_lang_code varchar(32),
	out out_msg text,
	out out_result int(10)
)
me:BEGIN
	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_themeaggfield_gid int default 0;
	declare v_themeagg_field_sno int default 0;
	declare v_themeagg_field text default '';
	declare v_themeagg_field_type text default '';
  declare v_themeagg_function text default '';
	declare v_count int default 0;
	declare v_msg text default '';
	declare v_rule_group text default '';
	declare v_field_type text default '';
	declare v_recon_code text default '';

	set v_recon_code= (select recon_code from recon_mst_ttheme
		where theme_code=in_theme_code
		and delete_flag = 'N');

  /*
	set v_field_type = (select recon_field_type from recon_mst_treconfield
		where recon_field_name = in_recon_field
		and recon_code = v_recon_code and delete_flag = 'N');

  set v_field_type = ifnull(v_field_type,'');

	if v_field_type = '' then
		set v_field_type = (select distinct field_type from recon_mst_tfieldstru
			where field_name = in_recon_field
			and delete_flag = 'N');
	end if;

	set v_field_type = ifnull(v_field_type,'');
  */

  -- get field type
  set v_themeagg_function = trim(upper(in_themeagg_function));

  if mid(v_themeagg_function,1,3) = 'SUM' then
    set v_field_type = 'NUMERIC';
  elseif mid(v_themeagg_function,1,3) = 'MIN' then
    set v_field_type = 'INTEGER';
  elseif mid(v_themeagg_function,1,3) = 'MAX' then
    set v_field_type = 'INTEGER';
  elseif mid(v_themeagg_function,1,5) = 'COUNT' then
    set v_field_type = 'INTEGER';
  elseif mid(v_themeagg_function,1,3) = 'AVG' then
    set v_field_type = 'NUMERIC';
  else
    set v_field_type = 'TEXT';
  end if;

	if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if in_recon_field = '' or in_recon_field is null then
			set err_msg := concat(err_msg,'Recon Field cannot be empty,');
			set err_flag := true;
		end if;

		if in_themeaggfield_name = '' or in_themeaggfield_name is null then
			set err_msg := concat(err_msg,'Aggregate Function Desc cannot be empty,');
			set err_flag := true;
		end if;

		if in_themeagg_function = '' or in_themeagg_function is null then
			set err_msg := concat(err_msg,'Aggregate Function cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_themeaggfield_seqno = 0 or in_themeaggfield_seqno is null then
			set err_msg := concat(err_msg,'Sequence number cannot be Zero,');
			set err_flag := true;
		end if;
	end if;
	
	if in_action = 'INSERT' then
		if exists (select themeaggfield_seqno from recon_mst_tthemeaggfield	
			where theme_code = in_theme_code 
			and themeaggfield_seqno = in_themeaggfield_seqno 
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Dublicate Sequence no.,');
			set err_flag := true;
		end if;
	elseif in_action = 'UPDATE' then
		if exists (select themeaggfield_seqno from recon_mst_tthemeaggfield 
			where theme_code = in_theme_code 
			and themeaggfield_seqno=in_themeaggfield_seqno 
			and themeaggfield_gid <> in_themeaggfield_gid 
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Dublicate Sequence no.,');
			set err_flag := true;
		end if;
	end if; 
	
  if in_action = "UPDATE"  or in_action = "DELETE" then
		if in_themeaggfield_gid = '' 
			or in_themeaggfield_gid is null 
			or in_themeaggfield_gid = 0 then
			set err_msg := concat(err_msg,'Invalid group gid,');
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
    select count(*) into v_count from recon_mst_tthemeaggfield 
		where theme_code=in_theme_code;
		
		if v_count = 0 then
			set v_themeagg_field_sno = 128;
			set v_themeagg_field = 'col128';
			set v_themeagg_field_type =v_field_type;
		else 
			set v_themeagg_field_sno = (select MIN(themeagg_field_sno) from recon_mst_tthemeaggfield 	where theme_code = in_theme_code) - 1;
			set v_themeagg_field = concat('col',v_themeagg_field_sno);
			set v_themeagg_field_type =v_field_type;
		end if;

		insert into recon_mst_tthemeaggfield
		(
        theme_code,
        themeaggfield_seqno,
        themeaggfield_applied_on,
        themeaggfield_name,
        recon_field,
        themeagg_function,
        themeagg_field,
        themeagg_field_sno,
        themeagg_field_type,
        active_status,
        insert_date,
        insert_by
		)
		value
		(
        in_theme_code,
        in_themeaggfield_seqno,
        in_themeaggfield_applied_on,
        in_themeaggfield_name,
        in_recon_field,
        in_themeagg_function,
        v_themeagg_field,
        v_themeagg_field_sno,
        v_themeagg_field_type,
        in_active_status,
        sysdate(),
        in_action_by
    );
		
    select max(themeaggfield_gid) into v_themeaggfield_gid from recon_mst_tthemeaggfield;
		
		set in_themeaggfield_gid = v_themeaggfield_gid;
		set v_msg = 'Record saved successfully.. !';
		
  elseif(in_action = 'UPDATE') then
    set v_themeagg_field_type = v_field_type;

		update recon_mst_tthemeaggfield set
			theme_code = in_theme_code,
			themeaggfield_seqno= in_themeaggfield_seqno,
			themeaggfield_applied_on =in_themeaggfield_applied_on,
			themeaggfield_name = in_themeaggfield_name,
			recon_field=in_recon_field,
			themeagg_function=in_themeagg_function,
      themeagg_field_type = v_themeagg_field_type,
			update_date = sysdate(),
			update_by = in_action_by
		where themeaggfield_gid = in_themeaggfield_gid 
		and active_status = 'Y' 
		and delete_flag = 'N';
		
    set v_themeaggfield_gid = in_themeaggfield_gid;
		set v_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then 
		update recon_mst_tthemeaggfield set
			update_date = sysdate(),
			update_by = in_action_by,
			active_status='N',delete_flag='Y'
		where themeaggfield_gid = in_themeaggfield_gid 
		and delete_flag = 'N';
		
		set v_themeaggfield_gid = in_themeaggfield_gid;
		set v_msg = 'Record deleted successfully.. !';
  end if;
	
	commit;
	
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;