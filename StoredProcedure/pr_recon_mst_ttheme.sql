DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_ttheme` $$
CREATE PROCEDURE `pr_recon_mst_ttheme`
(
  inout in_theme_gid int,
  inout in_theme_code varchar(32),
  in in_theme_name varchar(32),
  in in_theme_order varchar(32),
  in in_recon_code varchar(32), 
  in in_hold_flag varchar(32), 
  in in_clone_theme varchar(32), 
  in in_active_status char(1),
  in in_user_code varchar(32),
  in in_role_code varchar(32),
  in in_lang_code varchar(32),
  in in_action varchar(32),
  in in_action_by varchar(32),
  out out_msg text,
  out out_result int
)
me:BEGIN	

	declare v_msg text default '';

	declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_count int default 0;
  declare v_theme_gid int default 0;
		
	if(in_action = 'INSERT' or in_action = 'UPDATE') then  
		if not exists(select recon_gid from recon_mst_trecon 
			where recon_code = in_recon_code 
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid recon code');
			set err_flag := true;
		end if;
		
		if in_theme_name = '' or in_theme_name is null then
			set err_msg := concat(err_msg,'Theme Descripition cannot be empty,');
			set err_flag := true;
		end if;

    if in_theme_order = '' or in_theme_order is null then
			set err_msg := concat(err_msg,'Theme Order cannot be empty,');
			set err_flag := true;
		end if;

    if in_hold_flag <> 'Y' and in_hold_flag <> 'N'  or in_hold_flag is null then
			set err_msg := concat(err_msg,'Invalid Hold Flag value,');
			set err_flag := true;
		end if;
	end if;

   -- Duplicate validation
  if in_action = 'INSERT' then
		if exists (select theme_gid from recon_mst_ttheme
      where theme_desc = in_theme_name
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate Theme,');
			set err_flag := true;
		end if;
  elseif in_action = 'UPDATE' then
		if exists (select theme_gid from recon_mst_ttheme
      where theme_desc = in_theme_name
      and theme_gid <> in_theme_gid
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate Theme,');
			set err_flag := true;
		end if;
  end if;

  if (in_action = 'INSERT') then
    set in_theme_code = fn_get_autocode('THEME');

   	select count(*)+1 into v_count from recon_mst_ttheme;

		INSERT INTO recon_mst_ttheme
		(
			theme_code,
			theme_desc,
			recon_code,
			theme_order,
			hold_flag,
			active_status,
			insert_date,
			insert_by
		)
		VALUES
		(
			in_theme_code,
			in_theme_name,
			in_recon_code,
			v_count,
			in_hold_flag,
			in_active_status,
			sysdate(),
			in_user_code
		);

		select max(theme_gid) into v_theme_gid from recon_mst_ttheme;

		if in_clone_theme is not null then
			call pr_recon_mst_theme_clone(in_clone_theme,in_theme_code, in_action,in_user_code, @out_msg, @out_result);
    end if;

		set in_theme_gid = v_theme_gid;

		set out_result = 1;
		set out_msg = 'Record saved successfully.. !';

	elseif(in_action = 'UPDATE') then
		UPDATE recon_mst_ttheme SET
			theme_code = in_theme_code,
			theme_desc = in_theme_name,
			theme_order =in_theme_order,
			hold_flag=in_hold_flag,
			recon_code = in_recon_code,
			active_status=in_active_status,
			update_date = sysdate(),
			update_by = in_user_code
		where theme_gid = in_theme_gid
		and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then
		update recon_mst_ttheme set
      active_status='N',
			update_date = sysdate(),
			update_by = in_action_by
		where theme_gid = in_theme_gid
		and delete_flag = 'N';

		set out_result = 1;
		set out_msg = 'Record deleted successfully.. !';
	end if;
END $$

DELIMITER ;