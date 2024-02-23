DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_trecon` $$
CREATE PROCEDURE `pr_recon_mst_trecon`
(
  inout in_recon_gid int(10),
  in in_recon_code varchar(32),
  in in_recon_name varchar(255),
  in in_recontype_code varchar(32),
  in in_recon_automatch_partial char(1),
  in in_period_from date,
  in in_period_to date,
  in in_until_active_flag char(1),
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
	declare v_recon_gid int default 0;
	declare v_msg text default '';

  set in_recon_code = ifnull(in_recon_code,'');
  set in_recon_name = ifnull(in_recon_name,'');
  set in_recontype_code = ifnull(in_recontype_code,'');
  set in_until_active_flag = ifnull(in_until_active_flag,'');

	if in_action = "UPDATE"  or in_action = "INSERT" then
		if in_recon_name = '' or in_recon_name is null then
			set err_msg := concat(err_msg,'Recon Name cannot be empty,');
			set err_flag := true;
		end if;

    if in_recon_code = '' or in_recon_code is null then
			set err_msg := concat(err_msg,'Recon Code cannot be empty,');
			set err_flag := true;
		end if;

		if in_recontype_code = '' or in_recontype_code is null then
			set err_msg := concat(err_msg,'Type Code cannot be empty,');
			set err_flag := true;
		end if;

/*		if in_recon_automatch_partial = '' or in_recon_automatch_partial is null then
			set err_msg := concat(err_msg,'Recon Automatch cannot be empty,');
			set err_flag := true;
		end if;*/

    if in_period_from = '' or in_period_from is null then
			set err_msg := concat(err_msg,'Preiod from cannot be empty,');
			set err_flag := true;
		end if;

    if in_until_active_flag <> 'Y' and in_until_active_flag <> 'N' then
			set err_msg := concat(err_msg,'Invalid until active flag,');
			set err_flag := true;
		elseif in_until_active_flag = 'Y' then
			set in_period_to = null;
		else
			if in_period_to is null then
				set err_msg := concat(err_msg,'Invalid period to,');
				set err_flag := true;
			end if;
		end if;

    if in_active_status <> 'Y' and in_active_status <> 'N' and in_active_status <> 'D' or in_active_status is null then
			set err_msg := concat(err_msg,'Invalid active status value,');
			set err_flag := true;
		end if;
	end if;


  if in_action = "UPDATE"  or in_action = "DELETE" then
		if in_recon_gid = '' or in_recon_gid is null or in_recon_gid = 0 then
			set err_msg := concat(err_msg,'Invalid gid,');
			set err_flag := true;
		end if;

		if not exists(select recon_gid from recon_mst_trecon
			where recon_gid = in_recon_gid
            and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid record,');
			set err_flag := true;
		end if;
  end if;

  if in_action = "INSERT" then
		if exists(select recon_gid from recon_mst_trecon
			where recon_code = in_recon_code
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate record,');
			set err_flag := true;
		end if;
  end if;

  if in_action = "UPDATE" then
		if exists(select recon_gid from recon_mst_trecon
			where recon_code = in_recon_code
      and recon_gid <> in_recon_gid
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
	if(in_until_active_flag = 'Y') then
	  set in_period_to = null;
	end if;

  if (in_action = 'INSERT') then
		set in_recon_gid = 0;

		insert into recon_mst_trecon
    (
      recon_gid,
      recon_code,
      recon_name,
      recontype_code,
			recon_automatch_partial,
      period_from,
      period_to,
			until_active_flag,
      active_status,
      insert_date,
      insert_by
    )
    value
    (
			in_recon_gid,
      in_recon_code,
      in_recon_name,
      in_recontype_code,
			in_recon_automatch_partial,
      in_period_from,
      in_period_to,
			in_until_active_flag,
      in_active_status,
      sysdate(),
      in_user_code
    );

    select max(recon_gid) into v_recon_gid from recon_mst_trecon;
		set in_recon_gid = v_recon_gid;

		set v_msg = 'Record saved successfully.. !';

	elseif(in_action = 'UPDATE') then
		update recon_mst_trecon set
			recon_name = in_recon_name,
			recontype_code = in_recontype_code,
			recon_automatch_partial = in_recon_automatch_partial,
			period_from = in_period_from,
			period_to = in_period_to,
			until_active_flag = in_until_active_flag,
      active_status=in_active_status,
			update_date = sysdate(),
			update_by = in_user_code
		where recon_gid = in_recon_gid
    and delete_flag = 'N';

		set v_recon_gid = in_recon_gid;
		set v_msg = 'Record Updated Successfully.. !';
  elseif(in_action = 'DELETE') then
		update recon_mst_trecon set
			update_date = sysdate(),
			update_by = in_action_by,
			delete_flag = 'Y'
		where recon_gid = in_recon_gid
    and delete_flag = 'N';

    set v_recon_gid = in_recon_gid;
		set v_msg = 'Record deleted successfully.. !';
	end if;

  commit;

	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;