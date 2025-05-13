DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_truleaggfun` $$
CREATE PROCEDURE `pr_recon_mst_truleaggfun`(
	inout in_ruleaggfield_gid int(10),
	in in_rule_code varchar(32),
	in in_recon_field varchar(32),
	in in_ruleaggfield_seqno decimal(6,2),
	in in_ruleaggfield_applied_on varchar(32),
	in in_ruleaggfield_desc varchar(255),
	in in_ruleagg_function varchar(255),
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
	declare v_ruleaggfield_gid int default 0;
	declare v_ruleagg_field_sno int default 0;
	declare v_ruleagg_field text default '';
	declare v_ruleagg_field_type text default '';
	declare v_ruleagg_function text default '';
	declare v_count int default 0;
	declare v_msg text default '';
	declare v_rule_group text default '';
	declare v_field_type text default '';
	declare v_recon_code text default '';

	set v_recon_code= (select recon_code from recon_mst_trule
		where rule_code=in_rule_code
		and delete_flag = 'N');

  set v_ruleagg_function = trim(upper(in_ruleagg_function));

  if mid(v_ruleagg_function,1,3) = 'SUM' then
    set v_field_type = 'NUMERIC';
  elseif mid(v_ruleagg_function,1,3) = 'MIN' then
    set v_field_type = 'NUMERIC';
  elseif mid(v_ruleagg_function,1,3) = 'MAX' then
    set v_field_type = 'NUMERIC';
  elseif mid(v_ruleagg_function,1,5) = 'COUNT' then
    set v_field_type = 'INTEGER';
  elseif mid(v_ruleagg_function,1,3) = 'AVG' then
    set v_field_type = 'NUMERIC';
  elseif mid(v_ruleagg_function,1,3) = 'ABS' then
    set v_field_type = 'NUMERIC';
  else
    set v_field_type = 'TEXT';
  end if;

	if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if in_recon_field = '' or in_recon_field is null then
			set err_msg := concat(err_msg,'Recon Field cannot be empty,');
			set err_flag := true;
		end if;

		if in_ruleaggfield_desc = '' or in_ruleaggfield_desc is null then
			set err_msg := concat(err_msg,'Aggregate Function Desc cannot be empty,');
			set err_flag := true;
		end if;

		if in_ruleagg_function = '' or in_ruleagg_function is null then
			set err_msg := concat(err_msg,'Aggregate Function cannot be empty,');
			set err_flag := true;
		end if;

		if in_ruleaggfield_seqno = 0 or in_ruleaggfield_seqno is null then
			set err_msg := concat(err_msg,'Sequence number cannot be Zero,');
			set err_flag := true;
		end if;
	end if;

	if in_action = 'INSERT' then
		if exists (select ruleaggfield_seqno from recon_mst_truleaggfield
			where rule_code = in_rule_code
			and ruleaggfield_seqno = in_ruleaggfield_seqno
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Dublicate Sequence no.,');
			set err_flag := true;
		end if;
	elseif in_action = 'UPDATE' then
		if exists (select ruleaggfield_seqno from recon_mst_truleaggfield
			where rule_code = in_rule_code
			and ruleaggfield_seqno=in_ruleaggfield_seqno
			and ruleaggfield_gid <> in_ruleaggfield_gid
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Dublicate Sequence no.,');
			set err_flag := true;
		end if;
	end if;

  if in_action = "UPDATE"  or in_action = "DELETE" then
		if in_ruleaggfield_gid = ''
			or in_ruleaggfield_gid is null
			or in_ruleaggfield_gid = 0 then
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
    select count(*) into v_count from recon_mst_truleaggfield
		where rule_code=in_rule_code;

		if v_count = 0 then
			set v_ruleagg_field_sno = 128;
			set v_ruleagg_field = 'col128';
			set v_ruleagg_field_type =v_field_type;
		else
			set v_ruleagg_field_sno = (select MIN(ruleagg_field_sno) from recon_mst_truleaggfield	where rule_code = in_rule_code) - 1;
			set v_ruleagg_field = concat('col',v_ruleagg_field_sno);
			set v_ruleagg_field_type =v_field_type;
		end if;

		insert into recon_mst_truleaggfield
		(
        rule_code,
        ruleaggfield_seqno,
        ruleaggfield_applied_on,
        ruleaggfield_desc,
        recon_field,
        ruleagg_function,
        ruleagg_field,
        ruleagg_field_sno,
        ruleagg_field_type,
        active_status,
        insert_date,
        insert_by
		)
		value
		(
        in_rule_code,
        in_ruleaggfield_seqno,
        in_ruleaggfield_applied_on,
        in_ruleaggfield_desc,
        in_recon_field,
        in_ruleagg_function,
        v_ruleagg_field,
        v_ruleagg_field_sno,
        v_ruleagg_field_type,
        in_active_status,
        sysdate(),
        in_action_by
    );

    select max(ruleaggfield_gid) into v_ruleaggfield_gid from recon_mst_truleaggfield;

		set in_ruleaggfield_gid = v_ruleaggfield_gid;
		set v_msg = 'Record saved successfully.. !';

  elseif(in_action = 'UPDATE') then
    set v_ruleagg_field_type = v_field_type;

		update recon_mst_truleaggfield set
			rule_code = in_rule_code,
			ruleaggfield_seqno= in_ruleaggfield_seqno,
			ruleaggfield_applied_on =in_ruleaggfield_applied_on,
			ruleaggfield_desc = in_ruleaggfield_desc,
			recon_field=in_recon_field,
			ruleagg_function=in_ruleagg_function,
			ruleagg_field_type = v_ruleagg_field_type,
			update_date = sysdate(),
			update_by = in_action_by
		where ruleaggfield_gid = in_ruleaggfield_gid
		and active_status = 'Y'
		and delete_flag = 'N';

    set v_ruleaggfield_gid = in_ruleaggfield_gid;
		set v_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then
		update recon_mst_truleaggfield set
			update_date = sysdate(),
			update_by = in_action_by,
			active_status='N',delete_flag='Y'
		where ruleaggfield_gid = in_ruleaggfield_gid
		and delete_flag = 'N';

		set v_ruleaggfield_gid = in_ruleaggfield_gid;
		set v_msg = 'Record deleted successfully.. !';
  end if;

	commit;

	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;