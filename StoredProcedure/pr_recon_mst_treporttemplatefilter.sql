DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_recon_mst_treporttemplatefilter` $$
CREATE PROCEDURE `pr_recon_mst_treporttemplatefilter`
(
	inout in_reporttemplatefilter_gid int,
	in in_reporttemplate_code varchar(32),
	in in_filter_seqno int(32),
	in in_report_field varchar(255),
	in in_filter_criteria text,
	in in_filter_value text,
	in in_open_parentheses_flag char(1),
	in in_close_parentheses_flag char(1),
	in in_join_condition varchar(32),
	in in_action  varchar(32),
	in in_action_by varchar(32),
	out out_msg text,
	out out_result int
)
me:BEGIN
	/*
		Created By : Hema
		Created Date : Feb-15-2024
        
		Updated By : Hema
		Updated Date : Mar-01-2024
		
		Version No : 2
	*/
    
  declare err_msg text default '';
	declare err_flag boolean default false;
	declare v_msg text default '';
  declare v_reporttemplatefilter_gid int default 0;
    
  if(in_action = 'INSERT' or in_action = 'UPDATE') then
		if in_filter_seqno = '' or in_filter_seqno is null then
			set err_msg := concat(err_msg,'Seq No cannot be empty,');
			set err_flag := true;
		end if;
		
		if in_report_field = '' or in_report_field is null then
			set err_msg := concat(err_msg,'Report field cannot be empty,');
			set err_flag := true;
		end if;
    
    if in_filter_criteria = '' or in_filter_criteria is null then
			set err_msg := concat(err_msg,'Filter criteria cannot be empty,');
			set err_flag := true;
		end if;
    
		if in_filter_value = '' or in_filter_value is null then
			set err_msg := concat(err_msg,'Filter value cannot be empty,');
			set err_flag := true;
		end if;
		
		if not exists (select report_gid from recon_mst_treport
      where report_code = in_report_field
      and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Invalid report code,');
			set err_flag := true;
		end if;
	end if;

  -- duplicate filter seq
	if in_action ='INSERT' then
		if exists(select reporttemplatefilter_gid from recon_mst_treporttemplatefilter
			where reporttemplate_code = in_reporttemplate_code
			and filter_seqno = in_filter_seqno
			and active_status <> 'N'
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate Seq no.,');
			set err_flag := true;
		end if;
  elseif in_action ='UPDATE' then
		if exists(select reporttemplatefilter_gid from recon_mst_treporttemplatefilter
			where  reporttemplate_code = in_reporttemplate_code
			and filter_seqno = in_filter_seqno
			and active_status <> 'N'
			and reporttemplatefilter_gid <> reporttemplatefilter_gid
			and delete_flag = 'N') then
			set err_msg := concat(err_msg,'Duplicate seq no.,');
			set err_flag := true;
		end if;
	end if;
   
  if(in_action = 'DELETE' or in_action = 'UPDATE') then
		if in_reporttemplatefilter_gid = '' or in_reporttemplatefilter_gid or in_reporttemplatefilter_gid = 0 then
			set err_msg := concat(err_msg,'Invalid reporttemplatefilter_gid,');
			set err_flag := true;
		end if;
  end if;
    
  if (in_action = 'INSERT') then
    insert into recon_mst_treporttemplatefilter
		(
			reporttemplatefilter_gid, 
			reporttemplate_code, 
			filter_seqno, 
			report_field, 
			filter_criteria, 
			filter_value, 
			open_parentheses_flag, 
			close_parentheses_flag, 
			join_condition, 
			active_status, 
			insert_date, 
			insert_by
		) 
		value
		(
			0, 
			in_reporttemplate_code, 
			in_filter_seqno, 
			in_report_field, 
			in_filter_criteria, 
			in_filter_value, 
			in_open_parentheses_flag, 
			in_close_parentheses_flag, 
			in_join_condition, 
			'Y',
			sysdate(), 
			in_action_by
		);
		
		select max(reporttemplatefilter_gid) into v_reporttemplatefilter_gid from recon_mst_treporttemplatefilter;
		
		set in_reporttemplatefilter_gid = v_reporttemplatefilter_gid;
		set v_msg = 'Record saved successfully.. !';
    
	elseif(in_action = 'UPDATE') then
		update recon_mst_treporttemplatefilter set
			reporttemplate_code = in_reporttemplate_code,
			filter_seqno = in_filter_seqno,
			report_field = in_report_field,
			filter_criteria = in_filter_criteria,
			filter_value = in_filter_value,
			open_parentheses_flag = in_open_parentheses_flag,
			close_parentheses_flag = in_close_parentheses_flag,
			join_condition = in_join_condition,
			update_date = sysdate(),
			update_by = in_action_by
		where reporttemplatefilter_gid = in_reporttemplatefilter_gid
		and delete_flag = 'N';

		set v_reporttemplatefilter_gid = in_reporttemplatefilter_gid;
		set v_msg = 'Record Updated Successfully.. !';
	elseif(in_action = 'DELETE') then 
		update recon_mst_treporttemplatefilter set
			update_date = sysdate(),
			update_by = in_action_by,
			delete_flag = 'Y'  
		where reporttemplatefilter_gid = in_reporttemplatefilter_gid
		and delete_flag = 'N';
									
		set v_reporttemplatefilter_gid = in_reporttemplatefilter_gid;
		set v_msg = 'Record deleted successfully.. !';
	end if;
    
	commit;
	
	set out_result = 1;
	set out_msg = v_msg;
END $$

DELIMITER ;