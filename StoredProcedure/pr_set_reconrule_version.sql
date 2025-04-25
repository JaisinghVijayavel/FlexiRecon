DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_reconrule_version` $$
CREATE PROCEDURE `pr_set_reconrule_version`
(
  in in_recon_code text,
  in in_rule_code text,
  in in_theme_code text,
  in in_preprocess_code text,
  in in_reconrule_version text, 
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int(10)
  )
me:BEGIN
  declare v_err_flag boolean default false;
  declare v_err_msg text default '';

  declare v_txt text default '';
  declare v_rule_code text default '';
  declare v_system_match_flag varchar(32) default '';
  declare v_probable_match_flag varchar(32) default '';
  declare v_hold_flag varchar(32) default '';
  declare v_hold_flag_theme varchar(32) default '';
  declare v_txt_theme text default '';
  declare v_theme_code text default '';
  declare v_hold_flag_preprocess varchar(32) default '';
  declare v_txt_preprocess text default '';
  declare v_preprocess_code text default '';

  declare i int default 0;
  declare j int default 0;
  declare f int default 0;

  drop temporary table if exists recon_tmp_trule;
  drop temporary table if exists recon_tmp_ttheme;
  drop temporary table if exists recon_tmp_tpreprocess;

  CREATE temporary TABLE recon_tmp_trule
	(
    rule_code varchar(32) not null,
    system_match_flag varchar(32),
    probable_match_flag varchar(32),
    hold_flag varchar(32),
    PRIMARY KEY (rule_code)
  ) ENGINE = MyISAM;
  
  CREATE temporary TABLE recon_tmp_ttheme
	(
    theme_code varchar(32) not null,
    hold_flag varchar(32),
    PRIMARY KEY (theme_code)
  ) ENGINE = MyISAM;
  
  CREATE temporary TABLE recon_tmp_tpreprocess
	(
    preprocess_code varchar(32) not null,
    hold_flag varchar(32),
    PRIMARY KEY (preprocess_code)
  ) ENGINE = MyISAM;
  
  
  if not exists(select recon_code from recon_mst_trecon
    where recon_code = in_recon_code
    and period_from <= curdate()
    and (period_to >= curdate()
    or until_active_flag = 'Y')
    and active_status = 'Y'
    and delete_flag = 'N') then
    set v_err_msg = concat(v_err_msg,'Invalid recon,');
    set v_err_flag = true;
  end if;
  
  if exists(select recon_code from recon_mst_trulehistory
    where recon_code = in_recon_code
    and recon_version = in_reconrule_version
    and delete_flag = 'N') then
    set v_err_msg = concat(v_err_msg,'Recon rule version already available,');
    set v_err_flag = true;
  end if; 
  
  if exists(select theme_code from recon_mst_tthemehistory
    where recon_code = in_recon_code
    and recon_version = in_reconrule_version
    and delete_flag = 'N') then
    set v_err_msg = concat(v_err_msg,'Recon theme version already available,');
    set v_err_flag = true;
  end if;
	
	if in_rule_code != "" then
		set v_txt = in_rule_code;
		set i = instr(v_txt,'$');
		if i = 0 then set i = length(v_txt) + 1; end if;
		
		while (i > 0) do
			set v_rule_code = substr(v_txt,1,i - 1);
			set v_system_match_flag = ifnull(SPLIT(v_rule_code,'#',2),'');
			set v_hold_flag = ifnull(SPLIT(v_rule_code,'#',3),'');
			set v_probable_match_flag= ifnull(SPLIT(v_rule_code,'#',4),'');
			set v_rule_code = ifnull(SPLIT(v_rule_code,'#',1),'');
			
			if not exists(select rule_code from recon_mst_trule
				where rule_code = v_rule_code
				and active_status = 'Y'
				and delete_flag = 'N') then
				set v_err_msg = concat(v_err_msg,'Invalid rule code - ',v_rule_code,',');
				set v_err_flag = true;
			end if;

			if v_system_match_flag <> 'Y' and v_system_match_flag <> 'N' then
				set v_err_msg = concat(v_err_msg,'Invalid system match flag - ',v_rule_code,',');
				set v_err_flag = true;
			end if;

			if v_hold_flag <> 'Y' and v_hold_flag <> 'N' then
				set v_err_msg = concat(v_err_msg,'Invalid hold flag - ',v_rule_code,',');
				set v_err_flag = true;
			end if;
			
			insert into recon_tmp_trule select v_rule_code,v_system_match_flag,v_probable_match_flag,v_hold_flag;

			set v_txt = substr(v_txt,i+1);
			set i = instr(v_txt,'$');
			if i = 0 and v_txt <> '' then set i = length(v_txt) + 1; end if;
		end while;
	end if;	

	if in_theme_code != "" then
		set v_txt_theme = in_theme_code;
		set j = instr(v_txt_theme,'$');
		
		if j = 0 then set j = length(v_txt_theme) + 1; end if;
		
		while (j > 0) do
			set v_theme_code = substr(v_txt_theme,1,j - 1);
			set v_hold_flag_theme = ifnull(SPLIT(v_theme_code,'#',2),'');		
			set v_theme_code = ifnull(SPLIT(v_theme_code,'#',1),'');
			if not exists(select theme_code from recon_mst_ttheme
				where theme_code = v_theme_code
				and active_status = 'Y'
				and delete_flag = 'N') then
				set v_err_msg = concat(v_err_msg,'Invalid theme code - ',v_theme_code,',');
				set v_err_flag = true;
			end if;

			if v_hold_flag_theme <> 'Y' and v_hold_flag_theme <> 'N' then
				set v_err_msg = concat(v_err_msg,'Invalid hold flag - ',v_rule_code,',');
				set v_err_flag = true;
			end if;
				
			insert into recon_tmp_ttheme select v_theme_code,v_hold_flag_theme;

			set v_txt_theme = substr(v_txt_theme,j+1);
			set j = instr(v_txt_theme,'$');
			if j = 0 and v_txt_theme <> '' then set j = length(v_txt_theme) + 1; end if;
		end while;
  end if;
	
  if in_preprocess_code != "" then
		set v_txt_preprocess = in_preprocess_code; 
		set f = instr(v_txt_preprocess,'$');
		
		if f = 0 then set f = length(v_txt_preprocess) + 1; end if;
		
		while (f > 0) do
			set v_preprocess_code = substr(v_txt_preprocess,1,f-1);		
			set v_hold_flag_preprocess = ifnull(SPLIT(v_preprocess_code,'#',2),'');		
			set v_preprocess_code = ifnull(SPLIT(v_preprocess_code,'#',1),'');		
			
			if v_hold_flag_preprocess <> 'Y' and v_hold_flag_preprocess <> 'N' then
				set v_err_msg = concat(v_err_msg,'Invalid hold flag - ',v_preprocess_code,',');
				set v_err_flag = true;
			end if;
			
			if not exists(select preprocess_code from recon_mst_tpreprocess
				where preprocess_code = v_preprocess_code
				and active_status = 'Y'
				and delete_flag = 'N') then
				set v_err_msg = concat(v_err_msg,'Invalid Preprocess code - ',v_preprocess_code,',');
				set v_err_flag = true;
			end if;
			
			insert into recon_tmp_tpreprocess select v_preprocess_code,v_hold_flag_preprocess;
	 
			set v_txt_preprocess = substr(v_txt_preprocess,f+1);
			set f = instr(v_txt_preprocess,'$');
			if f = 0 and v_txt_preprocess <> '' then set f = length(v_txt_preprocess) + 1; end if;
		end while;
	end if;
	
  if v_err_flag = true then
    set out_msg = v_err_msg;
    set out_result = 0;
    leave me;
  end if;
	
	update recon_mst_trecon set 
		recon_rule_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where recon_code = in_recon_code 
	and delete_flag = 'N';
  
	update recon_mst_trulecondition set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N';
  
	update recon_mst_trulegrpfield set  
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)		
	and delete_flag = 'N';
  
	update recon_mst_trulerecorder set  
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and delete_flag = 'N';
   
	update recon_mst_truleselefilter set  
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and delete_flag = 'N';
  
	update recon_mst_ttheme set 
		recon_version = in_reconrule_version,
		update_by = in_user_code, 
		update_date = sysdate() 
	where recon_code = in_recon_code 
	and delete_flag = 'N';
 
	update recon_mst_tthemecondition set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N';
    
	update recon_mst_tthemefilter set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N';
 
	update recon_mst_tthemegrpfield set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N';
 
 	update recon_mst_tthemeaggfield set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N';
 
 	update recon_mst_tthemeaggcondition set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N'; 
 
	update recon_mst_tpreprocess set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where recon_code = in_recon_code 
	and delete_flag = 'N';  

	update recon_mst_tpreprocesscondition set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where preprocess_code in 
	(
		select preprocess_code from recon_mst_tpreprocess 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N'; 
 
	update recon_mst_tpreprocessfilter set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where preprocess_code in 
	(
		select preprocess_code from recon_mst_tpreprocess 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N'; 
 
 	update recon_mst_tpreprocesslookup set
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where preprocess_code in 
	(
		select preprocess_code from recon_mst_tpreprocess 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N'; 
 
	update recon_mst_tpreprocessrecorder set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where preprocess_code in 
	(
		select preprocess_code from recon_mst_tpreprocess 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N'; 

	update recon_mst_tpreprocessgrpfield set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where preprocess_code in 
	(
		select preprocess_code from recon_mst_tpreprocess 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	)  
	and delete_flag = 'N'; 
    
  update recon_mst_truleaggcondition set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and delete_flag = 'N'; 
    
  update recon_mst_truleaggfield set 
		recon_version = in_reconrule_version,
		update_by = in_user_code,
		update_date = sysdate()
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and delete_flag = 'N'; 
    	
	update recon_mst_trule as a 
	inner join recon_tmp_trule as b on a.rule_code = b.rule_code
	set 
		a.system_match_flag = b.system_match_flag,
		a.hold_flag = b.hold_flag,
		a.probable_match_flag=b.probable_match_flag,
		a.recon_version = in_reconrule_version
	where a.recon_code = in_recon_code
	and a.delete_flag = 'N';
  
	update recon_mst_ttheme as a 
	inner join recon_tmp_ttheme as b on a.theme_code = b.theme_code
	set a.hold_flag = b.hold_flag
	where a.recon_code = in_recon_code 
	and a.active_status ='Y'
	and a.delete_flag = 'N'; 
	
	update recon_mst_tpreprocess as a
	inner join recon_tmp_tpreprocess as b on a.preprocess_code = b.preprocess_code
	set a.hold_flag = b.hold_flag
	where a.recon_code = in_recon_code 
	and a.active_status ='Y'
	and a.delete_flag = 'N'; 

	replace into recon_mst_trulehistory
	(
    rule_code, rule_name, recon_code, rule_apply_on, rule_order,rule_remark,source_dataset_code, source_acc_mode, comparison_dataset_code, comparison_acc_mode,
    group_flag, group_method_flag, manytomany_match_flag,reversal_flag, system_match_flag, manual_match_flag, hold_flag,threshold_flag,rule_automatch_partial,threshold_code,threshold_plus_value,threshold_minus_value,
    period_from, period_to, until_active_flag,probable_match_flag,recon_version,active_status,insert_date,insert_by
	)
	select rule_code, rule_name, recon_code, rule_apply_on, rule_order,
		rule_remark,source_dataset_code, source_acc_mode, comparison_dataset_code, comparison_acc_mode,
    group_flag, group_method_flag, manytomany_match_flag,reversal_flag, system_match_flag, manual_match_flag, hold_flag,threshold_flag,rule_automatch_partial,threshold_code,threshold_plus_value,threshold_minus_value,
    period_from, period_to, until_active_flag,probable_match_flag,recon_version,active_status,sysdate(),in_user_code
	from recon_mst_trule
	where recon_code = in_recon_code 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';
  
	replace INTO recon_mst_truleconditionhistory
	(
	rule_code,rulecondition_seqno,source_field,extraction_criteria,extraction_filter,comparison_field,
	comparison_criteria,comparison_filter,open_parentheses_flag,close_parentheses_flag,join_condition,
	recon_version,active_status,insert_date,insert_by
	)
	SELECT
		rule_code,rulecondition_seqno,source_field,extraction_criteria,extraction_filter,
		comparison_field,comparison_criteria,comparison_filter,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_trulecondition
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';
  
	replace INTO recon_mst_trulegrpfieldhistory
	(
		rule_code,rulegrpfield_seqno,grp_field,recon_version,active_status,insert_date,insert_by
	)
	select 
		rule_code,rulegrpfield_seqno,grp_field,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_trulegrpfield
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';
  
	replace INTO recon_mst_trulerecorderhistory
	(
		rule_code,recorder_applied_on,recorder_seqno,recorder_field,recon_version,
		active_status,insert_date,insert_by
	)
	select 
		rule_code,recorder_applied_on,recorder_seqno,recorder_field,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_trulerecorder
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version	
	and delete_flag = 'N';
  
	replace INTO recon_mst_truleselefilterhistory
	(
		rule_code,ruleselefilter_seqno,filter_applied_on,filter_field,filter_criteria,add_filter,
		ident_criteria,ident_value_flag,ident_value,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,insert_date,insert_by
	)
	select 
		rule_code,ruleselefilter_seqno,filter_applied_on,filter_field,filter_criteria,add_filter,
		ident_criteria,ident_value_flag,ident_value,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_truleselefilter
	where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';
	
	replace into recon_mst_tthemehistory
	(
    theme_code,theme_name,recon_code,theme_desc,theme_type_code,theme_query,shortexcess_code,source_dataset_code,source_dataset_type,source_acc_mode,
		comparison_dataset_code,comparison_dataset_type,comparison_acc_mode,
    group_flag,hold_flag,theme_order,recon_version,active_status,inactive_reason,clone_theme_code
	)
	select
		theme_code,theme_name,recon_code,theme_desc,theme_type_code,theme_query,shortexcess_code,
		source_dataset_code,source_dataset_type,source_acc_mode,
		comparison_dataset_code,comparison_dataset_type,comparison_acc_mode,
		group_flag,hold_flag,theme_order,recon_version,active_status,inactive_reason,clone_theme_code
	from recon_mst_ttheme
	where recon_code = in_recon_code 
	and delete_flag = 'N';
	
	replace into recon_mst_tthemeconditionhistory
	(
		theme_code,themecondition_seqno,source_field,extraction_criteria,
		comparison_field,comparison_criteria,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,insert_date,insert_by
	)
	select 
		theme_code,themecondition_seqno,source_field,extraction_criteria,
		comparison_field,comparison_criteria,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_tthemecondition
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';

	replace into recon_mst_tthemefilterhistory
	(
		theme_code,themefilter_seqno,filter_applied_on,
		filter_field,filter_criteria,filter_value_flag,filter_value,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,insert_date,insert_by
	)
	select
		theme_code,themefilter_seqno,filter_applied_on,
		filter_field,filter_criteria,filter_value_flag,filter_value,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_tthemefilter
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) and recon_version = in_reconrule_version 
	and delete_flag = 'N';

	replace into recon_mst_tthemegrpfieldhistory
	(
		theme_code,grpfield_seqno,grpfield_applied_on,grp_field,active_status,
		recon_version,insert_date,insert_by
	)
	select 
		theme_code,grpfield_seqno,grpfield_applied_on,grp_field,active_status,
		recon_version,sysdate(),in_user_code
	from recon_mst_tthemegrpfield
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';

	replace into recon_mst_tthemeaggfieldhistory
	(
		theme_code,themeaggfield_seqno,themeaggfield_applied_on,themeaggfield_name,
		recon_field,themeagg_function,themeagg_field,themeagg_field_sno,themeagg_field_type,
		recon_version,active_status,insert_date,insert_by
	)
	select 
		theme_code,themeaggfield_seqno,themeaggfield_applied_on,themeaggfield_name,
		recon_field,themeagg_function,themeagg_field,themeagg_field_sno,themeagg_field_type,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_tthemeaggfield
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) and recon_version = in_reconrule_version 
	and delete_flag = 'N';

	replace into recon_mst_tthemeaggconditionhistory
	(
		theme_code,themeaggcondition_seqno,themeagg_applied_on,
		themeagg_field,themeagg_criteria,themeagg_value_flag,themeagg_value,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,insert_date,insert_by
	)
	select 
		theme_code,themeaggcondition_seqno,themeagg_applied_on,
		themeagg_field,themeagg_criteria,themeagg_value_flag,themeagg_value,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_tthemeaggcondition
	where theme_code in 
	(
		select theme_code from recon_mst_ttheme 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';

	replace into recon_mst_tpreprocesshistory
	(
    preprocess_code,preprocess_desc,recon_code,
		get_recon_field,set_recon_field,process_method,process_query,
    process_expression,process_function,lookup_dataset_code,
		lookup_multi_return_flag,lookup_return_field,
    lookup_group_flag,postprocess_flag,preprocess_order,hold_flag,
		recon_version,active_status,insert_date,insert_by
	)
	select
		preprocess_code,preprocess_desc,recon_code,
		get_recon_field,set_recon_field,process_method,process_query,
    process_expression,process_function,lookup_dataset_code,
		lookup_multi_return_flag,lookup_return_field,
    lookup_group_flag,postprocess_flag,preprocess_order,hold_flag,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_tpreprocess
	where recon_code = in_recon_code
	and recon_version = in_reconrule_version
	and delete_flag = 'N';

	replace into recon_mst_tpreprocessconditionhistory
	(
		preprocess_code,condition_seqno,
		recon_field,extraction_criteria,extraction_filter,lookup_field,
		comparison_criteria,comparison_filter,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,insert_date,insert_by
	)
	select
		preprocess_code,condition_seqno,
		recon_field,extraction_criteria,extraction_filter,lookup_field,
		comparison_criteria,comparison_filter,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_tpreprocesscondition
	where preprocess_code in 
	(
		select preprocess_code from recon_mst_tpreprocess 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';
  
	replace into recon_mst_tpreprocessfilterhistory
	(
		preprocess_code,filter_seqno,
		filter_field,filter_applied_on,filter_criteria,filter_value_flag,filter_value,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,insert_date,insert_by
	)
	select
		preprocess_code,filter_seqno,
		filter_field,filter_applied_on,filter_criteria,filter_value_flag,filter_value,
		open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,sysdate(),in_user_code
	from recon_mst_tpreprocessfilter
	where preprocess_code in 
	(
		select preprocess_code from recon_mst_tpreprocess 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';
    
  replace into recon_mst_tpreprocesslookuphistory
  (
		preprocess_code,lookup_seqno,lookup_return_field,set_recon_field,
    reverse_update_flag,
		recon_version,active_status,insert_date,insert_by
	)
  SELECT
    preprocess_code,lookup_seqno,lookup_return_field,set_recon_field,
    reverse_update_flag,
		recon_version,active_status,sysdate(),in_user_code
  from recon_mst_tpreprocesslookup
	where preprocess_code in
	(
		select preprocess_code from recon_mst_tpreprocess
		where recon_code = in_recon_code
		and delete_flag = 'N'
	)
	and recon_version = in_reconrule_version
	and delete_flag = 'N';

	replace into recon_mst_tpreprocessrecorderhistory 
  (
		preprocess_code,recorder_seqno,recorder_field,
		recon_version,active_status,insert_date,insert_by
	)
  SELECT 
    preprocess_code,recorder_seqno,recorder_field,
		recon_version,active_status,sysdate(),in_user_code
  from recon_mst_tpreprocessrecorder
	where preprocess_code in 
	(
		select preprocess_code from recon_mst_tpreprocess 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';
    
  replace into recon_mst_tpreprocessgrpfieldhistory 
  (
		preprocess_code,grpfield_seqno,grp_field,
		recon_version,active_status,insert_date,insert_by
	)
  SELECT 
    preprocess_code,grpfield_seqno,grp_field,
		recon_version,active_status,sysdate(),in_user_code
  from recon_mst_tpreprocessgrpfield
	where preprocess_code in 
	(
		select preprocess_code from recon_mst_tpreprocess 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version 
	and delete_flag = 'N';
   
  replace into recon_mst_truleaggconditionhistory
	(
		rule_code,ruleaggcondition_seqno,ruleagg_applied_on,
		ruleagg_field,ruleagg_criteria,ruleagg_value_flag,ruleagg_value,
    open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,insert_date,insert_by
	)
  select 
    rule_code,ruleaggcondition_seqno,ruleagg_applied_on,
		ruleagg_field,ruleagg_criteria,ruleagg_value_flag,ruleagg_value,
    open_parentheses_flag,close_parentheses_flag,join_condition,
		recon_version,active_status,sysdate(),in_user_code
  from recon_mst_truleaggcondition
  where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version	
	and delete_flag = 'N';
    
  replace INTO recon_mst_truleaggfieldhistory
	(
    rule_code,ruleaggfield_seqno,ruleaggfield_applied_on,ruleaggfield_desc,
		recon_field,ruleagg_function,ruleagg_field,ruleagg_field_sno,ruleagg_field_type,
		recon_version,active_status,insert_date,insert_by
	)
  select 
    rule_code,ruleaggfield_seqno,ruleaggfield_applied_on,ruleaggfield_desc,
		recon_field,ruleagg_function,ruleagg_field,ruleagg_field_sno,ruleagg_field_type,
		recon_version,active_status,insert_date,insert_by
	from recon_mst_truleaggfield
  where rule_code in 
	(
		select rule_code from recon_mst_trule 
		where recon_code = in_recon_code 
		and delete_flag = 'N'
	) 
	and recon_version = in_reconrule_version	
	and delete_flag = 'N';
    
	drop temporary table if exists recon_tmp_trule;
	drop temporary table if exists recon_tmp_ttheme;
	drop temporary table if exists recon_tmp_tpreprocess;
    
	set out_msg = 'Recon rule version updated successfully !';
	set out_result = 1;
END $$

DELIMITER ;