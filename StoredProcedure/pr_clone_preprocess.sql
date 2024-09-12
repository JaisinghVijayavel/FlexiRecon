DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_clone_preprocess` $$
CREATE PROCEDURE `pr_clone_preprocess`(
  in in_preprocess_name varchar(255),
  in in_clone_preprocess_code varchar(32),
  in in_user_code varchar(32),
  out out_preprocess_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_sql text default '';
  declare v_txt text default '';
  declare v_preprocess_code text default '';
  declare v_preprocess_gid int default 0;
  declare v_preprocess_order int default 0;
  declare in_recon_code text default '';
  set out_preprocess_code='';
  set in_preprocess_name = ifnull(in_preprocess_name,'');

	if in_preprocess_name = '' then
		set out_msg = 'Preprocess name is blank !';
		set out_result = 0;
		leave me;
	end if;

	set in_recon_code=(select recon_code from recon_mst_tpreprocess where preprocess_code=in_clone_preprocess_code);

	if exists(select preprocess_code from recon_mst_tpreprocess
    where preprocess_desc = in_preprocess_name
    and recon_code = in_recon_code
    and delete_flag = 'N') then
		set out_msg = 'Duplicate preprocess name !';
		set out_result = 0;
		leave me;
	end if;

	if exists(select preprocess_code from recon_mst_tpreprocess where preprocess_code = in_clone_preprocess_code and delete_flag = 'N') then
		set v_preprocess_code = fn_get_autocode('PP');
		select max(preprocess_order)+1 into v_preprocess_order from recon_mst_tpreprocess where recon_code=in_recon_code and delete_flag='N';
    -- insert in theme table
	INSERT INTO recon_mst_tpreprocess
	(
		preprocess_code,
		preprocess_desc,
		recon_code,
		get_recon_field,
		set_recon_field,
		process_method,
		process_query,
		process_function,
		lookup_dataset_code,
		lookup_return_field,
		preprocess_order,
		hold_flag,
		active_status,
		insert_date,
		insert_by)
    select
		v_preprocess_code,
		in_preprocess_name,
		recon_code,
		get_recon_field,
		set_recon_field,
		process_method,
		process_query,
		process_function,
		lookup_dataset_code,
		lookup_return_field,
		v_preprocess_order,
		hold_flag,
		'D',
		sysdate(),
		in_user_code
		from recon_mst_tpreprocess where preprocess_code = in_clone_preprocess_code and delete_flag = 'N';
    
    -- insert in preprocess condition
    INSERT INTO recon_mst_tpreprocesscondition
	(
		preprocess_code,
		condition_seqno,
		recon_field,
		extraction_criteria,
		extraction_filter,
		lookup_field,
		comparison_criteria,
		comparison_filter,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		active_status,
		insert_date,
		insert_by)
    select
		v_preprocess_code,
		condition_seqno,
		recon_field,
		extraction_criteria,
		extraction_filter,
		lookup_field,
		comparison_criteria,
		comparison_filter,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		active_status,
		sysdate(),
		in_user_code
		from recon_mst_tpreprocesscondition where preprocess_code = in_clone_preprocess_code and active_status = 'Y' and delete_flag = 'N';
    -- filter
    INSERT INTO recon_mst_tpreprocessfilter
	(
		preprocess_code,
		filter_seqno,
		filter_field,
		filter_criteria,
		filter_value_flag,
		filter_value,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		active_status,
		insert_date,
		insert_by) 
	select
		v_preprocess_code,
		filter_seqno,
		filter_field,
		filter_criteria,
		filter_value_flag,
		filter_value,
		open_parentheses_flag,
		close_parentheses_flag,
		join_condition,
		active_status, 
		sysdate(),
		in_user_code
		from recon_mst_tpreprocessfilter where preprocess_code = in_clone_preprocess_code and active_status = 'Y' and delete_flag = 'N';      
        
        INSERT INTO recon_mst_tpreprocesslookup
	(
		preprocess_code,
		lookup_seqno,
		lookup_return_field,
		set_recon_field,
		active_status,
		insert_date,
		insert_by)
	select
		v_preprocess_code,
		lookup_seqno,
		lookup_return_field,
		set_recon_field,
		active_status,
		sysdate(),
		in_user_code
	from recon_mst_tpreprocesslookup where preprocess_code = in_clone_preprocess_code and active_status = 'Y' and delete_flag = 'N';      
 
  end if;
	set out_preprocess_code = v_preprocess_code;
	set out_result = 1;
	set out_msg = 'Success';
end $$

DELIMITER ;