DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_recon_mindate` $$
CREATE PROCEDURE `pr_get_recon_mindate`
(
  in in_recon_code varchar(32)
)
me:begin
  declare v_sql text default '';

  declare v_finyear_start_date date;
  declare v_min_tran_date date;

	declare v_tran_table text default '';
  declare v_concurrent_ko_flag text default '';

  set in_recon_code = ifnull(in_recon_code,'');

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' and in_recon_code <> '' then
	  set v_tran_table = concat(in_recon_code,'_tran');
  else
	  set v_tran_table = 'recon_trn_ttran';
  end if;

  if month(curdate()) > 3 then
    set v_finyear_start_date = cast(concat(cast(year(curdate()) as nchar),'-04-01') as date);
  else
    set v_finyear_start_date = cast(concat(cast(year(curdate())-1 as nchar),'-04-01') as date);
  end if;

  set v_sql = concat("select count(*) into @rec_count2 from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and delete_flag = 'N' limit 1 LOCK IN SHARE MODE");

  call pr_run_sql2(v_sql,@msg2,@result2);

  set @rec_count2 = ifnull(@rec_count2,0);

  if @rec_count2 > 0 then
    set v_sql = concat("
    select
      ifnull(min(tran_date),curdate()) into @v_min_tran_date
    from ",v_tran_table,"
    where recon_code = '",in_recon_code,"'
    and excp_value <> 0
    and delete_flag = 'N' LOCK IN SHARE MODE");

    call pr_run_sql2(v_sql,@msg2,@result2);

    set v_min_tran_date = ifnull(@v_min_tran_date,curdate());

    if datediff(v_finyear_start_date,v_min_tran_date) < 0 then
      set v_min_tran_date = v_finyear_start_date;
    end if;

    select DATE_FORMAT(v_min_tran_date,'%d/%m/%Y') as min_tran_date;
  else
    select DATE_FORMAT(curdate(),'%d/%m/%Y') as min_tran_date;
  end if;
end $$

DELIMITER ;