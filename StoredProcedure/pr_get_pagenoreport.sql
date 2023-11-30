DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_pagenoreport` $$
CREATE PROCEDURE `pr_get_pagenoreport`(
  in in_rptsession_gid int,
  in in_page_no int,
  in in_page_size int,
  in in_tot_records int,
  out out_msg text,
  out out_result int
)
me:BEGIN
  declare v_report_code text default '';
  declare v_table_name text default '';
  declare v_condition text default '';
  declare v_start_rec_no int default 0;

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
    @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;

    set @text = concat(@text,' ',err_msg);

    SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);

    ROLLBACK;

    set out_msg = @full_error;
    set out_result = 0;

    SIGNAL SQLSTATE '99999' SET
    MYSQL_ERRNO = @errno,
    MESSAGE_TEXT = @text;
  END;

  if exists(select rptsession_gid from recon_trn_treportsession
     where rptsession_gid = in_rptsession_gid
     and delete_flag = 'N') then
    select
      b.table_name
    into
      v_table_name
    from recon_trn_treportsession as a
    inner join recon_mst_treport as b on a.report_code = b.report_code and b.delete_flag = 'N'
    where a.rptsession_gid = in_rptsession_gid
    and a.delete_flag = 'N';
  else
    set out_msg = 'Invalid session';
    set out_result = 0;

    leave me;
  end if;

  set v_table_name = ifnull(v_table_name,'');

  if v_table_name = '' then
    set out_msg = 'Invalid table name';
    set out_result = 0;

    leave me;
  end if;

  set v_start_rec_no = (in_page_no - 1)*in_page_size;

  set v_condition = concat('and rptsession_gid = ',cast(in_rptsession_gid as nchar) ,' ');
  set v_condition = concat(v_condition,' limit ',cast(v_start_rec_no as nchar),',',cast(in_page_size as nchar),' ');


  call pr_get_tablequery(v_table_name,v_condition,0,'',@msg,@result);

  set out_msg = @msg;
  set out_result = @result;
end $$

DELIMITER ;