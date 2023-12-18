DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_ins_file` $$
CREATE PROCEDURE `pr_ins_file`(
  in in_file_name varchar(128),
  in in_file_type char(1),
  in in_filetemplate_gid int,
  in in_csv_columns int,
  in in_acno varchar (255),
  in in_recon_gid int,
  in in_tranbrkptype_gid int,
  in in_action_by varchar(10),
  out out_file_gid int,
  out out_msg text,
  out out_result int(10)
)
me:BEGIN

  declare err_msg text default '';
  declare err_flag varchar(10) default false;

  

  set in_acno = ifnull(in_acno,'');
  
  if in_file_Name = '' then
    set err_msg := concat(err_msg,'Blank file Name,');
    set err_flag := true;
  end if;

  if not exists(select file_type from recon_mst_tfiletype
    where file_type = in_file_type
    and delete_flag = 'N') then
    set err_msg := concat(err_msg,'Invalid file type,');
    set err_flag := true;
  end if;


  if exists(select file_gid from recon_trn_tfile
    where file_name = in_file_name
    and file_type = in_file_type
    and delete_flag = 'N') then
    set err_msg  := concat(err_msg,'File already exists');
    set err_flag := true;
  end if;

  if in_file_type = 'S' or in_file_type = 'M' then
    if not exists(select recon_gid from recon_mst_trecon
      where recon_gid = in_recon_gid
      and delete_flag = 'N') then
      set err_msg := concat(err_msg,'Invalid recon,');
      set err_flag := true;
    end if;

    if in_file_type = 'S' or in_file_type = 'P' then
      if not exists(select tranbrkptype_gid from recon_mst_ttranbrkptype
        where tranbrkptype_gid = in_tranbrkptype_gid
        and delete_flag = 'N') then
        set err_msg := concat(err_msg,'Invalid supporting file type,');
        set err_flag := true;
      end if;
    end if;
  end if;

  select
    template_type,
    ifnull(csv_seperator,"") as csv_seperator,
    header_flag,
    acc_no_flag,
    tran_amount_type,
    bal_amount_flag,
    bal_amount_type,
    tran_date_format
  into
    @template_type1,
    @csv_separator1,
    @header_flag1,
    @acc_no_flag1,
    @tran_amount_type1,
    @bal_amount_flag1,
    @bal_amount_type1,
    @tran_date_format1
  from recon_mst_tfiletemplate
  where filetemplate_gid = in_filetemplate_gid
  and delete_flag = 'N'
  limit 0,1;

  if in_file_type = 'U' or in_file_type = 'Q' then
    set @template_type1 = '';
    set @csv_separator1 = '';
    set @header_flag1 = 'N';
    set @acc_no_flag1 = 'N';
    set @tran_amount_type1 = '';
    set @bal_amount_flag1 = '';
    set @bal_amount_type1 = '';
    set @tran_date_format1 = '';
  end if;

  if @acc_no_flag1 = 'N' and in_acno = ''
    and (in_file_type = 'T' or in_file_type = 'B' or in_file_type = 'S') then
    set err_msg := concat(err_msg,'A/C No cannot be empty,');
    set err_flag := true;
  end if;

  if err_flag = true then
    set out_result = 0;
    set out_msg = err_msg;
    set out_file_gid=0;
    leave me;
  end if;

  START TRANSACTION;

  INSERT INTO recon_trn_tfile
  (
    filetemplate_gid,
    file_name,
    file_type,
    import_date,
    template_type,
    csv_columns,
    csv_separator,
    header_flag,
    acc_no_flag,
    acc_no,
    tran_amount_type,
    bal_amount_flag,
    bal_amount_type,
    tran_date_format,
    recon_gid,
    tranbrkptype_gid,
    insert_date,
    insert_by
  )
  VALUES
  (
    in_filetemplate_gid,
    in_file_name,
    in_file_type,
    curdate(),
    @template_type1,
    in_csv_columns,
    @csv_separator1,
    @header_flag1,
    @acc_no_flag1,
    in_acno,
    @tran_amount_type1,
    @bal_amount_flag1,
    @bal_amount_type1,
    @tran_date_format1,
    in_recon_gid,
    in_tranbrkptype_gid,
    sysdate(),
    in_action_by
  );

  COMMIT;

  select max(file_gid) into out_file_gid from recon_trn_tfile;

  set out_result = 1;
  set out_msg = 'Record saved successfully !';
 END $$

DELIMITER ;