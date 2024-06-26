﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_undokojob` $$
CREATE PROCEDURE `pr_set_undokojob`(
  in in_job_gid int,
  in in_undo_job_reason varchar(255),
  in in_ip_addr varchar(128),
  in in_user_code varchar(16),
  out out_result int,
  out out_msg text
)
me:begin
  declare v_sql text default '';
  declare v_txt text default '';
  declare v_job_type text default '';
  declare v_ko_gid int default 0;
  declare v_undo_job_gid int default 0;
  declare v_job_date date;
  declare v_job_undo_period int default 0;

  set out_result = 0;
  set out_msg = 'initiated';

  drop temporary table if exists recon_tmp_ttranko;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;

  create temporary table recon_tmp_ttranko(
    tran_gid int(10) unsigned NOT NULL,
    ko_amount decimal(15,2) not null default 0,
    PRIMARY KEY (tran_gid)
  );

  CREATE temporary TABLE recon_tmp_ttrangid(
    tran_gid int(10) unsigned NOT NULL,
    PRIMARY KEY (tran_gid)
  );

  CREATE temporary TABLE recon_tmp_ttranbrkpgid(
    tranbrkp_gid int(10) unsigned NOT NULL,
    PRIMARY KEY (tranbrkp_gid)
  );

  if exists(select job_gid from recon_trn_tjob
    where job_gid = in_job_gid
    and job_type in ('A','S','M')
    and job_status = 'C'
    and delete_flag = 'N') then
    select
      job_type,cast(start_date as date)
    into
      v_job_type, v_job_date
    from recon_trn_tjob
    where job_gid = in_job_gid
    and job_type in ('A','S','M')
    and job_status = 'C'
    and delete_flag = 'N';

    -- get undo job threshold
    set v_txt = fn_get_configvalue('job_undo_period');
    set v_job_undo_period = cast(ifnull(v_txt,'0') as unsigned);

    -- validate
    if curdate() > adddate(v_job_date,interval v_job_undo_period day) then
      set out_msg = ('Undo job failed ! It should be done with in ',cast(v_job_undo_period as nchar),' day(s) !)');
      leave me;
    end if;


    if v_job_type = 'A' or v_job_type = 'M' then
			-- update job status
      if v_job_type = 'A' then
        call pr_ins_job('U',in_job_gid,concat('Undo auto match ',ifnull(in_undo_job_reason,'')),in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);
      else
        call pr_ins_job('U',in_job_gid,concat('Undo manual match ',ifnull(in_undo_job_reason,'')),in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);
      end if;

      set v_undo_job_gid = @out_job_gid;

      insert into recon_tmp_ttranko (tran_gid,ko_amount)
        select b.tran_gid,sum(b.ko_amount) as ko_amount from recon_trn_tko as a
        inner join recon_trn_tkodtl as b on a.ko_gid = b.ko_gid and b.delete_flag = 'N'
        where a.job_gid = in_job_gid
        and a.delete_flag = 'N'
        group by b.tran_gid;

      insert into recon_tmp_ttrangid
        select tran_gid from recon_tmp_ttranko;

      insert into recon_tmp_ttranbrkpgid
        select distinct b.tranbrkp_gid from recon_trn_tko as a
        inner join recon_trn_tkodtl as b on a.ko_gid = b.ko_gid and b.tranbrkp_gid > 0 and b.delete_flag = 'N'
        where a.job_gid = in_job_gid
        and a.delete_flag = 'N';

      insert into recon_trn_ttran
        select b.* from recon_tmp_ttrangid as a
        inner join recon_trn_ttranko as b on a.tran_gid = b.tran_gid and b.delete_flag = 'N';

      insert into recon_trn_ttranbrkp
        select b.* from recon_tmp_ttranbrkpgid as a
        inner join recon_trn_ttranbrkpko as b on a.tranbrkp_gid = b.tranbrkp_gid and b.delete_flag = 'N';

      delete from recon_trn_ttranko where tran_gid in (
        select tran_gid from recon_tmp_ttrangid)
        and delete_flag = 'N';

      delete from recon_trn_ttranbrkpko where tranbrkp_gid in (
        select tranbrkp_gid from recon_tmp_ttranbrkpgid)
        and delete_flag = 'N';

      -- add ko amount in exception amount
      update recon_tmp_ttranko as a
      inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid
      and b.delete_flag = 'N'
      set
        b.excp_amount = b.excp_amount + a.ko_amount,
        b.ko_gid = 0,
        b.ko_date = null;



      update recon_trn_tko as k
      inner join recon_trn_tkodtl as a on k.ko_gid = a.ko_gid and a.delete_flag = 'N'
      inner join recon_trn_ttranbrkp as c on a.tranbrkp_gid = c.tranbrkp_gid
        and c.delete_flag = 'N'
      set
        c.excp_amount = c.excp_amount + a.ko_amount,
        c.ko_gid = 0,
        c.ko_date = null
      where k.job_gid = in_job_gid
      and k.delete_flag = 'N';

      update recon_trn_tko as k
      inner join recon_trn_tkodtl as a on k.ko_gid = a.ko_gid and a.delete_flag = 'N'
      set
        k.undo_ko_reason = in_undo_job_reason,
        k.update_date = sysdate(),
        k.update_by = in_user_code,
        k.delete_flag = 'Y',
        a.delete_flag = 'Y'
      where k.job_gid = in_job_gid
      and k.delete_flag = 'N';

    elseif v_job_type = 'S' then
      if exists(select tranbrkp_gid from recon_trn_ttranbrkpko
                         where post_job_gid = in_job_gid
                         and delete_flag = 'N') then
        set out_msg = 'Access denied ! Few line(s) seems to be knocked off ! Please refer report';
        leave me;
      end if;


      call pr_ins_job('U',in_job_gid,concat('Undo supporting file posting ',ifnull(in_undo_job_reason,'')),in_user_code,in_ip_addr,'I','Initiated...',@out_job_gid,@msg,@result);

      set v_undo_job_gid = @out_job_gid;

      update recon_trn_ttran
      set mapped_amount = 0
      where tran_gid in (select distinct tran_gid from recon_trn_ttranbrkp
                         where post_job_gid = in_job_gid
                         and delete_flag = 'N')
      and mapped_amount = amount
      and delete_flag = 'N';

      update recon_trn_ttranbrkp
      set tran_gid = 0,post_job_gid = 0
      where post_job_gid = in_job_gid
      and delete_flag = 'N';
    end if;

    call pr_upd_job(in_job_gid,'U','Undo completed...',@msg,@result);
    call pr_upd_job(v_undo_job_gid,'C','Undo completed...',@msg,@result);

    set out_result = 1;
    set out_msg = 'Job undo made successfully !';
  else
    set out_msg = 'Invalid job_gid';
  end if;

  drop temporary table if exists recon_tmp_ttranko;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;
end $$

DELIMITER ;