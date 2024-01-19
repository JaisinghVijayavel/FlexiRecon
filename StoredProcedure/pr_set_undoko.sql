DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_undoko` $$
CREATE PROCEDURE `pr_set_undoko`
(
  in in_ko_gid int,
  in in_undo_ko_reason varchar(255),
  in in_user_code varchar(16),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_sql text default '';
  declare v_txt text default '';
  declare v_ko_gid int default 0;
  declare v_ko_date date;
  declare v_ko_undo_period int default 0;

  set out_result = 0;
  set out_msg = 'initiated';

  drop temporary table if exists recon_tmp_ttranko;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;

  create temporary table recon_tmp_ttranko(
    tran_gid int(10) unsigned NOT NULL,
    ko_value decimal(15,2) not null default 0,
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


  if exists(select ko_gid from recon_trn_tko
    where ko_gid = in_ko_gid
    and delete_flag = 'N') then

    select ko_date into v_ko_date from recon_trn_tko
    where ko_gid = in_ko_gid
    and delete_flag = 'N';

    
    set v_txt = fn_get_configvalue('ko_undo_period');
    set v_ko_undo_period = cast(ifnull(v_txt,'0') as unsigned);

    
    if curdate() > adddate(v_ko_date,interval v_ko_undo_period day) then
      set out_msg = concat('Undo ko failed ! It should be done with in ',cast(v_ko_undo_period as nchar),' day(s) !)');
      leave me;
    end if;

    insert into recon_tmp_ttranko (tran_gid,ko_value)
      select tran_gid,sum(ko_value) as ko_value from recon_trn_tkodtl
      where ko_gid = in_ko_gid
      and delete_flag = 'N'
      group by tran_gid;

    insert into recon_tmp_ttrangid
      select tran_gid from recon_tmp_ttranko;

    insert into recon_tmp_ttranbrkpgid
      select distinct tranbrkp_gid from recon_trn_tkodtl
      where ko_gid = in_ko_gid and tranbrkp_gid > 0 and delete_flag = 'N';

    insert ignore into recon_tmp_ttranbrkpgid
      select tranbrkp_gid from recon_trn_ttranbrkpko
      where ko_gid = in_ko_gid and excp_value = 0 and delete_flag = 'N';

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

    update recon_tmp_ttranko as a
    inner join recon_trn_ttran as b on a.tran_gid = b.tran_gid
    and b.delete_flag = 'N'
    set
      b.excp_value = b.excp_value + a.ko_value,
      b.ko_gid = 0,
      b.ko_date = null;

    update recon_trn_tkodtl as a
    inner join recon_trn_ttranbrkp as c on a.tranbrkp_gid = c.tranbrkp_gid
      and c.delete_flag = 'N'
    set
      c.excp_value = c.excp_value + a.ko_value,
      c.ko_gid = 0,
      c.ko_date = null
    where a.ko_gid = in_ko_gid
    and a.delete_flag = 'N';

    update recon_trn_ttranbrkp
    set
      excp_value = tran_value,
      ko_gid = 0,
      ko_date = null
    where ko_gid = in_ko_gid
    and delete_flag = 'N';

    update recon_trn_tko set
      undo_ko_reason = in_undo_ko_reason,
      update_date = sysdate(),
      update_by = in_user_code,
      delete_flag = 'Y'
    where ko_gid = in_ko_gid
    and delete_flag = 'N';

    update recon_trn_tkodtl set
      delete_flag = 'Y'
    where ko_gid = in_ko_gid
    and delete_flag = 'N';

    set out_result = 1;
    set out_msg = 'Ko undo made successfully !';
  else
    set out_msg = 'Invalid ko_gid';
  end if;

  drop temporary table if exists recon_tmp_ttranko;
  drop temporary table if exists recon_tmp_ttrangid;
  drop temporary table if exists recon_tmp_ttranbrkpgid;
end $$

DELIMITER ;