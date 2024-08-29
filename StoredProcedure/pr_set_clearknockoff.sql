﻿DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_clearknockoff` $$
CREATE PROCEDURE `pr_set_clearknockoff`
(
  in in_recon_code varchar(32)
)
begin
  declare v_sql text default '';

  -- kodtl
  delete from recon_trn_tkodtl
  where ko_gid in
  (
    select ko_gid from recon_trn_tko
    where recon_code = in_recon_code
    and delete_flag = 'N'
  )
  and delete_flag = 'N';

  -- koroundoff
  delete from recon_trn_tkoroundoff
  where ko_gid in
  (
    select ko_gid from recon_trn_tko
    where recon_code = in_recon_code
    and delete_flag = 'N'
  )
  and delete_flag = 'N';

  -- ko
  delete from recon_trn_tko
  where recon_code = in_recon_code
  and delete_flag = 'N';

  -- tran
  insert into recon_trn_ttran
    select * from recon_trn_ttranko
    where recon_code = in_recon_code
    and delete_flag = 'N';

  update recon_trn_ttran set
    excp_value = tran_value,
    roundoff_value = 0,
    mapped_value = 0,
    theme_code = '',
    ko_gid = 0,
    ko_date = null
  where recon_code = in_recon_code
  and delete_flag = 'N';

  delete from recon_trn_ttranko
  where recon_code = in_recon_code
  and delete_flag = 'N';

  -- tranbrkp
  insert into recon_trn_ttranbrkp
    select * from recon_trn_ttranbrkpko
    where recon_code = in_recon_code
    and delete_flag = 'N';

  update recon_trn_ttranbrkp set
    excp_value = tran_value,
    roundoff_value = 0,
    mapped_value = 0,
    theme_code = '',
    tran_gid = 0,
    posted_job_gid = 0,
    posted_rule_code = '',
    ko_gid = 0,
    ko_date = null
  where recon_code = in_recon_code
  and delete_flag = 'N';

  delete from recon_trn_ttranbrkpko
  where recon_code = in_recon_code
  and delete_flag = 'N';
end $$

DELIMITER ;