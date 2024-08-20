DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_clearrecon` $$
CREATE PROCEDURE `pr_set_clearrecon`
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
  delete from recon_trn_ttran
  where recon_code = in_recon_code
  and delete_flag = 'N';

  -- tranko
  delete from recon_trn_ttranko
  where recon_code = in_recon_code
  and delete_flag = 'N';

  -- tranbrkp
  delete from recon_trn_ttranbrkp
  where recon_code = in_recon_code
  and delete_flag = 'N';

  -- tranbrkpko
  delete from recon_trn_ttranbrkpko
  where recon_code = in_recon_code
  and delete_flag = 'N';
end $$

DELIMITER ;