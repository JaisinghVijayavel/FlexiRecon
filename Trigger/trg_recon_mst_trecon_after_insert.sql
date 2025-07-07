DELIMITER $$

drop trigger if exists trg_recon_mst_trecon_after_insert $$
create trigger trg_recon_mst_trecon_after_insert after insert on recon_mst_trecon
for each row
begin
  call pr_set_recontable(New.recon_code,@msg12,@result12);
end $$

DELIMITER ;