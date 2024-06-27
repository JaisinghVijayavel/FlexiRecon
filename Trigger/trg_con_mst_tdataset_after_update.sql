DELIMITER $$

drop trigger if exists trg_con_mst_tdataset_after_update $$
create trigger trg_con_mst_tdataset_after_update after update on recon_mst_tdataset
for each row
begin
  if New.active_status = 'Y' then
    insert ignore into recon_mst_treport
    (
      report_code,
      report_desc,
      report_exec_type,
      active_status,
      display_order,
      insert_date,
      insert_by
    )
    select
      New.dataset_code,
      New.dataset_name,
      'D',
      'Y',
      @sno,
      sysdate(),
      New.insert_by;
  end if;

	if New.active_status <> Old.active_status
    or New.delete_flag <> Old.delete_flag then
    update recon_mst_treport set
      active_status = New.active_status,
      update_date = sysdate(),
      update_by = New.update_by,
      delete_flag = New.delete_flag
    where report_code = New.dataset_code;
  end if;
end $$

DELIMITER ;