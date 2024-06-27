DELIMITER $$

drop trigger if exists trg_con_mst_tdataset_after_insert $$
create trigger trg_con_mst_tdataset_after_insert after insert on recon_mst_tdataset
for each row
begin
	if New.active_status = 'Y' then
    set @sno := (select max(display_order) from recon_mst_treport where active_status = 'Y' and delete_flag = 'N');

    insert into recon_mst_treport
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
end $$

DELIMITER ;