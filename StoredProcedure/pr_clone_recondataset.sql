DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_clone_recondataset` $$
CREATE PROCEDURE `pr_clone_recondataset`(
  in in_recon_code varchar(32),
  in in_dataset_code varchar(32),
  in in_parent_dataset_code varchar(32),
  in in_clone_recon_code varchar(32),
  in in_clone_dataset_code varchar(32),
  in in_user_code varchar(32),
  out out_msg text,
  out out_result int
)
me:begin
  declare v_sql text default '';
  declare v_txt text default '';

  set in_parent_dataset_code = ifnull(in_parent_dataset_code,'');

  drop temporary table if exists recon_tmp_tdatasetfield;

  create temporary table recon_tmp_tdatasetfield
  (
    dataset_code varchar(32) not null,
    dataset_field_name varchar(128) not null,
    PRIMARY KEY (dataset_code,dataset_field_name)
  );

  -- check new dataset mapping status
  if exists(select recon_code from recon_mst_trecondataset
    where recon_code = in_recon_code
    and dataset_code = in_dataset_code
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Recon dataset was already mapped !';

    leave me;
  end if;

  -- check new dataset mapping status
  if in_parent_dataset_code <> '' then
    if not exists(select recon_code from recon_mst_trecondataset
      where recon_code = in_recon_code
      and dataset_code = in_parent_dataset_code
      and delete_flag = 'N') then

      set out_result = 0;
      set out_msg = 'Recon parent dataset was not mapped !';

      leave me;
    end if;
  end if;

  -- check clone dataset status
  if not exists(select recon_code from recon_mst_trecondataset
    where recon_code = in_clone_recon_code
    and dataset_code = in_clone_dataset_code
    and delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'This clone dataset was invalid !';

    leave me;
  end if;

  -- recon field validation with clone recon
  if exists(select * from recon_mst_treconfield as a
    left join recon_mst_treconfield as b on a.recon_field_name = b.recon_field_name
      and b.recon_code = in_clone_recon_code
      and b.active_status = 'Y'
      and b.delete_flag = 'N'
    where a.recon_code = in_recon_code
    and a.active_status = 'Y'
    and b.recon_field_name is null
    and a.delete_flag = 'N') or
    exists(select * from recon_mst_treconfield as a
    right join recon_mst_treconfield as b on a.recon_field_name = b.recon_field_name
      and b.recon_code = in_clone_recon_code
      and b.active_status = 'Y'
      and b.delete_flag = 'N'
    where a.recon_code = in_recon_code
    and a.active_status = 'Y'
    and a.recon_field_name is null
    and a.delete_flag = 'N') then

    set out_result = 0;
    set out_msg = 'Recon field not matched with clone recon !';

    leave me;
  end if;

  -- temporary dataset field
  insert into recon_tmp_tdatasetfield
  (
    dataset_code,
    dataset_field_name
  )
  select
    a.dataset_code,
    a.dataset_table_field
  from recon_mst_tdatasetfield as a
  inner join recon_mst_tdatasetfield as b on a.dataset_table_field = b.dataset_table_field
    and b.dataset_code = in_clone_dataset_code
    and b.active_status = 'Y'
    and b.delete_flag = 'N'
  where a.dataset_code = in_dataset_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  -- insert in recon dataset
  insert recon_mst_trecondataset
  (
    recon_code,
    dataset_code,
    dataset_type,
    parent_dataset_code,
    clone_recon_code,
    clone_dataset_code,
    active_status,
    insert_date,
    insert_by
  )
  select
    in_recon_code,
    in_dataset_code,
    dataset_type,
    in_parent_dataset_code,
    recon_code,
    dataset_code,
    'Y',
    sysdate(),
    in_user_code
  from recon_mst_trecondataset
  where recon_code = in_clone_recon_code
  and dataset_code = in_clone_dataset_code
  and active_status = 'Y'
  and delete_flag = 'N';

  -- insert in recon field mapping
  insert into recon_mst_treconfieldmapping
  (
    recon_code,
    recon_field_name,
    dataset_code,
    dataset_field_name,
    active_status,
    insert_date,
    insert_by
  )
  select
    in_recon_code,
    a.recon_field_name,
    in_dataset_code,
    a.dataset_field_name,
    a.active_status,
    sysdate(),
    in_user_code
  from recon_mst_treconfieldmapping as a
  inner join recon_tmp_tdatasetfield as b on a.dataset_field_name = b.dataset_field_name
  where a.recon_code = in_clone_recon_code
  and a.dataset_code = in_clone_dataset_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  drop temporary table if exists recon_tmp_tdatasetfield;

  set out_result = 1;
  set out_msg = 'Success';
end $$

DELIMITER ;