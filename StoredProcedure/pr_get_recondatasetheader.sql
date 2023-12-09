DELIMITER $$
DROP PROCEDURE IF EXISTS `pr_get_recondatasetheader` $$

CREATE procedure `pr_get_recondatasetheader`
(
  in in_recon_code text,
  in in_dataset_code text,

  out out_dataset_field_all text,
  out out_recon_field_all text
)
begin
  /*
    Created By : Vijayavel
    Created Date : 07-10-2023

    Updated By : Vijayavel
    Updated Date :

    Version : 1
  */

  declare v_recon_field text default '';
  declare v_recon_field_type text default '';
  declare v_recon_field_org_type text default '';
  declare v_recon_field_length text default '';
  declare v_dataset_field text default '';
  declare v_dataset_field_type text default '';
  declare v_dataset_field_org_type text default '';

  declare v_recon_field_all text default '';
  declare v_dataset_field_all text default '';
  declare v_field text default '';

  declare v_sql text default '';
  declare i int default 0;

  drop temporary table if exists recon_tmp_treconfield;

  CREATE temporary TABLE recon_tmp_treconfield
  (
    recon_field varchar(255) NOT NULL,
    recon_field_type varchar(32),
    recon_field_org_type varchar(32),
    recon_field_length varchar(32),
    dataset_field varchar(255) not null,
    dataset_field_type varchar(32),
    dataset_field_org_type varchar(32),
    PRIMARY KEY (recon_field)
  );

  insert into recon_tmp_treconfield
  (
    recon_field,recon_field_type,recon_field_org_type,recon_field_length,
    dataset_field,dataset_field_type,dataset_field_org_type
  )
  select
    b.recon_field_name,
    c.recon_field_type,
    d.field_org_type,
    c.recon_field_length,
    b.dataset_field_name,
    a.field_type as dataset_field_type,
    e.field_org_type
  from recon_mst_tdatasetfield as a
  inner join recon_mst_treconfieldmapping as b on a.dataset_code = b.dataset_code
    and b.dataset_field_name = a.dataset_table_field
    and b.active_status = 'Y'
    and b.delete_flag = 'N'
  inner join recon_mst_treconfield as c on b.recon_field_name = c.recon_field_name
    and b.recon_code = c.recon_code
    and c.active_status = 'Y'
    and c.delete_flag = 'N'
  inner join recon_mst_tfieldstru as d on b.recon_field_name = d.field_name
    and d.delete_flag = 'N'
  inner join recon_mst_tfieldstru as e on a.dataset_table_field = e.field_name
    and e.delete_flag = 'N'
  where a.dataset_code = in_dataset_code
  and b.recon_code = in_recon_code
  and a.active_status = 'Y'
  and a.delete_flag = 'N';

  field_block:begin
    declare field_done int default 0;
    declare field_cursor cursor for
	    select
        recon_field,
        recon_field_type,
        recon_field_org_type,
        recon_field_length,
        dataset_field,
        dataset_field_type,
        dataset_field_org_type
      from recon_tmp_treconfield;

    declare continue handler for not found set field_done=1;

    open field_cursor;

    field_loop: loop
      fetch field_cursor into v_recon_field,v_recon_field_type,v_recon_field_org_type,v_recon_field_length,
                              v_dataset_field,v_dataset_field_type,v_dataset_field_org_type;

      if field_done = 1 then leave field_loop; end if;

      -- dataset field
      set v_field = v_dataset_field;

      if v_recon_field_org_type = v_dataset_field_org_type then
        set v_field = v_dataset_field;
      else
        if v_recon_field_type = v_dataset_field_type then
          set v_field = v_dataset_field;
        else
          if v_recon_field_type = 'TEXT' then
            set v_field = concat('cast(',v_dataset_field,' as nchar)');

            if v_recon_field_length <> '' then
              set v_field = concat('substr(',v_field,',1,',v_recon_field_length,')');
            end if;
          elseif v_recon_field_type = 'NUMERIC' then
            if v_dataset_field_type = 'TEXT' then
              if v_recon_field_length <> '' then
                set v_field = concat('cast(',v_field,' as decimal(',v_recon_field_length,'))');
              else
                set v_field = concat('cast(',v_field,' as decimal)');
              end if;
            else
              set v_field = '0';
            end if;
          elseif v_recon_field_type = 'INTEGER' then
            if v_dataset_field_type = 'TEXT' then
              set v_field = concat('cast(',v_field,' as integer)');

              if v_recon_field_length <> '' then
                set v_field = concat('cast(',v_field,' as decimal(',v_recon_field_length,'))');
              else
                set v_field = concat('cast(',v_field,' as decimal)');
              end if;
            else
              set v_field = 'null';
            end if;
          elseif v_recon_field_type = 'DATE' then
            if v_dataset_field_type = 'TEXT' then
              set v_field = concat('cast(',v_field,' as date)');
            else
              set v_field = 'null';
            end if;
          elseif v_recon_field_type = 'DATETIME' then
            if v_dataset_field_type = 'TEXT' then
              set v_field = concat('cast(',v_field,' as datetime)');
            else
              set v_field = 'null';
            end if;
          end if;
        end if;
      end if;

      if v_dataset_field_all = '' then
        set v_dataset_field_all = v_field;
      else
        set v_dataset_field_all = concat(v_dataset_field_all,',',v_field);
      end if;

      if v_recon_field_all = '' then
        set v_recon_field_all = v_recon_field;
      else
        set v_recon_field_all = concat(v_recon_field_all,',',v_recon_field);
      end if;
    end loop field_loop;

    close field_cursor;
  end field_block;

  drop temporary table if exists recon_tmp_treconfield;

  set out_dataset_field_all = v_dataset_field_all;
  set out_recon_field_all = v_recon_field_all;
end $$

DELIMITER ;