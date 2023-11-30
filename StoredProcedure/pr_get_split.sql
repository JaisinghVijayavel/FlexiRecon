DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_split` $$
CREATE PROCEDURE `pr_get_split`(in_text nvarchar(255), in_split_char nvarchar(8), in_show_flag boolean)
BEGIN

    drop temporary table if exists tb_search;
    create temporary table if not exists tb_search
        (
            item nvarchar(6500)
        );

    call pr_set_split(in_text,in_split_char, 'tb_search');

    if in_show_flag = true then
      select * from tb_search where length(trim(item)) > 0;
    end if;
end $$

DELIMITER ;