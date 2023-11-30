DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_set_split` $$
CREATE PROCEDURE `pr_set_split`
(
  str nvarchar(6500),
  dilimiter varchar(15),
  tmp_name varchar(50)
)
begin
    declare end_index   int;
    declare part        nvarchar(6500);
    declare remain_len  int;

    set end_index      = INSTR(str, dilimiter);

    while(end_index   != 0) do
        set part       = SUBSTRING(str, 1, end_index - 1);

        call `pr_ins_split`(tmp_name, part);

        set remain_len = length(str) - end_index;
        set str = substring(str, end_index + 1, remain_len);

        set end_index  = INSTR(str, dilimiter);
    end while;

    if(length(str) > 0) then
        call `pr_ins_split`(tmp_name, str);
    end if;
end $$

DELIMITER ;