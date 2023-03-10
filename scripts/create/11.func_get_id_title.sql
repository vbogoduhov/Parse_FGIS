--Функция, возвращающая идентификатор id_title из таблицы
--tbtitle по значению
create or replace function get_id_title(si_title text) returns int as $$
begin 
	return (select tbtitle.id_title from tbtitle where tbtitle.title = si_title);
end;
$$ language plpgsql;