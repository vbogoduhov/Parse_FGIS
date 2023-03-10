--Функция, возвращающая идентификатор id_mod из таблицы
--tbmodofication по значению
create or replace function get_id_mod(modif text) returns int as $$
begin 
	return (select tbmodification.id_mod from tbmodification where tbmodification.modification = modif);
end;
$$ language plpgsql;
