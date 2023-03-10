--Процедура добавления данных в tbmodification
create or replace procedure add_modification(modif text) as $$
begin
	insert into tbmodification (modification) values (modif);
end;
$$ language plpgsql;
