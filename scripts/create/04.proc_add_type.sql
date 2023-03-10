--Процедура добавления данных в tbtype
create or replace procedure add_type(type_t text, type_n text) as $$
begin 
	insert into tbtype (type_title, type_number) values (type_t, type_n);
end;
$$ language plpgsql;
