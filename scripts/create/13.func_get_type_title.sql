create function get_type_title(id_t int, t_title out text, t_number out text) as $$
begin
		t_title := (select type_title from tbtype where id_type = id_t);
		t_number := (select type_number from tbtype where id_type = id_t);
end;
$$ language plpgsql;
