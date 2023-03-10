create function get_id_type_full(t_name text, t_number text) returns int as $$
begin
		return (select tbtype.id_type from tbtype where tbtype.type_title = t_name and tbtype.type_number = t_number);
end;
$$ language plpgsql;