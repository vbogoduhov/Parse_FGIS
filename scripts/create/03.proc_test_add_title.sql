--�������� ��������� ���������� ������ � ������� tbtitle
create procedure add_title(title_si text)
as $$
begin
insert into tbtitle (title) values (title_si);
end;
$$ language plpgsql;
