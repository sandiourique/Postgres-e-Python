-- Database: atividadetreze

-- DROP DATABASE atividadetreze;

CREATE DATABASE atividadetreze
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Portuguese_Brazil.1252'
    LC_CTYPE = 'Portuguese_Brazil.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
	


drop table vendas;
drop table log_vendas;
drop table estatisticas;
drop table log_est;


create table vendas(
	id serial not null primary key,
    data DATE,
    valor numeric
);

create table estatisticas(
	id serial,
    media numeric,
    mediana numeric,
    moda numeric,
    desvio_padrao numeric,
    maior numeric,
    menor numeric,
    data_inicio timestamp,
    data_fim timestamp
);

create table log_est(
    usuario_log varchar (60) not null,
    data_criacao timestamp not null,
    operacao_realizada text not null
);


create table log_vendas(
    usuario_log varchar (60) not null,
    data_criacao timestamp not null,
    operacao_realizada text not null
);

create or replace function tgr_function_log_vendas()
returns trigger as $$
	begin
		if (TG_OP = 'INSERT') then 
			insert into log_vendas(usuario_log, data_criacao, operacao_realizada)
			values (current_user, current_timestamp, ' Operação de inserção: '|| new.* || ' .');
			return new;
		ELSIF (TG_OP = 'UPDATE') then
			insert into log_vendas(usuario_log, data_criacao, operacao_realizada)
			values (current_user, current_timestamp, ' Operação de update.' 
					|| old.* || ' para ' || new.* || ' .');
			return new;
		ELSIF (TG_OP = 'DELETE') then
			insert into log_vendas(usuario_log, data_criacao, operacao_realizada)
			values (current_user, current_timestamp, ' Operação de delete.' || old.* || ' .');
				return old;
		end if;
		return null;
	end;
$$
language 'plpgsql';

create trigger trg_status_vendas
	after insert or update or delete on vendas
		for each row 
			execute procedure tgr_function_log_vendas();
			
			
			
create or replace function tgr_function_log_estaistica()
returns trigger as $$
	begin
		if (TG_OP = 'INSERT') then 
			insert into log_est(usuario_log, data_criacao, operacao_realizada)
			values (current_user, current_timestamp, ' Operação de inserção: '|| new.* || ' .');
			return new;
		ELSIF (TG_OP = 'UPDATE') then
			insert into log_est(usuario_log, data_criacao, operacao_realizada)
			values (current_user, current_timestamp, ' Operação de update.' 
					|| old.* || ' para ' || new.* || ' .');
			return new;
		ELSIF (TG_OP = 'DELETE') then
			insert into log_est(usuario_log, data_criacao, operacao_realizada)
			values (current_user, current_timestamp, ' Operação de delete.' || old.* || ' .');
				return old;
		end if;
		return null;
	end;
$$
language 'plpgsql';

create trigger trg_status_estatistica
	after insert or update or delete on estatisticas
		for each row 
			execute procedure tgr_function_log_estaistica();



select * from estatisticas;
select * from vendas;
select * from log_vendas;
select * from log_est;



