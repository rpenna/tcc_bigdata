create table localidade(
	id_cidade smallint,
    no_cidade varchar(50),
    no_uf varchar(2),
    primary key(id_cidade));

create table beneficiario(
	nu_nis bigint not null,
    no_beneficiario varchar(100) not null,
    id_cidade smallint not null,
	primary key(nu_nis),
    foreign key(id_cidade) 
		references localidade(id_cidade)
	);

create table pagamento(
	nu_nis bigint not null,
    dt_referencia varchar(6),
    dt_competencia varchar(6),
    vl_beneficio float not null,
    primary key(nu_nis, dt_referencia, dt_competencia),
    foreign key (nu_nis)
		references beneficiario(nu_nis)
	);

create index localidade_index_id_cidade on localidade(id_cidade);
create index beneficiario_index_nu_nis on beneficiario(nu_nis);
create index pagamento_index_nu_nis on pagamento(nu_nis);