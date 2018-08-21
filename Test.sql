--������� � �������� ������
docker run -itd --name clickhouse-server --ulimit nofile=262144:262144 -p8123:8123/tcp -p9000:9000/tcp -p9009:9009/tcp yandex/clickhouse-server

--��������� ����� ��������� ������
docker start some-clickhouse-server

--����������� � �������� ������
docker run -itd --rm --name clickhouse-client -v d:\projects:/project --link some-clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server

--������������ � ��������
docker exec -it clickhouse-client bash

--������� �������
drop table default.chains;

create table default.chains (
intChainID String,
varGln String,
intDocsCount String,
intArchive String,
varHash String,
intImportant String,
intVisualStatus String,
intLastInDocID String,
intLastOutDocID String,
intLastCommentID String
) engine = Log;

drop table default.chains_docs;

create table default.chains_docs (
intChainID String,
intDocID String
) engine = Log;

--����������� �����
cat /project/clickhouse_docs_export.csv | clickhouse-client --host clickhouse-server --query="INSERT INTO default.chains FORMAT CSV";
cat /project/clickhouse_chains_docs_export.csv | clickhouse-client --host clickhouse-server --query="INSERT INTO default.chains_docs FORMAT CSV";

--select count(1) from default.chains;
--select count(1) from default.chains_docs;

--������� MergeTree �������
drop table default.chains_docs_mt;
create table default.chains_docs_mt
engine = MergeTree partition by intChainID order by intChainID
as
select *
	from default.chains_docs
	limit 100000
;

drop table default.chains_mt;
create table default.chains_mt
engine = MergeTree partition by intChainID order by intChainID
as
select *
	from default.chains
	limit 100000
;

select *
	from default.chains_mt
;

select *
	from default.chains_docs_mt
;

--�����

select count(intChainID) cnt
	from default.chains_mt
		any inner join default.chains_docs_mt
			using intChainID;