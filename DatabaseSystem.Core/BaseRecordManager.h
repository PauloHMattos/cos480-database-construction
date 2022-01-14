#pragma once

#include "Schema.h"
#include "Record.h"

class BaseRecordManager
{
public:
	virtual void Create(string path, Schema* schema) = 0;
	virtual void Create(string path, Schema* schema, unsigned int orderByColumnId) = 0;
	virtual void Open(string path) = 0;
	virtual void Close() = 0;
	virtual Schema* GetSchema() = 0;
	virtual unsigned long long GetSize() = 0;
	unsigned long long GetLastQueryBlockAccessCount() const;

	// ---------------------------------------------- <INSERT> --------------------------------------------------------------------------
	/*
	* Inser��o de um �nico registro.
	*/
	virtual void Insert(Record record) = 0;
	/*
	* Inser��o de um conjunto de registros.
	*/
	virtual void InsertMany(vector<Record> records);
	// ---------------------------------------------- </INSERT> --------------------------------------------------------------------------


	// ---------------------------------------------- <SELECT> --------------------------------------------------------------------------
	/*
	* Sele��o de um �nico registro (Find) pela sua chave prim�ria (ou campo UNIQUE).
	*	Por exemplo, selecionar o registro de um ALUNO pelo seu DRE fornecido.
	* 
	* Penso que todos os registros obrigat�riamente v�o ter a coluna ID que � unica e auto increment�vel.
	* O ultimo Id pode fazer parte do header do arquivo
	*/
	virtual Record* Select(unsigned long long id);
	/*
	* Sele��o de conjunto de registros (FindAll) cujas chave prim�ria (ou campo UNIQUE) estejam contidas
	* em um conjunto de valores n�o necessariamente sequenciais.
	*	Por exemplo, selecionar todos os registros dos ALUNOS cuja lista de DRE est�o inscritos na TURMA de c�digo 1020.
	*/
	virtual vector<Record*> Select(vector<unsigned long long> ids);
	/*
	* Sele��o de um conjunto de registros (FindAll) cujas chave prim�ria (ou campo UNIQUE) estejam contidas
	* em uma faixa de valores dado como par�metro.
	*	Por exemplo, todos os ALUNOS cujo DRE esteja na faixa entre "119nnnnnn" e "120nnnnnn"
	*	(onde "n" � qualquer d�gito de 0 a 9, ou em SQL: ... where DRE between 119000000 and 120999999
	*/
	virtual vector<Record*> SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max);
	/*
	* Sele��o de todos os registros (FindAll) cujos valores de um campo n�o chave sejam iguais a um dado par�metro fornecido.
	*	Ou seja, sele��o por um campo que permite repeti��o de valores entre registros.
	*	Por exemplo, recuperar todos os registros das PESSOAS cujo campo CIDADE seja igual a "Rio de Janeiro".
	*/
	virtual vector<Record*> SelectWhereEquals(unsigned int columnId, span<unsigned char> data);
	// ---------------------------------------------- </SELECT> --------------------------------------------------------------------------
	
	
	// ---------------------------------------------- <DELETE> --------------------------------------------------------------------------
	/*
	* Remo��o de um �nico registro selecionado atrav�s da chave prim�ria (ou campo cujo valor seja �nico)
	*	Por exemplo, remover o ALUNO cujo DRE � dado.
	*/
	virtual void Delete(unsigned long long id);
	/*
	* Remo��o de um conjunto de registros selecionados por algum crit�rio
	*	Por exemplo, remover todos os ALUNOS da tabela INSCRITOS cuja turma seja a de NUMERO=1023.
	*/
	virtual int DeleteWhereEquals(unsigned int columnId, span<unsigned char> data);
	// ---------------------------------------------- </DELETE> --------------------------------------------------------------------------

protected:
	unsigned long long m_LastQueryBlockAccessCount;
	bool MoveNext(Record* record, unsigned long long& accessedBlocks);

	virtual void MoveToStart() = 0;
	virtual bool MoveNext(Record* record, unsigned long long& accessedBlocks, unsigned long long& blockId, unsigned long long& recordNumberInBlock) = 0;
	virtual void DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock) = 0;
};

