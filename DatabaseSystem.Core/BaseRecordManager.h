#pragma once

#include "Schema.h"
#include "Record.h"
#include "File.h"
#include "FileHead.h"

class BaseRecordManager
{
public:
	BaseRecordManager();
	virtual void Create(string path, Schema* schema);
	virtual void Open(string path);
	virtual void Close();
	Schema* GetSchema();
	unsigned long long GetSize();
	unsigned long long GetLastQueryBlockReadAccessCount() const;
	unsigned long long GetLastQueryBlockWriteAccessCount() const;

	// ---------------------------------------------- <INSERT> --------------------------------------------------------------------------
	/*
	* Inserção de um único registro.
	*/
	virtual void Insert(Record record) = 0;
	/*
	* Inserção de um conjunto de registros.
	*/
	virtual void InsertMany(vector<Record> records);
	// ---------------------------------------------- </INSERT> --------------------------------------------------------------------------


	// ---------------------------------------------- <SELECT> --------------------------------------------------------------------------
	/*
	* Seleção de um único registro (Find) pela sua chave primária (ou campo UNIQUE).
	*	Por exemplo, selecionar o registro de um ALUNO pelo seu DRE fornecido.
	* 
	* Penso que todos os registros obrigatóriamente vão ter a coluna ID que é unica e auto incrementável.
	* O ultimo Id pode fazer parte do header do arquivo
	*/
	virtual Record* Select(unsigned long long id);
	/*
	* Seleção de conjunto de registros (FindAll) cujas chave primária (ou campo UNIQUE) estejam contidas
	* em um conjunto de valores não necessariamente sequenciais.
	*	Por exemplo, selecionar todos os registros dos ALUNOS cuja lista de DRE estão inscritos na TURMA de código 1020.
	*/
	virtual vector<Record*> Select(vector<unsigned long long> ids);
	/*
	* Seleção de um conjunto de registros (FindAll) cujas chave primária (ou campo UNIQUE) estejam contidas
	* em uma faixa de valores dado como parâmetro.
	*	Por exemplo, todos os ALUNOS cujo DRE esteja na faixa entre "119nnnnnn" e "120nnnnnn"
	*	(onde "n" é qualquer dígito de 0 a 9, ou em SQL: ... where DRE between 119000000 and 120999999
	*/
	virtual vector<Record*> SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max);
	/*
	* Seleção de todos os registros (FindAll) cujos valores de um campo não chave sejam iguais a um dado parâmetro fornecido.
	*	Ou seja, seleção por um campo que permite repetição de valores entre registros.
	*	Por exemplo, recuperar todos os registros das PESSOAS cujo campo CIDADE seja igual a "Rio de Janeiro".
	*/
	virtual vector<Record*> SelectWhereEquals(unsigned int columnId, span<unsigned char> data);
	// ---------------------------------------------- </SELECT> --------------------------------------------------------------------------
	
	
	// ---------------------------------------------- <DELETE> --------------------------------------------------------------------------
	/*
	* Remoção de um único registro selecionado através da chave primária (ou campo cujo valor seja único)
	*	Por exemplo, remover o ALUNO cujo DRE é dado.
	*/
	virtual void Delete(unsigned long long id);
	/*
	* Remoção de um conjunto de registros selecionados por algum critério
	*	Por exemplo, remover todos os ALUNOS da tabela INSCRITOS cuja turma seja a de NUMERO=1023.
	*/
	virtual int DeleteWhereEquals(unsigned int columnId, span<unsigned char> data);
	// ---------------------------------------------- </DELETE> --------------------------------------------------------------------------

protected:
	struct BaseRecord
	{
		unsigned long long Id;
	};

	Block* m_ReadBlock;
	Block* m_WriteBlock;
	unsigned long long m_RecordsPerBlock;
	unsigned long long m_NextReadBlockNumber;
	unsigned long long m_LastQueryBlockReadAccessCount;
	unsigned long long m_LastQueryBlockWriteAccessCount;

	virtual unsigned long long GetBlocksCount();
	virtual void ReadNextBlock();
	virtual void ReadBlock(Block* block, unsigned long long blockId);
	virtual void WriteBlock(Block* block, unsigned long long blockId);
	virtual void AddBlock(Block* block);
	
	bool TryGetNextValidRecord(Record* record);
	void MoveToStart();
	bool MoveNext(Record* record, unsigned long long& accessedBlocks);
	bool MoveNext(Record* record, unsigned long long& accessedBlocks, unsigned long long& blockId, unsigned long long& recordNumberInBlock);
	
	void ClearAccessCount();
	virtual FileHead* CreateNewFileHead(Schema* schema) = 0;
	virtual FileWrapper<FileHead>* GetFile() = 0;
	virtual void DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock) = 0;
};

