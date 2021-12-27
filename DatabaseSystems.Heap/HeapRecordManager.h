#pragma once
#include "../DatabaseSystem.Core/BaseRecordManager.h"
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/File.h"
#include "../DatabaseSystem.Core/Block.h"
#include "HeapFileHead.h"

/*
	Heap, ou arquivo sem qualquer ordena��o, com registros de tamanho fixos,
	inser��o de novos ao final do arquivo, e remo��o baseada em lista encadeada dos registros removidos,
	que dever�o ser reaproveitados em uma nova inser��o posterior a remo��o.
*/
class HeapRecordManager : public BaseRecordManager
{
public:
	HeapRecordManager(size_t blockSize);
	virtual void Create(string path, Schema schema) override;
	virtual void Open(string path) override;
	virtual void Close() override;

	// Inherited via BaseRecordManager
	virtual Schema& GetSchema() override;
	virtual void Insert(Record record) override;
	virtual void Delete(unsigned long long id) override;

protected:
	// Inherited via BaseRecordManager
	virtual void MoveToStart() override;
	virtual bool MoveNext(Record* record) override;

private:
	File<HeapFileHead>* m_File;
	Block* m_ReadBlock;
	Block* m_WriteBlock;
	unsigned long long m_NextReadBlockNumber;
	list<unsigned long long> m_RemovedRecords;
	unsigned long long m_RecordsPerBlock;

	void WriteAndRead();
	void ReadNextBlock();

	struct HeapRecord {
		unsigned long long Id;
	};
};

