#pragma once
#include "../DatabaseSystem.Core/BaseRecordManager.h"
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/File.h"
#include "../DatabaseSystem.Core/Block.h"
#include "HashFileHead.h"

/*
	Heap, ou arquivo sem qualquer ordenação, com registros de tamanho fixos,
	inserção de novos ao final do arquivo, e remoção baseada em lista encadeada dos registros removidos,
	que deverão ser reaproveitados em uma nova inserção posterior a remoção.
*/
class HashRecordManager : public BaseRecordManager
{
public:
	HashRecordManager(size_t blockSize, unsigned int numberOfBuckets);
	virtual void Create(string path, Schema* schema) override;
	virtual void Open(string path) override;
	virtual void Close() override;

	// Inherited via BaseRecordManager
	virtual Schema* GetSchema() override;
	virtual unsigned long long GetSize() override;
	virtual Record* Select(unsigned long long id) override;
	virtual void Insert(Record record) override;
	virtual void Delete(unsigned long long id) override;

protected:
	// Inherited via BaseRecordManager
	virtual void MoveToStart() override;
	virtual bool MoveNext(Record* record, unsigned long long& accessedBlocks) override;

private:
	File<HashFileHead>* m_File;
	Block* m_ReadBlock;
	Block* m_WriteBlock;
	unsigned long long m_NextReadBlockNumber;
	list<unsigned long long> m_RemovedRecords;
	unsigned long long m_RecordsPerBlock;
	unsigned int m_NumberOfBuckets;
	vector<Bucket> m_Buckets;

	void WriteAndRead();
	void ReadNextBlock();
	unsigned int hashFunction(unsigned long long key);

	struct HashRecord {
		unsigned long long Id;
	};
};

