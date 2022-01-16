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

	// Inherited via BaseRecordManager
	virtual Record* Select(unsigned long long id) override;
	virtual void Insert(Record record) override;
	virtual void Delete(unsigned long long id) override;

protected:
	// Inherited via BaseRecordManager
	virtual FileHead* CreateNewFileHead(Schema* schema) override;
	virtual FileWrapper<FileHead>* GetFile() override;
	virtual void DeleteInternal(unsigned long long blockId, unsigned long long recordNumberInBlock) override;

private:
	FileWrapper<HashFileHead>* m_File;
	int m_NumberOfBuckets;

	unsigned int hashFunction(unsigned long long key);

	struct HashRecord {
		unsigned long long Id;
	};
};

