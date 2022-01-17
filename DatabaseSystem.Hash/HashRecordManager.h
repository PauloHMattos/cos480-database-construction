#pragma once
#include "../DatabaseSystem.Core/BaseRecordManager.h"
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/File.h"
#include "../DatabaseSystem.Core/Block.h"
#include "HashFileHead.h"

/*
	Hash externo est�tico, com registros distribu�dos segundo o campo Id como chave de hashing.
	Foi utilizada a fun��o m�dulo usando o n�mero de buckets alocados como fun��o de hashing.
	O tratamento de colis�o foi feito por meio do conjunto de overflow buckets.
*/
class HashRecordManager : public BaseRecordManager
{
public:
	HashRecordManager(size_t blockSize, unsigned int numberOfBuckets);
	virtual void Create(string path, Schema* schema) override;
	virtual void Open(string path) override;

	// Inherited via BaseRecordManager
	virtual Record* Select(unsigned long long id) override;
	virtual vector<Record*> SelectWhereEquals(unsigned int columnId, span<unsigned char> data) override;

	virtual void Insert(Record record) override;
	virtual void Delete(unsigned long long id) override;
	virtual int DeleteWhereEquals(unsigned int columnId, span<unsigned char> data);

protected:
	// Inherited via BaseRecordManager
	virtual FileHead* CreateNewFileHead(Schema* schema) override;
	virtual FileWrapper<FileHead>* GetFile() override;
	virtual void DeleteInternal(unsigned long long recordId, unsigned long long blockId, unsigned long long recordNumberInBlock) override;
	virtual void Reorganize() override;

private:
	FileWrapper<HashFileHead>* m_File;
	int m_NumberOfBuckets;

	unsigned int hashFunction(unsigned long long key);
	bool SelectSpan(unsigned long long id, span<unsigned char>* data);
	
	struct HashRecord {
		unsigned long long Id;
	};
};

