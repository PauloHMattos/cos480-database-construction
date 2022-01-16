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
	HeapRecordManager(size_t blockSize, int reorderCount);

	// Inherited via BaseRecordManager
	virtual void Insert(Record record) override;

protected:
	// Inherited via BaseRecordManager
	virtual FileHead* CreateNewFileHead(Schema* schema) override;
	virtual FileWrapper<FileHead>* GetFile() override;
	virtual void DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock) override;
	void Reorganize();
	
private:
	FileWrapper<HeapFileHead>* m_File;
	int m_ReorganizeCount;

	void WriteAndRead();

	struct HeapRecord {
		unsigned long long Id;
		RecordPointer NextDeleted;
	};
};

