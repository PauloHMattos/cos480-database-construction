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
	HeapRecordManager(size_t blockSize, float reorderCount);

	// Inherited via BaseRecordManager
	virtual void Insert(Record record) override;

protected:
	// Inherited via BaseRecordManager
	virtual FileHead* CreateNewFileHead(Schema* schema) override;
	virtual FileWrapper<FileHead>* GetFile() override;
	virtual void DeleteInternal(unsigned long long recordId, unsigned long long blockNumber, unsigned long long recordNumberInBlock) override;
	virtual void Reorganize() override;
	
private:
	FileWrapper<HeapFileHead>* m_File;
	float m_MaxPercentEmptySpace;

	struct HeapRecord {
		unsigned long long Id;
		RecordPointer NextDeleted;
	};
};

