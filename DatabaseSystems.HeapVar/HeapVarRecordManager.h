#pragma once
#include "../DatabaseSystem.Core/BaseRecordManager.h"
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/File.h"
#include "../DatabaseSystem.Core/Block.h"
#include "HeapVarFileHead.h"

/*
	Heap, ou arquivo sem qualquer ordenação, com registros de tamanho fixos,
	inserção de novos ao final do arquivo, e remoção baseada em lista encadeada dos registros removidos,
	que deverão ser reaproveitados em uma nova inserção posterior a remoção.
*/
class HeapVarRecordManager : public BaseRecordManager
{
public:
	HeapVarRecordManager(size_t blockSize, float maxPercentEmptySpace);

	// Inherited via BaseRecordManager
	virtual void Insert(Record record) override;

	void UpdateRecordRangePos(unsigned int maxRecord, unsigned int bytesShifted);
	virtual void Reorganize() override;

protected:
	// Inherited via BaseRecordManager
	virtual void DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock) override;
	bool GetNextRecordInFile(Record* record);
	virtual FileHead* CreateNewFileHead(Schema* schema) override;
	virtual FileWrapper<FileHead>* GetFile() override;


private:
	FileWrapper<HeapVarFileHead>* m_File;
	float m_MaxPercentEmptySpace;

	struct HeapVarRecord {
		unsigned long long Id;
		RecordVarPointer NextDeleted;
	};
};

