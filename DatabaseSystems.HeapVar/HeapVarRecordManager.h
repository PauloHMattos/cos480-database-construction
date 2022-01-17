#pragma once
#include "../DatabaseSystem.Core/BaseRecordManager.h"
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/File.h"
#include "../DatabaseSystem.Core/Block.h"
#include "HeapVarFileHead.h"

/*
	Heap, ou arquivo sem qualquer ordena��o, com registros de tamanho fixos,
	inser��o de novos ao final do arquivo, e remo��o baseada em lista encadeada dos registros removidos,
	que dever�o ser reaproveitados em uma nova inser��o posterior a remo��o.
*/
class HeapVarRecordManager : public BaseRecordManager
{
public:
	HeapVarRecordManager(size_t blockSize, int reorderCount);
	virtual void Create(string path, Schema* schema) override;
	virtual void Open(string path) override;
	virtual void Close() override;

	// Inherited via BaseRecordManager
	virtual void Insert(Record record) override;

	void UpdateRecordRangePos(unsigned int maxRecord, unsigned int bytesShifted);
	void Reorganize();

protected:
	// Inherited via BaseRecordManager
	virtual void DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock) override;
	bool GetNextRecordInFile(Record* record);
	virtual FileHead* CreateNewFileHead(Schema* schema) override;
	virtual FileWrapper<FileHead>* GetFile() override;


private:
	FileWrapper<HeapVarFileHead>* m_File;
	Block* m_ReadBlock;
	Block* m_WriteBlock;
	unsigned long long m_NextReadBlockNumber;
	unsigned long long m_RecordsPerBlock;
	int m_ReorganizeCount;

	void WriteAndRead();
	void ReadNextBlock();

	struct HeapVarRecord {
		unsigned long long Id;
		RecordVarPointer NextDeleted;
	};
};

