#include "pch.h"
#include "../DatabaseSystem.Core/Assertions.h"
#include "HeapVarRecordManager.h"

HeapVarRecordManager::HeapVarRecordManager(size_t blockSize, int reorderCount) :
	m_File(new FileWrapper<HeapVarFileHead>(blockSize)),
	m_ReadBlock(nullptr),
	m_WriteBlock(nullptr),
	m_NextReadBlockNumber(0),
	m_RecordsPerBlock(0),
	m_ReorganizeCount(reorderCount)
{
}

void HeapVarRecordManager::Create(string path, Schema* schema)
{
	m_File->NewFile(path, new HeapVarFileHead(schema));

	auto schemaSize = GetSchema()->GetSize();
	auto blockLength = m_File->GetBlockSize();
	auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
	m_RecordsPerBlock = floor(blockContentLength / schemaSize);

	m_ReadBlock = m_File->CreateBlock();
	m_WriteBlock = m_File->CreateBlock();
}

void HeapVarRecordManager::Open(string path)
{
	m_File->Open(path, new HeapVarFileHead());

	auto schemaSize = GetSchema()->GetSize();
	auto blockLength = m_File->GetBlockSize();
	auto blockContentLength = m_File->GetBlockSize() - sizeof(unsigned int);
	m_RecordsPerBlock = floor(blockContentLength / schemaSize);

	m_ReadBlock = m_File->CreateBlock();
	m_WriteBlock = m_File->CreateBlock();
}

void HeapVarRecordManager::Close()
{
	m_File->Close();
}

Schema* HeapVarRecordManager::GetSchema()
{
	return m_File->GetHead()->GetSchema();
}

unsigned long long HeapVarRecordManager::GetSize()
{
	auto writtenBlocks = m_File->GetBlockSize() * m_File->GetHead()->GetBlocksCount();
	auto recordsToBeWritten = m_WriteBlock->GetRecordsCount() * GetSchema()->GetSize();
	return writtenBlocks + recordsToBeWritten;
}

void HeapVarRecordManager::Insert(Record record)
{
	auto fileHead = m_File->GetHead();
	auto heapRecord = record.As<HeapVarRecord>();
	heapRecord->Id = fileHead->NextId;
	fileHead->NextId += 1;

	if (fileHead->RemovedCount > 0)
	{
		auto pointerToRecordToReplace = fileHead->RemovedRecordHead;

		m_File->GetBlock(pointerToRecordToReplace.BlockId, m_ReadBlock);

		auto recordToRemovePointer = RecordVarPointer();

		span<unsigned char> recordToReplace;
		if (!m_ReadBlock->GetRecordSpan(pointerToRecordToReplace.RecordNumberInBlock, &recordToReplace)) {
			Assert(false, "Record to replace not found");
			// Skip and write to the WriteBlock so the record is not lost
			// Can we treat this better?
			goto write_block;
		}

		auto recordToRemoveHeapData = Record::Cast<HeapVarRecord>(&recordToReplace);
		fileHead->RemovedRecordHead = recordToRemoveHeapData->NextDeleted;

		memcpy(recordToReplace.data(), record.GetData(), GetSchema()->GetSize());

		recordToRemoveHeapData->NextDeleted = m_File->GetHead()->RemovedRecordHead;
		m_File->WriteBlock(m_ReadBlock, pointerToRecordToReplace.BlockId);

		fileHead->RemovedCount -= 1;
		return;
	}

write_block:
	auto nextRecordMapSize = m_WriteBlock->GetFinishRecordMap() + sizeof(struct Block::BlockRecordMap);
	auto newStartRecordsPos = m_WriteBlock->GetStartRecords() - record.GetDataSize();
	int evaluate = newStartRecordsPos - nextRecordMapSize;
	if (evaluate <= 0) {
		m_File->AddBlock(m_WriteBlock);
		m_WriteBlock->Clear();
	}

	auto recordData = record.GetData();
	m_WriteBlock->Append(*recordData);
}

void HeapVarRecordManager::MoveToStart()
{
	m_NextReadBlockNumber = 0;
}

bool HeapVarRecordManager::MoveNext(Record* record, unsigned long long& accessedBlocks, unsigned long long& blockId, unsigned long long& recordNumberInBlock)
{
	auto blocksCount = m_File->GetHead()->GetBlocksCount();
	auto initialBlock = m_NextReadBlockNumber;
	if (m_NextReadBlockNumber == 0)
	{
		//did not start to read before
		if (blocksCount > 0)
		{
			ReadNextBlock();
		}
	}

	bool returnVal = GetNextRecordInFile(record);

	if (returnVal)
	{
		recordNumberInBlock = m_ReadBlock->GetPosition() - 1;
		blockId = m_NextReadBlockNumber - 1;
	}
	accessedBlocks = m_NextReadBlockNumber - initialBlock;
	return returnVal;
}

void HeapVarRecordManager::DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock)
{
	auto fileHead = m_File->GetHead();
	if (blockNumber == fileHead->GetBlocksCount())
	{
		// Record to remove was not written to file
		// Remove it from m_WriteBlock

		Assert(m_WriteBlock->RemoveRecordAt(recordNumberInBlock), "Block was invalid");
	}


	auto recordToRemovePointer = RecordVarPointer();
	recordToRemovePointer.BlockId = blockNumber;
	recordToRemovePointer.RecordNumberInBlock = recordNumberInBlock;

	if (fileHead->RemovedCount > 0)
	{
		auto pointerToLastRemovedRecord = m_File->GetHead()->RemovedRecordTail;
		m_File->GetBlock(pointerToLastRemovedRecord.BlockId, m_ReadBlock);

		span<unsigned char> lastRemovedRecord;
		if (!m_ReadBlock->GetRecordSpan(pointerToLastRemovedRecord.RecordNumberInBlock, &lastRemovedRecord)) {
			Assert(false, "Oh no!");
			return;
		}

		auto lastRemovedRecordHeapData = Record::Cast<HeapVarRecord>(&lastRemovedRecord);
		lastRemovedRecordHeapData->NextDeleted = recordToRemovePointer;
		m_File->WriteBlock(m_ReadBlock, pointerToLastRemovedRecord.BlockId);
	}
	else
	{
		fileHead->RemovedRecordHead = recordToRemovePointer;
	}
	fileHead->RemovedRecordTail = recordToRemovePointer;
	fileHead->RemovedCount += 1;


	m_File->GetBlock(blockNumber, m_ReadBlock);
	span<unsigned char> recordToRemove;
	if (!m_ReadBlock->GetRecordSpan(recordNumberInBlock, &recordToRemove)) {
		Assert(false, "Oh no!");
		return;
	}

	// Mark as removed
	auto recordToRemoveHeapData = Record::Cast<HeapVarRecord>(&recordToRemove);
	recordToRemoveHeapData->Id = -1;
	m_File->WriteBlock(m_ReadBlock, blockNumber);

	if (fileHead->RemovedCount > m_ReorganizeCount) {
		Reorganize();
	}
}

void HeapVarRecordManager::WriteAndRead()
{
	m_File->AddBlock(m_WriteBlock);
	m_WriteBlock->Clear();
	ReadNextBlock();
}

void HeapVarRecordManager::ReadNextBlock()
{
	m_ReadBlock->Clear();
	m_File->GetBlock(m_NextReadBlockNumber, m_ReadBlock);
	m_ReadBlock->MoveToStart();
	m_WriteBlock->MoveToStart();
	m_NextReadBlockNumber++;
}

bool HeapVarRecordManager::GetNextRecordInFile(Record* record)
{
	auto recordData = record->GetData();
	auto blocksInFile = m_File->GetHead()->GetBlocksCount();
	while (blocksInFile > 0 && m_NextReadBlockNumber < blocksInFile - 1)
	{
		//auto currRecordSize = m_ReadBlock->GetCurrRecordSize();
		//if (currRecordSize > 0)
		//    record->ResizeData(currRecordSize);
		while (m_ReadBlock->GetRecord(recordData))
		{
			auto recordHeapData = record->As<HeapVarRecord>();
			if (recordHeapData->Id != -1)
			{
				return true;
			}
		}
		ReadNextBlock();
	}

	// Not found in the file
	// Look in the write buffer
	if (m_WriteBlock->GetRecordsCount() > 0)
	{

		while (m_WriteBlock->GetPosition() < m_WriteBlock->GetRecordsCount())
		{
			if (m_WriteBlock->GetRecord(recordData))
			{
				// Here we dont have to check for the id. 
				// When we remove records from the write block we dont set the id.
				// Just remove from the list
				return true;
			}
		}
	}
	return false;
}

void HeapVarRecordManager::Reorganize()
{

	auto fileHead = m_File->GetHead();
	auto blocksCount = fileHead->GetBlocksCount();
	auto tempBuffer = vector<unsigned char>(m_File->GetBlockSize());

	for (int blockId = 0;blockId < blocksCount; blockId++) {
		m_File->GetBlock(blockId, m_ReadBlock);

		auto recordsCount = m_ReadBlock->GetRecordsCount();

		unsigned int leftPointer = m_ReadBlock->GetRecordPos(recordsCount);
		unsigned int rightPointer = leftPointer;
		unsigned int bytesShifted = 0;
		unsigned int bytesShiftedSum = 0;

		auto removedRecords = vector<int>();

		//m_ReadBlock->PrintRecordMapper();
		for (int recordNumber = recordsCount; recordNumber >= 0; recordNumber--) {
			auto id = m_ReadBlock->GetVarRecordId(recordNumber);
			auto recordLength = m_ReadBlock->GetRecordLength(recordNumber);
			if (id == -1) {
				//TODO: Case when first sequence of blocks was ddeleted
				removedRecords.push_back(recordNumber);
				bytesShifted = recordLength;
				bytesShiftedSum += bytesShifted;

				m_ReadBlock->ShiftBytes(tempBuffer, leftPointer, rightPointer - leftPointer, bytesShifted, false);
				UpdateRecordRangePos(recordNumber, bytesShifted);
				leftPointer += bytesShifted;
				rightPointer += bytesShifted;

			}
			else {
				rightPointer += recordLength;
			}
		}

		auto newPos = m_ReadBlock->GetStartRecords() - bytesShiftedSum;
		m_ReadBlock->UpdateStartRecordPos(newPos);
		auto newCount = removedRecords.size();
		m_ReadBlock->UpdateRecordsCount(recordsCount - newCount);

		// Update record map, add shifted bytes to initialPos + shift map removing record maps of deleted records
		for (int i = 0; i < removedRecords.size(); i++) {
			auto removedRecordNumber = removedRecords[i] - 1;
			m_ReadBlock->RemoveRecordMap(tempBuffer, removedRecordNumber);
		}

		//m_ReadBlock->PrintRecordMapper();
		memset(tempBuffer.data(), 0, sizeof(tempBuffer));
		m_File->WriteBlock(m_ReadBlock, blockId);
	}
}

void HeapVarRecordManager::UpdateRecordRangePos(unsigned int maxRecord, unsigned int bytesShifted)
{
	for (int recordNumber = maxRecord - 1; recordNumber < m_ReadBlock->GetRecordsCount(); recordNumber++) {
		m_ReadBlock->UpdateRecordMapPos(recordNumber, bytesShifted);
	}
}