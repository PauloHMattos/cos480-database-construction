#include "pch.h"
#include "../DatabaseSystem.Core/Assertions.h"
#include "HeapVarRecordManager.h"

HeapVarRecordManager::HeapVarRecordManager(size_t blockSize, float maxPercentEmptySpace) :
	BaseRecordManager(),
	m_MaxPercentEmptySpace(maxPercentEmptySpace),
	m_File(new FileWrapper<HeapVarFileHead>(blockSize))
{
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

void HeapVarRecordManager::DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock)
{
	auto fileHead = m_File->GetHead();
	if (blockNumber == fileHead->GetBlocksCount())
	{
		// Record to remove was not written to file
		// Remove it from m_WriteBlock
		auto removed = m_WriteBlock->RemoveRecordAt(recordNumberInBlock);
		Assert(removed, "Block was invalid");
		return;
	}

	auto recordToRemovePointer = RecordVarPointer();
	recordToRemovePointer.BlockId = blockNumber;
	recordToRemovePointer.RecordNumberInBlock = recordNumberInBlock;

	if (fileHead->RemovedCount > 0)
	{
		auto pointerToLastRemovedRecord = m_File->GetHead()->RemovedRecordTail;
		if (blockNumber != pointerToLastRemovedRecord.BlockId)
		{
			ReadBlock(m_ReadBlock, pointerToLastRemovedRecord.BlockId);
		}

		span<unsigned char> lastRemovedRecord;
		if (!m_ReadBlock->GetRecordSpan(pointerToLastRemovedRecord.RecordNumberInBlock, &lastRemovedRecord))
		{
			Assert(false, "Could not retrieve record span");
			return;
		}

		auto lastRemovedRecordHeapData = Record::Cast<HeapVarRecord>(&lastRemovedRecord);
		Assert(lastRemovedRecordHeapData->Id == -1, "This record was not marked as deleted");
		lastRemovedRecordHeapData->NextDeleted = recordToRemovePointer;
		WriteBlock(m_ReadBlock, pointerToLastRemovedRecord.BlockId);
	}
	else
	{
		fileHead->RemovedRecordHead = recordToRemovePointer;
	}

	ReadBlock(m_ReadBlock, blockNumber);
	span<unsigned char> recordToRemove;
	if (!m_ReadBlock->GetRecordSpan(recordNumberInBlock, &recordToRemove))
	{
		Assert(false, "Could not retrieve record span");
		return;
	}

	// Mark as removed
	auto recordToRemoveHeapData = Record::Cast<HeapVarRecord>(&recordToRemove);
	recordToRemoveHeapData->Id = -1;
	WriteBlock(m_ReadBlock, blockNumber);

	fileHead->RemovedRecordTail = recordToRemovePointer;
	fileHead->RemovedCount += 1;
}

void HeapVarRecordManager::Reorganize()
{
	auto fileHead = m_File->GetHead();
	auto recordSize = GetSchema()->GetSize();
	if (fileHead->RemovedCount * recordSize < m_MaxPercentEmptySpace * GetSize())
	{
		return;
	}

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

FileHead* HeapVarRecordManager::CreateNewFileHead(Schema* schema)
{
	auto fileHead = new HeapVarFileHead(schema);
	return fileHead;
}

FileWrapper<FileHead>* HeapVarRecordManager::GetFile()
{
	return (FileWrapper<FileHead>*)m_File;
}
