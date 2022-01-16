#include "pch.h"
#include "HeapRecordManager.h"
#include "../DatabaseSystem.Core/Assertions.h"

HeapRecordManager::HeapRecordManager(size_t blockSize, float maxPercentEmptySpace) :
    BaseRecordManager(),
    m_File(new FileWrapper<HeapFileHead>(blockSize)),
    m_MaxPercentEmptySpace(maxPercentEmptySpace)
{
}

void HeapRecordManager::Insert(Record record)
{
    ClearAccessCount();

    auto fileHead = m_File->GetHead();
    auto heapRecord = record.As<HeapRecord>();
    heapRecord->Id = fileHead->NextId;
    fileHead->NextId += 1;

    if (fileHead->RemovedCount > 0)
    {
        auto pointerToRecordToReplace = fileHead->RemovedRecordHead;
        
        ReadBlock(m_ReadBlock, pointerToRecordToReplace.BlockId);

        auto recordToRemovePointer = RecordPointer();

        span<unsigned char> recordToReplace;
        if (!m_ReadBlock->GetRecordSpan(pointerToRecordToReplace.RecordNumberInBlock, &recordToReplace)) 
        {
            Assert(false, "Record to replace not found");
            // Skip and write to the WriteBlock so the record is not lost
            // Can we treat this better?
            goto write_block; 
        }

        auto recordToRemoveHeapData = Record::Cast<HeapRecord>(&recordToReplace);
        fileHead->RemovedRecordHead = recordToRemoveHeapData->NextDeleted;

        memcpy(recordToReplace.data(), record.GetData(), GetSchema()->GetSize());
        
        recordToRemoveHeapData->NextDeleted = fileHead->RemovedRecordHead;
        WriteBlock(m_ReadBlock, pointerToRecordToReplace.BlockId);
        fileHead->RemovedCount -= 1;
        return;
    }

write_block:
    auto recordsCount = m_WriteBlock->GetRecordsCount();
    if (recordsCount == m_RecordsPerBlock) 
    {
        AddBlock(m_WriteBlock);
        m_WriteBlock->Clear();
    }

    auto recordData = record.GetData();
    m_WriteBlock->Append(*recordData);
}

FileHead* HeapRecordManager::CreateNewFileHead(Schema* schema)
{
    return new HeapFileHead(schema);
}

FileWrapper<FileHead>* HeapRecordManager::GetFile()
{
    return (FileWrapper<FileHead>*)m_File;
}

void HeapRecordManager::DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock)
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

    auto recordToRemovePointer = RecordPointer();
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

        auto lastRemovedRecordHeapData = Record::Cast<HeapRecord>(&lastRemovedRecord);
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
    auto recordToRemoveHeapData = Record::Cast<HeapRecord>(&recordToRemove);
    recordToRemoveHeapData->Id = -1;
    WriteBlock(m_ReadBlock, blockNumber);

    fileHead->RemovedRecordTail = recordToRemovePointer;
    fileHead->RemovedCount += 1;
}


void HeapRecordManager::Reorganize()
{
    auto fileHead = m_File->GetHead();
    auto recordSize = GetSchema()->GetSize();
    if (fileHead->RemovedCount * recordSize < m_MaxPercentEmptySpace * GetSize())
    {
        return;
    }

    // Write all data to the file
    if (m_WriteBlock->GetRecordsCount() > 0)
    {
        AddBlock(m_WriteBlock);
        m_WriteBlock->Clear();
        ReadNextBlock();
    }

    auto totalBlocksCount = fileHead->GetBlocksCount();
    if (totalBlocksCount == 0)
    {
        return;
    }

    auto pointerToRecordToReplace = fileHead->RemovedRecordHead;
    ReadBlock(m_WriteBlock, pointerToRecordToReplace.BlockId);

    auto currentReadBlock = totalBlocksCount - 1;
    while (fileHead->RemovedCount > 0 && currentReadBlock > 0)
    {
        ReadBlock(m_ReadBlock, currentReadBlock);
        m_ReadBlock->MoveToEnd();

        while (fileHead->RemovedCount > 0 && m_ReadBlock->GetRecordsCount() > 0)
        {
            m_ReadBlock->Retreat();
            span<unsigned char> recordData;
            if (!m_ReadBlock->GetCurrentSpan(&recordData))
            {
                Assert(false, "Could not retrieve record span");
                return;
            }

            if (m_WriteBlock->GetRecordsCount() == 0)
            {
                // == 0 indicates that we already cleared from here
                // because of the back walking we are doing
                fileHead->RemovedCount = 0;
                pointerToRecordToReplace = RecordPointer();
                pointerToRecordToReplace.BlockId = -1;
                fileHead->RemovedRecordTail = pointerToRecordToReplace;
                break;
            }

            span<unsigned char> recordToReplaceData;
            m_WriteBlock->GetRecordSpan(pointerToRecordToReplace.RecordNumberInBlock, &recordToReplaceData);

            auto currentBlock = pointerToRecordToReplace.BlockId;
            auto recordToReplace = Record::Cast<HeapRecord>(&recordToReplaceData);
            Assert(recordToReplace->Id == -1, "This record was not removed");
            pointerToRecordToReplace = recordToReplace->NextDeleted;

            memcpy(recordToReplaceData.data(), recordData.data(), recordData.size());

            if (currentBlock != pointerToRecordToReplace.BlockId)
            {
                WriteBlock(m_WriteBlock, currentBlock);
                ReadBlock(m_WriteBlock, pointerToRecordToReplace.BlockId);
            }
            m_ReadBlock->Remove();
            fileHead->RemovedCount--;
        }

        WriteBlock(m_ReadBlock, currentReadBlock);
        currentReadBlock--;
    }
    fileHead->RemovedRecordHead = pointerToRecordToReplace;
    fileHead->SetBlocksCount(currentReadBlock);
    m_File->Trim();
}