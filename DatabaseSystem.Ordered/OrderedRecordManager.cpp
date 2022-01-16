#include "pch.h"
#include "OrderedRecordManager.h"
#include "../DatabaseSystem.Core/Assertions.h"

OrderedRecordManager::OrderedRecordManager(size_t blockSize) : 
    BaseRecordManager(),
    m_File(new FileWrapper<OrderedFileHead>(blockSize)),
    m_ExtensionFile(new FileWrapper<OrderedFileHead>(blockSize)),
    m_OrderedByColumnId(0),
    m_MaxExtensionFileSize(1000),
    m_DeletedRecords(0),
    m_MaxPercentEmptySpace(0.2),
    m_UsingExtensionAsMain(false)
{
}

OrderedRecordManager::OrderedRecordManager(size_t blockSize, unsigned int orderedByColumnId) : OrderedRecordManager(blockSize)
{
    m_OrderedByColumnId = orderedByColumnId;
}

void OrderedRecordManager::Create(string path, Schema *schema)
{
    BaseRecordManager::Create(path, schema);
    auto extension_path = path.append(".extension");
    m_ExtensionFile->NewFile(extension_path, (OrderedFileHead*)CreateNewFileHead(schema));
}

void OrderedRecordManager::Open(string path)
{
    BaseRecordManager::Open(path);
    auto extension_path = path.append(".extension");
    m_ExtensionFile->Open(extension_path, (OrderedFileHead*)CreateNewFileHead(nullptr));
}

void OrderedRecordManager::Close()
{
    if (m_ExtensionFile->GetHead()->GetBlocksCount() > 0) 
    {
        Reorder();
    }
    if (m_UsingExtensionAsMain) {
        SwitchFilesHard();
    }
    m_File->Close();
    m_ExtensionFile->Close();
}

Schema *OrderedRecordManager::GetSchema()
{
    return m_File->GetHead()->GetSchema();
}

unsigned long long OrderedRecordManager::GetSize()
{
    auto writtenBlocks = m_File->GetBlockSize() * m_File->GetHead()->GetBlocksCount();
    auto extensionBlocks = m_ExtensionFile->GetBlockSize() * m_ExtensionFile->GetHead()->GetBlocksCount();
    auto recordsWritten = m_WriteBlock->GetRecordsCount() * GetSchema()->GetSize();
    return writtenBlocks + extensionBlocks + recordsWritten;
}

void OrderedRecordManager::Insert(Record record)
{
    auto orderedRecord = record.As<OrderedRecord>();
    orderedRecord->Id = m_ExtensionFile->GetHead()->NextId;
    m_ExtensionFile->GetHead()
        ->NextId += 1;

    auto recordsCount = m_WriteBlock->GetRecordsCount();
    if (recordsCount == m_RecordsPerBlock)
    {
        m_ExtensionFile->AddBlock(m_WriteBlock);
        m_WriteBlock->Clear();

        auto blocksCount = m_ExtensionFile->GetHead()->GetBlocksCount();
        if (blocksCount == m_MaxExtensionFileSize)
        {
            // if Extension File has reached max size, call Reorder
            Reorder();
        }
    }

    auto recordData = record.GetData();
    m_WriteBlock->Append(*recordData);
}

Record *OrderedRecordManager::Select(unsigned long long id)
{
    // if the file is ordered by the id
    if (m_OrderedByColumnId == 0) {
        
        m_LastQueryBlockAccessCount = 0;
        unsigned long long accessedBlocks = 0;

        unsigned long long blockId;
        unsigned long long recordNumberInBlock;

        // try to binary search m_File
        auto evalFunc = [](int eval) {
            if (eval == 0) { // equal
                return true;
            }
            return false;
        };
        auto currentRecord = BinarySearch(span<unsigned char>((unsigned char *)&id, sizeof(id)), evalFunc, accessedBlocks);
        m_LastQueryBlockAccessCount += accessedBlocks;
        
        // if not found, try to linear search m_ExtensionFile
        if (currentRecord == nullptr) {
            currentRecord = new Record(GetSchema());

            MoveToExtension();
            while (MoveNext(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
            {
                m_LastQueryBlockAccessCount += accessedBlocks;
                if (currentRecord->getId() == id) {
                    return currentRecord;
                }
            }
        }
        
        return currentRecord;
    }
    // if the file is not ordered by id, linear search everything
    return BaseRecordManager::Select(id);
}

vector<Record *> OrderedRecordManager::SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max)
{
    // if the file is ordered by the column we are selecting
    if (columnId == m_OrderedByColumnId) {

        m_LastQueryBlockAccessCount = 0;
        unsigned long long accessedBlocks = 0;

        auto records = vector<Record*>();
        auto schema = GetSchema();
        auto column = schema->GetColumn(columnId);

        unsigned long long blockId;
        unsigned long long recordNumberInBlock;

        // binary search min
        auto evalFunc = [](int eval) {
            if (eval >= 0) { // value >= target
                return true;
            }
            return false;
        };
        auto currentRecord = BinarySearch(min, evalFunc, accessedBlocks);
        if (currentRecord != nullptr)
        {
            // if min was found, it might not be the first instance
            // MovePrev until finding first
            while (MovePrev(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
            {
                m_LastQueryBlockAccessCount += accessedBlocks;
                auto value = schema->GetValue(currentRecord->GetData(), columnId);

                if (Column::Compare(column, value, min) < 0)
                {
                    break;
                }
            }

            // at this point we should be at the first record 
            // MoveNext while record is smaller than max
            while (MoveNext(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
            {
                m_LastQueryBlockAccessCount += accessedBlocks;
                auto value = schema->GetValue(currentRecord->GetData(), columnId);

                if (Column::Compare(column, value, max) > 0) {
                    break;
                }

                if (Column::Compare(column, value, min) >= 0 && Column::Compare(column, value, max) <= 0)
                {
                    auto newRecord = new Record(schema);
                    memcpy(newRecord->GetData()->data(), currentRecord->GetData()->data(), schema->GetSize());
                    records.push_back(newRecord);
                }
            }
        }
        // at this point, the range is not present in the main file 
        // OR we found all records in the range in the main file 
        // OR we stopped somewhere in the extension file
        // there might still be records in the range in the extension file

        auto mainFileblocksCount = m_File->GetHead()->GetBlocksCount();
        if (m_NextReadBlockNumber < mainFileblocksCount || currentRecord == nullptr) // we stopped somewhere in the main file
        {
            // we found all records in the range in the main file
            // move to the star of extension file and continue
            MoveToExtension();
            currentRecord = new Record(schema);
        }

        // m_NextBlockNumber is somewhere in the extension file
        // linear search extension file continuing from that point
        while (MoveNext(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
        {
            m_LastQueryBlockAccessCount += accessedBlocks;
            auto value = schema->GetValue(currentRecord->GetData(), columnId);

            if (Column::Compare(column, value, min) >= 0 && Column::Compare(column, value, max) <= 0)
            {
                auto newRecord = new Record(schema);
                memcpy(newRecord->GetData()->data(), currentRecord->GetData()->data(), schema->GetSize());
                records.push_back(newRecord);
            }
        }
        return records;
    }
    // if the file is not ordered by id, linear search everything
    return BaseRecordManager::SelectWhereBetween(columnId, min, max);

}

vector<Record *> OrderedRecordManager::SelectWhereEquals(unsigned int columnId, span<unsigned char> data)
{
    // if the file is ordered by the column we are selecting
    if (columnId == m_OrderedByColumnId) {

        m_LastQueryBlockAccessCount = 0;
        unsigned long long accessedBlocks = 0;

        unsigned long long blockId;
        unsigned long long recordNumberInBlock;

        auto records = vector<Record*>();
        auto schema = GetSchema();
        auto column = schema->GetColumn(columnId);

        // try to binary search m_File
        auto evalFunc = [](int eval) {
            if (eval == 0) { // equal
                return true;
            }
            return false;
        };
        auto currentRecord = BinarySearch(data, evalFunc, accessedBlocks);
        if (currentRecord != nullptr)
        {
            // if data was found, it might not be the first instance
            // MovePrev until finding first
            while (MovePrev(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
            {
                m_LastQueryBlockAccessCount += accessedBlocks;
                auto value = schema->GetValue(currentRecord->GetData(), columnId);

                if (!Column::Equals(column, value, data))
                {
                    break;
                }
            }

            // at this point we should be at the first record 
            // MoveNext until record is different than data
            auto enteredRange = false;
            while (MoveNext(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
            {
                m_LastQueryBlockAccessCount += accessedBlocks;
                auto value = schema->GetValue(currentRecord->GetData(), columnId);

                // the value of records found here should all be equal to data
                // when records value != data, we left the range and should stop adding data to the return vector
                // however, we can move too far back when searching for first instance
                if (enteredRange && !Column::Equals(column, value, data)) { // if we have started to see records with value == data AND the current value is different
                    // we have left the range and should stop
                    break;
                }

                if (Column::Equals(column, value, data))
                {
                    enteredRange = true; // found something equal to data, we are inside the range
                    auto newRecord = new Record(schema);
                    memcpy(newRecord->GetData()->data(), currentRecord->GetData()->data(), schema->GetSize());
                    records.push_back(newRecord);
                }
            }
        }
        // at this point, the data is not present in the main file OR we found all records equal to data in the main file OR we stopped somewhere in the extension file
        // there might still be records in the range in the extension file

        auto mainFileblocksCount = m_File->GetHead()->GetBlocksCount();
        if (m_NextReadBlockNumber < mainFileblocksCount || currentRecord == nullptr) // we stopped somewhere in the main file
        {
            // we found all records equal to data in the main file
            // move to the star of extension file and continue
            MoveToExtension();
            currentRecord = new Record(schema);
        }


        // m_NextBlockNumber is somewhere in the extension file
        // linear search extension file continuing from that point
        while (MoveNext(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
        {
            m_LastQueryBlockAccessCount += accessedBlocks;
            auto value = schema->GetValue(currentRecord->GetData(), columnId);

            if (Column::Equals(column, value, data))
            {
                auto newRecord = new Record(schema);
                memcpy(newRecord->GetData()->data(), currentRecord->GetData()->data(), schema->GetSize());
                records.push_back(newRecord);
            }
        }
        return records;
    }
    // if the file is not ordered by id, linear search everything
    return BaseRecordManager::SelectWhereEquals(columnId, data);
}

void OrderedRecordManager::Delete(unsigned long long id)
{
    auto schema = GetSchema();
    auto recordSize = schema->GetSize();
    if (m_DeletedRecords * recordSize == m_MaxPercentEmptySpace * GetSize())
    {
        Compress();
    }
    auto record = Select(id);
    auto blockId = m_NextReadBlockNumber - 1;
    auto recordNumberInBlock = m_ReadBlock->GetPosition() - 1;

    if (m_OrderedByColumnId == 0 && 
        m_NextReadBlockNumber < m_File->GetHead()->GetBlocksCount()) // did a binary search and found the record in main file
    {
        blockId = m_NextReadBlockNumber;
        recordNumberInBlock = m_ReadBlock->GetPosition() - 1;
    }
    DeleteInternal(blockId, recordNumberInBlock);
}

int OrderedRecordManager::DeleteWhereEquals(unsigned int columnId, span<unsigned char> data)
{
    auto schema = GetSchema();
    auto recordSize = schema->GetSize();
    if (m_DeletedRecords * recordSize == m_MaxPercentEmptySpace * GetSize())
    {
        Compress();
    }

    // if the file is ordered by the column we are selecting
    if (columnId == m_OrderedByColumnId) {

        m_LastQueryBlockAccessCount = 0;
        unsigned long long accessedBlocks = 0;

        unsigned long long blockId;
        unsigned long long recordNumberInBlock;

        int removedCount = 0;
        auto column = schema->GetColumn(columnId);

        // try to binary search m_File
        auto evalFunc = [](int eval) {
            if (eval == 0) { // equal
                return true;
            }
            return false;
        };
        auto currentRecord = BinarySearch(data, evalFunc, accessedBlocks);
        if (currentRecord != nullptr)
        {
            // if data was found, it might not be the first instance
            // MovePrev until finding first
            while (MovePrev(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
            {
                m_LastQueryBlockAccessCount += accessedBlocks;
                auto value = schema->GetValue(currentRecord->GetData(), columnId);

                if (!Column::Equals(column, value, data))
                {
                    break;
                }
            }

            // at this point we should be at the first record 
            // MoveNext until record is different than data
            auto enteredRange = false;
            while (MoveNext(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
            {
                m_LastQueryBlockAccessCount += accessedBlocks;
                auto value = schema->GetValue(currentRecord->GetData(), columnId);

                // the value of records found here should all be equal to data
                // when records value != data, we left the range and should stop adding data to the return vector
                // however, we can move too far back when searching for first instance
                if (enteredRange && !Column::Equals(column, value, data)) { // if we have started to see records with value == data AND the current value is different
                    // we have left the range and should stop
                    break;
                }

                if (Column::Equals(column, value, data))
                {
                    enteredRange = true; // found something equal to data, we are inside the range
                    removedCount++;
                    DeleteInternal(blockId, recordNumberInBlock);
                }
            }
        }
        // at this point, the data is not present in the main file OR we found all records equal to data in the main file OR we stopped somewhere in the extension file
        // there might still be records in the range in the extension file

        auto mainFileblocksCount = m_File->GetHead()->GetBlocksCount();
        if (m_NextReadBlockNumber < mainFileblocksCount || currentRecord == nullptr) // we stopped somewhere in the main file
        {
            // we found all records equal to data in the main file
            // move to the star of extension file and continue
            MoveToExtension();
            currentRecord = new Record(schema);
        }

        // m_NextBlockNumber is somewhere in the extension file
        // linear search extension file continuing from that point
        while (MoveNext(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
        {
            m_LastQueryBlockAccessCount += accessedBlocks;
            auto value = schema->GetValue(currentRecord->GetData(), columnId);

            if (Column::Equals(column, value, data))
            {
                removedCount++;
                DeleteInternal(blockId, recordNumberInBlock);
            }
        }
        return removedCount;
    }
    // if the file is not ordered by id, linear search everything
    return BaseRecordManager::DeleteWhereEquals(columnId, data);

}

bool OrderedRecordManager::MovePrev(Record* record, unsigned long long& accessedBlocks, unsigned long long& blockId, unsigned long long& recordNumberInBlock)
{
    auto mainBlocksCount = m_File->GetHead()->GetBlocksCount();
    auto blocksCount = mainBlocksCount + m_ExtensionFile->GetHead()->GetBlocksCount();
    auto intialBlock = m_NextReadBlockNumber;

    if (m_NextReadBlockNumber == blocksCount - 1 || m_NextReadBlockNumber == mainBlocksCount)
    {
        // did not start to read before or at start of extension file
        if (blocksCount > 0)
        {
            ReadPrevBlock();
        }
    }

    bool returnVal = GetPrevRecordInFile(record);

    if (returnVal)
    {
        recordNumberInBlock = m_ReadBlock->GetPosition() + 1;
        blockId = m_NextReadBlockNumber + 1;
    }
    accessedBlocks = intialBlock - m_NextReadBlockNumber;
    return returnVal;
}

void OrderedRecordManager::DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock)
{
    auto mainFileHead = m_File->GetHead();
    auto extensionFileHead = m_ExtensionFile->GetHead();
    auto blocksCount = mainFileHead->GetBlocksCount() + extensionFileHead->GetBlocksCount();
    if (blockNumber == blocksCount)
    {
        // Record to remove was not written to file
        // Remove it from m_WriteBlock

        auto success = m_WriteBlock->RemoveRecordAt(recordNumberInBlock);

        Assert(success, "Block was invalid");
    }

    m_DeletedRecords++;

    ReadBlock(blockNumber);
    span<unsigned char> recordToRemove;
    if (!m_ReadBlock->GetRecordSpan(recordNumberInBlock, &recordToRemove))
    {
        Assert(false, "Oh no!");
        return;
    }

    // Mark as removed
    auto orderedRecord = Record::Cast<OrderedRecord>(&recordToRemove);
    orderedRecord->Id = -1;
    m_File->WriteBlock(m_ReadBlock, blockNumber);
}

bool OrderedRecordManager::GetNextRecordInFile(Record* record)
{
    auto recordData = record->GetData();
    auto blocksInFile = m_File->GetHead()->GetBlocksCount() + m_ExtensionFile->GetHead()->GetBlocksCount();
    while (blocksInFile > 0 && m_NextReadBlockNumber < blocksInFile - 1)
    {
        while (m_ReadBlock->GetRecord(recordData))
        {
            auto orderedRecord = record->As<OrderedRecord>();
            if (orderedRecord->Id != -1)
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

bool OrderedRecordManager::GetPrevRecordInFile(Record* record)
{
    auto recordData = record->GetData();
    auto blocksInFile = m_File->GetHead()->GetBlocksCount() + m_ExtensionFile->GetHead()->GetBlocksCount();

    while (blocksInFile > 0 && m_NextReadBlockNumber > -1)
    {
        while (m_ReadBlock->GetRecordBack(recordData))
        {
            auto orderedRecord = record->As<OrderedRecord>();
            if (orderedRecord->Id != -1)
            {
                return true;
            }
        }
        ReadPrevBlock();
    }

    // Not found in the file
    // Look in the write buffer
    if (m_WriteBlock->GetRecordsCount() > 0)
    {
        m_WriteBlock->MoveToEnd();
        while (m_WriteBlock->GetPosition() > -1)
        {
            if (m_WriteBlock->GetRecordBack(recordData))
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

void OrderedRecordManager::MoveToExtension()
{
    m_NextReadBlockNumber = m_File->GetHead()->GetBlocksCount();
}

void OrderedRecordManager::ReadPrevBlock()
{
    m_ReadBlock->Clear();
    auto mainFileBlockCount = m_File->GetHead()->GetBlocksCount();
    if (m_NextReadBlockNumber < mainFileBlockCount) {
        m_File->GetBlock(m_NextReadBlockNumber, m_ReadBlock);
    }
    else {
        auto blockId = m_NextReadBlockNumber - mainFileBlockCount;
        m_ExtensionFile->GetBlock(blockId, m_ReadBlock);
    }
    m_ReadBlock->MoveToEnd();
    m_NextReadBlockNumber--;
}

FileHead* OrderedRecordManager::CreateNewFileHead(Schema* schema)
{
    return new OrderedFileHead(schema);
}

FileWrapper<FileHead>* OrderedRecordManager::GetFile()
{
    return (FileWrapper<FileHead>*)m_File;
}

void OrderedRecordManager::ReadBlock(unsigned long long blockId)
{
    m_ReadBlock->Clear();
    auto mainFileBlockCount = m_File->GetHead()->GetBlocksCount();
    if (blockId < mainFileBlockCount) {
        m_File->GetBlock(blockId, m_ReadBlock);
    }
    else {
        auto correctedBlockId = m_NextReadBlockNumber - mainFileBlockCount;
        m_ExtensionFile->GetBlock(correctedBlockId, m_ReadBlock);
    }
    m_ReadBlock->MoveToStart();
}

bool GetRecord(Block *block, Record *record)
{
    auto recordData = record->GetData();
    return block->GetRecord(recordData);
}

bool GetRecord(Block *block, Record *record, unsigned int offset)
{
    auto recordData = record->GetData();
    return block->MoveToAndGetRecord(offset, recordData);
}

function<bool(Record, Record)> MakeComparer(Schema* schema, unsigned int columnId)
{
    return [schema, columnId](Record a, Record b) {


        // if both records deleted, keep order
        if (a.getId() < 0 && b.getId() < 0) return true;

        // if first record deleted, swap records
        if (a.getId() < 0) return false;

        // if second record deleted, keep order
        if (b.getId() < 0) return true;

        auto valueA = schema->GetValue(a.GetData(), columnId);
        auto valueB = schema->GetValue(b.GetData(), columnId);
        auto column = schema->GetColumn(columnId);

        // if both record valid, swap records if first is larger
        return Column::Compare(column, valueA, valueB) < 0;
    };
}

void OrderedRecordManager::Reorder()
{
    if (DEBUG) 
    {
        MemoryReorder();
        return;
    }

    auto schema = GetSchema();
    Record record = Record(schema);
    auto recordSize = schema->GetSize();

    auto comparer = MakeComparer(schema, m_OrderedByColumnId);

    // TODO
    // insert blocks from m_ExtensionFile into m_File, reordering

    // Sort blocks from m_ExtensionFile and insert them into m_File
    Block *block = m_ExtensionFile->CreateBlock();
    auto blocksCount = m_ExtensionFile->GetHead()->GetBlocksCount();

    unsigned long long blockId;
    for (blockId = 0; blockId < blocksCount; blockId++)
    {
        auto blockRecords = vector<Record>();
        block->Clear();
        m_ExtensionFile->GetBlock(blockId, block);
        block->MoveToStart();
        while (GetRecord(block, &record))
        {
            blockRecords.push_back(record);
        }

        sort(blockRecords.begin(), blockRecords.end(), comparer);

        // Write sorted data into block and add block into m_File
        block->Clear();
        for (auto record : blockRecords)
        {
            auto recordData = record.GetData();
            block->Append(*recordData);
        }
        m_File->AddBlock(block);
    }

    // Sort all blocks in m_File and write to m_ExtensionFile
    auto inputBuffer = vector<Record>();
    auto outputBuffer = vector<Record>();

    m_ExtensionFile->SeekHead();

    unsigned int recordPointer = 0;
    while (recordPointer < m_RecordsPerBlock)
    {
        // Get one record from each block
        blocksCount = m_File->GetHead()->GetBlocksCount();
        for (blockId = 0; blockId < blocksCount; blockId++)
        {
            block->Clear();
            m_File->GetBlock(blockId, block);
            if (!GetRecord(block, &record, recordPointer))
                continue;
            inputBuffer.push_back(record);
        }

        // Sort input buffer and copy into output buffer
        sort(inputBuffer.begin(), inputBuffer.end(), comparer);
        outputBuffer.insert(outputBuffer.end(), inputBuffer.begin(), inputBuffer.end());

        if (outputBuffer.size() >= m_RecordsPerBlock)
        {
            // Copy output buffer into m_ExtensionFile
            auto outBlock = m_ExtensionFile->CreateBlock();
            for (int i = 0; i < m_RecordsPerBlock; i++)
            {
                record = outputBuffer[i];
                auto recordData = record.GetData();
                outBlock->Append(*recordData);
            }
            m_ExtensionFile->AddBlock(outBlock);
            outputBuffer = vector(outputBuffer.begin() + m_RecordsPerBlock, outputBuffer.end());
        }
        inputBuffer = vector<Record>();
        recordPointer++;
    }

    SwitchFilesSoft();
  
    // let n be the amount of blocks to merge
    // Read the first RecordsPerBlock / n+1 records of each sorted block into n input buffers in main memory and allocate the remaining block space for an output buffer.
    // CREATE N INPUT BUFFERS
    // CREATE 1 OUTPUT BUFFER
    // READ ith BLOCK FROM MAIN FILE
    // WRITE RecordsPerBlock / n+1 RECORDS INTO ith INPUT BUFFER
    // MERGE SORT INPUT BUFFERS INTO OUTPUT BUFFER
    // WHEN OUTPUT BUFFER FULL, WRITE TO TMP FILE
    // WHEN ith INPUT BUFFER FULL, READ FROM ith BLOCK
    // WHEN MAIN FILE IS EMPTY, COPY TMP FILE INTO FILE

    // Perform a merge and store the result in the output buffer. Whenever the output buffer fills, write it to the final sorted file and empty it. Whenever any of the input buffers empties, fill it with the next RecordsPerBlock / n+1 records of its associated sorted block until no more data from the chunk is available. This is the key step that makes external merge sort work externally—because the merge algorithm only makes one pass sequentially through each of the blocks, each block does not have to be loaded completely; rather, sequential parts of the chunk can be loaded as needed.
}

void OrderedRecordManager::MemoryReorder() 
{
    auto records = vector<Record>();
    auto schema = GetSchema();
    auto record = Record(schema);
    auto blocksCount = m_File->GetHead()->GetBlocksCount();

    // Read all records from m_File
    unsigned long long blockId;
    for (blockId = 0; blockId < blocksCount; blockId++)
    {
        m_ReadBlock->Clear();
        m_File->GetBlock(blockId, m_ReadBlock);
        m_ReadBlock->MoveToStart();
        while (GetRecord(m_ReadBlock, &record))
        {
            records.push_back(record);
        }
    }

    blocksCount = m_ExtensionFile->GetHead()->GetBlocksCount();
    // Read all records from m_ExtensionFile
    for (blockId = 0; blockId < blocksCount; blockId++)
    {
        m_ReadBlock->Clear();
        m_ExtensionFile->GetBlock(blockId, m_ReadBlock);
        m_ReadBlock->MoveToStart();
        while (GetRecord(m_ReadBlock, &record))
        {
            records.push_back(record);
        }
    }
    m_ExtensionFile->SeekHead();

    // Sort all records
    auto comparer = MakeComparer(schema, m_OrderedByColumnId);
    sort(records.begin(), records.end(), comparer);

    // Write back to m_File
    m_File->SeekHead();
    m_WriteBlock->Clear();
    for (auto &record : records) {
        auto recordData = record.GetData();
        m_WriteBlock->Append(*recordData);

        auto recordsCount = m_WriteBlock->GetRecordsCount();
        if (recordsCount == m_RecordsPerBlock) {
            m_File->AddBlock(m_WriteBlock);
            m_WriteBlock->Clear();
        }
    }
}

void OrderedRecordManager::Compress()
{
    Reorder(); // records with id -1 will be pushed to the end
}

void OrderedRecordManager::SwitchFilesSoft()
{
    auto mainFilePath = m_File->GetPath();
    auto mainFileHead = m_File->GetHead();

    auto extensionFilePath = m_ExtensionFile->GetPath();
    auto extensionFileHead = m_ExtensionFile->GetHead();

    m_File->UpdatePath(extensionFilePath, extensionFileHead);

    m_ExtensionFile->UpdatePath(mainFilePath, mainFileHead);
    m_ExtensionFile->SeekHead();

    m_UsingExtensionAsMain = !m_UsingExtensionAsMain;
}

void OrderedRecordManager::SwitchFilesHard()
{
    // TODO
    // copy content of extension file into main file

}

Record* OrderedRecordManager::BinarySearch(span<unsigned char> target, EvalFunctionType evalFunc, unsigned long long& accessedBlocks)
{
    auto recordCount = m_File->GetHead()->GetBlocksCount() * m_RecordsPerBlock;
    auto schema = GetSchema();
    auto column = schema->GetColumn(m_OrderedByColumnId);

    auto currentRecord = new Record(schema);

    auto min = 0ull;
    auto max = recordCount - 1;
    unsigned long long pivot;
    while (min <= max) {
        pivot = (min + max) / 2;
        m_NextReadBlockNumber = pivot / m_RecordsPerBlock;
        auto recordOffset = pivot % m_RecordsPerBlock;

        ReadBlock(m_NextReadBlockNumber);
        accessedBlocks++;
        GetRecord(m_ReadBlock, currentRecord, recordOffset);
        auto value = schema->GetValue(currentRecord->GetData(), m_OrderedByColumnId);

        auto eval = Column::Compare(column, value, target);


        if (evalFunc(eval)) {
            return currentRecord;
        }
        else if (eval < 0)  // value < target
        {
            min = pivot + 1;
        }
        else // value > target
        { 
            max = pivot - 1;
        }
    }

    return nullptr;
}