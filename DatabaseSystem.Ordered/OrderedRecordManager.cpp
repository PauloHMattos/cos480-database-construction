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
    m_MaxPercentEmptySpace(0.2)
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
    m_OrderedByColumnId = m_File->GetHead()->OrderedByColumnId;
    auto extension_path = path.append(".extension");
    m_ExtensionFile->Open(extension_path, (OrderedFileHead*)CreateNewFileHead(nullptr));
}

void OrderedRecordManager::Close()
{
    if (m_ExtensionFile->GetHead()->GetBlocksCount() > 0) 
    {
        Reorganize();
    }
    m_File->Close();
    m_ExtensionFile->Close();
}


unsigned long long OrderedRecordManager::GetBlocksCount()
{
    return m_File->GetHead()->GetBlocksCount() + m_ExtensionFile->GetHead()->GetBlocksCount();
}

void OrderedRecordManager::Insert(Record record)
{
    ClearAccessCount();
    auto orderedRecord = record.As<OrderedRecord>();
    orderedRecord->Id = m_ExtensionFile->GetHead()->NextId;
    m_ExtensionFile->GetHead()
        ->NextId += 1;

    auto recordsCount = m_WriteBlock->GetRecordsCount();
    if (recordsCount == m_RecordsPerBlock)
    {
        AddToExtension(m_WriteBlock);
        m_WriteBlock->Clear();

        auto blocksCount = m_ExtensionFile->GetHead()->GetBlocksCount();
        if (blocksCount == m_MaxExtensionFileSize)
        {
            // if Extension File has reached max size, call Reorder
            Reorganize();
        }
    }

    auto recordData = record.GetData();
    m_WriteBlock->Append(*recordData);
}

Record *OrderedRecordManager::Select(unsigned long long id)
{
    // if the file is ordered by the id
    if (m_OrderedByColumnId == 0) {
        
        ClearAccessCount();
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

        if (currentRecord != nullptr)
            return currentRecord;
        
        // if not found, try to linear search m_ExtensionFile
        currentRecord = new Record(GetSchema());

        MoveToExtension();
        while (MoveNext(currentRecord, accessedBlocks, blockId, recordNumberInBlock))
        {
            if (currentRecord->getId() == id) {
                return currentRecord;
            }
        }
        
        return nullptr;
    }
    // if the file is not ordered by id, linear search everything
    return BaseRecordManager::Select(id);
}

vector<Record *> OrderedRecordManager::SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max)
{
    // if the file is ordered by the column we are selecting
    if (columnId == m_OrderedByColumnId) {
        ClearAccessCount();
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

        ClearAccessCount();
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
    if (m_DeletedRecords * recordSize >= m_MaxPercentEmptySpace * GetSize())
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

        ClearAccessCount();
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

    ReadBlock(m_ReadBlock, blockNumber);
    span<unsigned char> recordToRemove;
    if (!m_ReadBlock->GetRecordSpan(recordNumberInBlock, &recordToRemove))
    {
        Assert(false, "Oh no!");
        return;
    }

    // Mark as removed
    auto orderedRecord = Record::Cast<OrderedRecord>(&recordToRemove);
    orderedRecord->Id = -1;
    WriteBlock(m_ReadBlock, blockNumber);
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
    ReadNextBlock();
}

void OrderedRecordManager::AddToExtension(Block* block)
{
    m_ExtensionFile->AddBlock(block);
    m_LastQueryBlockWriteAccessCount++;
}

void OrderedRecordManager::WriteToExtension(Block* block, unsigned long long blockNumber)
{
    m_ExtensionFile->WriteBlock(block, blockNumber);
    m_LastQueryBlockWriteAccessCount++;
}

void OrderedRecordManager::GetBlockFromExtension(Block* block, unsigned long long blockNumber)
{
    block->Clear();
    m_ExtensionFile->GetBlock(blockNumber, block);
    block->MoveToStart();
    m_LastQueryBlockReadAccessCount++;
}

void OrderedRecordManager::GetBlockFromMainFile(Block* block, unsigned long long blockNumber)
{
    block->Clear();
    m_File->GetBlock(blockNumber, block);
    block->MoveToStart();
    m_LastQueryBlockReadAccessCount++;
}

void OrderedRecordManager::ReadPrevBlock()
{
    ReadBlock(m_ReadBlock, m_NextReadBlockNumber);
    m_ReadBlock->MoveToEnd();
    m_NextReadBlockNumber--;
}

FileHead* OrderedRecordManager::CreateNewFileHead(Schema* schema)
{
    auto fileHead =  new OrderedFileHead(schema);
    fileHead->OrderedByColumnId = m_OrderedByColumnId;
    return fileHead;
}

FileWrapper<FileHead>* OrderedRecordManager::GetFile()
{
    return (FileWrapper<FileHead>*)m_File;
}

void OrderedRecordManager::ReadBlock(Block* block, unsigned long long blockId)
{
    block->Clear();
    auto mainFileBlockCount = m_File->GetHead()->GetBlocksCount();
    if (blockId < mainFileBlockCount) {
        GetBlockFromMainFile(block, blockId);
    }
    else {
        auto correctedBlockId = m_NextReadBlockNumber - mainFileBlockCount;
        GetBlockFromExtension(block, correctedBlockId);
    }
    block->MoveToStart();
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

void OrderedRecordManager::Reorganize()
{
    if (DEBUG) 
    {
        MemoryReorder();
        return;
    }

    unsigned long long accessedBlocks = 0;

    auto schema = GetSchema();
    Record record = Record(schema);
    auto recordSize = schema->GetSize();

    auto comparer = MakeComparer(schema, m_OrderedByColumnId);

    // Sort blocks from m_ExtensionFile and insert them into m_File
    auto blocksCount = m_ExtensionFile->GetHead()->GetBlocksCount();

    unsigned long long blockId;
    for (blockId = 0; blockId < blocksCount; blockId++)
    {
        auto blockRecords = vector<Record>();
        GetBlockFromExtension(m_ReadBlock, blockId);
        while (GetRecord(m_ReadBlock, &record))
        {
            blockRecords.push_back(record);
        }

        sort(blockRecords.begin(), blockRecords.end(), comparer);

        // Write sorted data into block and add block into m_File
        m_WriteBlock->Clear();
        for (auto record : blockRecords)
        {
            auto recordData = record.GetData();
            m_WriteBlock->Append(*recordData);
        }
        AddBlock(m_WriteBlock);
    }
    m_ExtensionFile->SeekHead();

    // Split m_File into partitions of size 1 block
    blocksCount = m_File->GetHead()->GetBlocksCount();
    auto partitions = vector<Partition>();
    Partition fullFile;
    fullFile.firstBlock = 0;
    fullFile.blocksCount = blocksCount;
    fullFile.level = 0;
    partitions.push_back(fullFile);
    while (partitions.size() < blocksCount)
    {
        partitions = SplitPartitions(partitions);
    }

    // Merge partitions into m_File, reordering
    unsigned long deepestLevel = ceil(log2(blocksCount));
    while (partitions.size() > 1)
    {
        partitions = MergePartitions(partitions, deepestLevel);
        deepestLevel--;
    }

    if (deepestLevel != 0)
    {
        Assert(false, "After full merge the full file partition should have level 0");
    }
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
        ReadBlock(m_ReadBlock, blockId);
        while (GetRecord(m_ReadBlock, &record))
        {
            records.push_back(record);
        }
    }

    blocksCount = m_ExtensionFile->GetHead()->GetBlocksCount();
    // Read all records from m_ExtensionFile
    for (blockId = 0; blockId < blocksCount; blockId++)
    {
        GetBlockFromExtension(m_ReadBlock, blockId);
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
            AddBlock(m_WriteBlock);
            m_WriteBlock->Clear();
        }
    }
}

void OrderedRecordManager::Compress()
{
    Reorganize(); // records with id -1 will be pushed to the end
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

        ReadBlock(m_ReadBlock, m_NextReadBlockNumber);
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

vector<Partition> OrderedRecordManager::SplitPartitions(vector<Partition> partitions)
{
    auto splitPartitions = vector<Partition>();
    // [                           (0,10,0)          ]
    // [           (0,5,1),                    (5,5,1)   ] 
    // [   (0,3,2),         (3,2,2),        (5,3,2),   (8,2,2) ]
    // [(0,2,3),(2,1,3),(3,1,3),(4,1,3),(5,2,3)(7,1,3), (8,1,3), (9,1,3)]
    // [(0,1,4),(1,1,4)(2,1,3),(3,1,3),(4,1,3),(5,1,4),(6,1,4),(7,1,3),(8,1,3),(9,1,3)]
    // [(0,2,3),(2,1,3),(3,1,3),(4,1,3),(5,1,4),(6,1,4),(7,1,3),(8,1,3),(9,1,3)]
    // [(0,2,3),(2,1,3),(3,1,3),(4,1,3),(5,2,3),(7,1,3),(8,1,3),(9,1,3)]
    // [(0,3,2),(3,1,3),(4,1,3),(5,2,3),(7,1,3),(8,1,3),(9,1,3)]
    // [(0,3,2),(3,2,2),(5,2,3),(7,1,3),(8,1,3),(9,1,3)]
    // [(0,3,2),(3,2,2),(5,3,2),(8,1,3),(9,1,3)]
    // [(0,3,2),(3,2,2),(5,3,2),(8,2,2)]
    // [(0,5,1),(5,3,2),(8,2,2)]
    // [(0,5,1),(5,5,1)]
    // [(0,10,0)]
    for (auto p : partitions)
    {
        if (p.blocksCount == 1)
        {
            splitPartitions.push_back(p);
            continue;
        }

        auto splitP = Split(p);

        splitPartitions.push_back(splitP[0]);
        splitPartitions.push_back(splitP[1]);
    }

    return splitPartitions;
}

vector<Partition> OrderedRecordManager::Split(Partition p)
{
    auto splitPartition = vector<Partition>();
    auto newBlocksCount = p.blocksCount / 2;
    Partition p1;
    Partition p2;

    p1.firstBlock = p.firstBlock;
    p1.blocksCount = newBlocksCount;
    p1.level = p.level + 1;

    p2.firstBlock = p.firstBlock + newBlocksCount;
    p2.blocksCount = newBlocksCount;
    p2.level = p.level + 1;

    if (p.blocksCount % 2 != 0) // odd number of blocks in partition
    {
        p1.blocksCount += 1;
        p2.firstBlock += 1;
    }

    splitPartition.push_back(p1);
    splitPartition.push_back(p2);

    return splitPartition;
}

vector<Partition> OrderedRecordManager::MergePartitions(vector<Partition> partitions, unsigned long deepestLevel)
{
    auto mergedPartitions = vector<Partition>();
    auto i = 0;
    while(i < partitions.size())
    {
        auto p1 = partitions[i];

        if (p1.level < deepestLevel)
        {
            mergedPartitions.push_back(p1);
            i++;
            continue;
        }
        else if (p1.level == deepestLevel)
        {
            auto p2 = partitions[i + 1];

            auto p = Merge(p1, p2);
            mergedPartitions.push_back(p);
            i += 2;
        }
        else
        {
            Assert(false, "Should not have partitions deeper than the deepest level");
        }
    }

    return mergedPartitions;
}

Partition OrderedRecordManager::Merge(Partition p1, Partition p2)
{
    if (p1.level != p2.level)
    {
        Assert(false, "Should not merge partitions of different levels");
    }
    auto schema = GetSchema();
    auto comparer = MakeComparer(schema, m_OrderedByColumnId);

    auto recordPointer1 = p1.firstBlock * m_RecordsPerBlock;
    auto lastRecordP1 = recordPointer1 + p1.blocksCount*m_RecordsPerBlock - 1;

    auto recordPointer2 = p2.firstBlock * m_RecordsPerBlock;
    auto lastRecordP2 = recordPointer2 + p2.blocksCount*m_RecordsPerBlock - 1;

    auto readBlock1 = m_File->CreateBlock();
    auto record1 = Record(schema);

    auto readBlock2 = m_File->CreateBlock();
    auto record2 = Record(schema);

    unsigned long long writeBlockPointer = min(recordPointer1, recordPointer2) / m_RecordsPerBlock;

    m_WriteBlock->Clear();
    while (recordPointer1 <= lastRecordP1 && recordPointer2 <= lastRecordP2)
    {
        auto blockPointer1 = recordPointer1 / m_RecordsPerBlock;
        auto recordOffset1 = recordPointer1 % m_RecordsPerBlock;

        GetBlockFromMainFile(readBlock1, blockPointer1);
        GetRecord(readBlock1, &record1, recordOffset1);


        auto blockPointer2 = recordPointer2 / m_RecordsPerBlock;
        auto recordOffset2 = recordPointer2 % m_RecordsPerBlock;

        GetBlockFromMainFile(readBlock2, blockPointer2);
        GetRecord(readBlock2, &record2, recordOffset2);

        if (comparer(record1, record2)) // record 1 should come before record 2
        {
            auto recordData = record1.GetData();
            m_WriteBlock->Append(*recordData);
            recordPointer1++;
        }
        else // record 2 should come before record 1
        {
            auto recordData = record2.GetData();
            m_WriteBlock->Append(*recordData);
            recordPointer2++;
        }

        auto recordsInWriteBlock = m_WriteBlock->GetRecordsCount();
        if (recordsInWriteBlock == m_RecordsPerBlock)
        {
            // write block to m_File at writeBlockPointer
            WriteBlock(m_WriteBlock, writeBlockPointer);
            m_WriteBlock->Clear();
            writeBlockPointer++;
        }
    }

    // one of the partitions is over, copy everything in the remaining partition into the file
    auto remainingRecordPointer = (recordPointer1 > lastRecordP1) ? recordPointer2 : recordPointer1;
    auto lastRecord = (remainingRecordPointer == recordPointer2) ? lastRecordP2 : lastRecordP1;
    while (remainingRecordPointer <= lastRecord)
    {
        auto blockPointer = remainingRecordPointer / m_RecordsPerBlock;
        auto recordOffset = remainingRecordPointer % m_RecordsPerBlock;

        GetBlockFromMainFile(readBlock1, blockPointer);
        GetRecord(readBlock1, &record1, recordOffset);

        auto recordData = record1.GetData();
        m_WriteBlock->Append(*recordData);
        remainingRecordPointer++;

        auto recordsInWriteBlock = m_WriteBlock->GetRecordsCount();
        if (recordsInWriteBlock == m_RecordsPerBlock)
        {
            // write block to m_File at writeBlockPointer
            WriteBlock(m_WriteBlock, writeBlockPointer);
            m_WriteBlock->Clear();
            writeBlockPointer++;
        }
    }

    Partition mergedPartition;
    mergedPartition.firstBlock = min(p1.firstBlock, p2.firstBlock);
    mergedPartition.blocksCount = p1.blocksCount + p2.blocksCount;
    mergedPartition.level = p1.level - 1;

    return mergedPartition;
}
