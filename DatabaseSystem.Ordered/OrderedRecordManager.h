#pragma once
#include "../DatabaseSystem.Core/BaseRecordManager.h"
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/File.h"
#include "../DatabaseSystem.Core/Block.h"
#include "OrderedFileHead.h"

typedef bool (*EvalFunctionType)(int);

typedef struct partition
{
    unsigned long long firstBlock;
    unsigned long long blocksCount;
    unsigned long level;
} Partition;

/*
  Ordered, ou arquivo sequencial ordenado, com registros de tamanho fixo,
  inserção de novos em um arquivo de extensão e posterior reordenação, e remoção baseada em marcação dos registros removidos,
  que deverão ser reorganizados posteriormente para compressão do arquivo.
*/
class OrderedRecordManager : public BaseRecordManager
{
public:
    OrderedRecordManager(size_t blockSize);
    OrderedRecordManager(size_t blockSize, unsigned int orderedByColumnId);
    virtual void Create(string path, Schema* schema) override;
    virtual void Open(string path) override;
    virtual void Close() override;

    // Inherited via BaseRecordManager
    virtual void Insert(Record record) override;
    virtual Record* Select(unsigned long long id) override;
    virtual vector<Record*> SelectWhereBetween(unsigned int columnId, span<unsigned char> min, span<unsigned char> max) override;
    virtual vector<Record*> SelectWhereEquals(unsigned int columnId, span<unsigned char> data) override;
    virtual void Delete(unsigned long long id) override;
    virtual int DeleteWhereEquals(unsigned int columnId, span<unsigned char> data) override;

    const bool DEBUG = false;

protected:
    // Inherited via BaseRecordManager
    virtual FileHead* CreateNewFileHead(Schema* schema) override;
    virtual FileWrapper<FileHead>* GetFile() override;
    virtual unsigned long long GetBlocksCount() override;
    virtual bool ReadBlock(Block* block, unsigned long long blockId) override;
    void MoveToExtension();
    bool MovePrev(Record* record, unsigned long long& accessedBlocks, unsigned long long& blockId, unsigned long long& recordNumberInBlock);
    virtual void DeleteInternal(unsigned long long recordId, unsigned long long blockNumber, unsigned long long recordNumberInBlock);
    bool GetNextRecordInFile(Record* record);
    bool GetPrevRecordInFile(Record* record);
    virtual void Reorganize() override;  // inserts records from extension file into main file, reordering

private:
    FileWrapper<OrderedFileHead>* m_File;
    FileWrapper<OrderedFileHead>* m_ExtensionFile;
    unsigned int m_OrderedByColumnId;
    unsigned long long m_MaxExtensionFileSize;
    unsigned long long m_DeletedRecords;
    float m_MaxPercentEmptySpace;

    void AddToExtension(Block* block);
    void WriteToExtension(Block* block, unsigned long long blockNumber);
    bool GetBlockFromExtension(Block* block, unsigned long long blockNumber);
    bool GetBlockFromMainFile(Block* block, unsigned long long blockNumber);
    void ReadPrevBlock();
    void MemoryReorder(); // reads all records from main file and extension file into memory and reorders, for debugging
    void ReorganizeInternal();  // inserts records from extension file into main file, reordering
    Record* BinarySearch(span<unsigned char> target, EvalFunctionType evalFunc, unsigned long long& accessedBlocks);
    vector<Partition> SplitPartitions(vector<Partition> partitons);
    vector<Partition> Split(Partition p);
    vector<Partition> MergePartitions(vector<Partition> partitions, unsigned long deepestLevel);
    Partition Merge(Partition p1, Partition p2);

    struct OrderedRecord
    {
        unsigned long long Id;
    };
};