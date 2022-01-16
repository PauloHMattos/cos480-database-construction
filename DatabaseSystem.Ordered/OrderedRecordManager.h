#pragma once
#include "../DatabaseSystem.Core/BaseRecordManager.h"
#include "../DatabaseSystem.Core/Record.h"
#include "../DatabaseSystem.Core/File.h"
#include "../DatabaseSystem.Core/Block.h"
#include "OrderedFileHead.h"

typedef bool (*EvalFunctionType)(int);

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

    const bool DEBUG = true;

protected:
    // Inherited via BaseRecordManager
    virtual FileHead* CreateNewFileHead(Schema* schema) override;
    virtual FileWrapper<FileHead>* GetFile() override;
    virtual void ReadBlock(unsigned long long blockId) override;
    void MoveToExtension();
    bool MovePrev(Record* record, unsigned long long& accessedBlocks, unsigned long long& blockId, unsigned long long& recordNumberInBlock);
    virtual void DeleteInternal(unsigned long long blockNumber, unsigned long long recordNumberInBlock);
    bool GetNextRecordInFile(Record* record);
    bool GetPrevRecordInFile(Record* record);

private:
    FileWrapper<OrderedFileHead>* m_File;
    FileWrapper<OrderedFileHead>* m_ExtensionFile;
    unsigned int m_OrderedByColumnId;
    unsigned long long m_MaxExtensionFileSize;
    unsigned long long m_DeletedRecords;
    float m_MaxPercentEmptySpace;
    bool m_UsingExtensionAsMain;

    void ReadPrevBlock();
    void Reorder();  // inserts records from extension file into main file, reordering
    void MemoryReorder(); // reads all records from main file and extension file into memory and reorders, for debugging
    void Compress(); // removes records marked as deleted from the file, compressing the empty space
    void SwitchFilesSoft(); // switches m_File and m_ExtensionFile paths
    void SwitchFilesHard(); // copy m_ExtensionFile data into m_File
    Record* BinarySearch(span<unsigned char> target, EvalFunctionType evalFunc, unsigned long long& accessedBlocks);

    struct OrderedRecord
    {
        unsigned long long Id;
    };
};