#pragma once
#include "Record.h"
#include "BaseRecordManager.h"

class Table
{
public:
	Table(BaseRecordManager& recordManager);

	void Load(string path);
	void Create(string path, Schema* schema);
	void Close();

	unsigned long long GetSize();
	Schema* GetSchema();
	unsigned long long GetLastQueryAccessedBlocksCount();


	// ---------------------------------------------- <INSERT> --------------------------------------------------------------------------
	void Insert(Record record);
	void InsertMany(vector<Record> records);
	// ---------------------------------------------- </INSERT> --------------------------------------------------------------------------


	// ---------------------------------------------- <SELECT> --------------------------------------------------------------------------
	Record* Select(unsigned long long id);
	vector<Record*> Select(vector<unsigned long long> ids);
	vector<Record*> SelectWhereBetween(string columnName, span<unsigned char> min, span<unsigned char> max);
	vector<Record*> SelectWhereEquals(string columnName, span<unsigned char> data);
	// ---------------------------------------------- </SELECT> --------------------------------------------------------------------------
	
	
	// ---------------------------------------------- <DELETE> --------------------------------------------------------------------------
	void Delete(unsigned long long id);
	int DeleteWhereEquals(string columnName, span<unsigned char> data);
	// ---------------------------------------------- </DELETE> --------------------------------------------------------------------------

private:
	BaseRecordManager& m_RecordManager;
};