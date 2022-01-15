#include "pch.h"
#include "Record.h"


Record::Record(Schema* schema) : m_Schema(schema)
{
	m_Data = vector<unsigned char>(m_Schema->GetSize());
	m_VarData = vector<unsigned char>();
}

vector<unsigned char>* Record::GetData()
{
	return &m_Data;
}

void Record::SetData(size_t index, unsigned char value)
{
	m_Data[index] = value;
}

unsigned long long Record::getId() const
{
	return *(unsigned long long*)(m_Data.data());
}

void Record::Write(ostream& out) {
	m_Schema->Write(out, GetData());
}

unsigned int Record::GetVarDataSize()
{
	return m_VarData.size();
}

unsigned int Record::GetHeadSize()
{
	return m_Schema->GetVarSize();
}

vector<Record> Record::LoadFromCsv(Schema& schema, string path) {
	auto result = vector<Record>();

	ifstream infile(path);

	string line;
	
	// Skip first line
	getline(infile, line);
	while (getline(infile, line))
	{
		auto record = Record(&schema);
		auto lineStream = istringstream(line);

		int columnId = 1;

		// VarColumns
		int recordSomething = 24; //TODO: MARRETADO!! Trocar! 24 bytes de Id + Struct com ponteiros de NextDeleted
		int varColumnId = 0;
		int varHeadOffset = recordSomething;
		size_t sizeOfVarColumnMap = sizeof(struct Record::VarColumnMap);
		
		string cell;
		while (getline(lineStream, cell, ','))
		{
			auto currentColumn = schema.GetColumn(columnId);
			if (schema.IsVarLengthColumn(currentColumn.Type)) {
				// Create map register
				VarColumnMap varColMap;
				varColMap.m_Start = schema.GetSize() + record.GetVarDataSize() + recordSomething;
				varColMap.m_Length = cell.size();

				// Fill into record 
				auto columnDataSpan = span(&record.GetData()->data()[varHeadOffset], sizeOfVarColumnMap);
				memcpy(columnDataSpan.data(), &varColMap, sizeOfVarColumnMap);

				// Register value into varData
				record.m_VarData.insert(record.m_VarData.end(), cell.begin(), cell.end());
				varHeadOffset += sizeOfVarColumnMap;
			}
			else {
				auto columnDataSpan = schema.GetValue(record.GetData(), columnId);
				Column::Parse(currentColumn, columnDataSpan, cell);
			}
			columnId++;
		}

		// Append varData into record data
		unsigned char k = 'o';
		record.m_Data.push_back(k);
		result.push_back(record);
	}
	return result;
}
