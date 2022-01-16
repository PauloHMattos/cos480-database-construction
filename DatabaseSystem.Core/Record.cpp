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

void Record::ResizeData(unsigned int newSize)
{
	m_Data.resize(newSize);
}

unsigned long long Record::getId() const
{
	return *(unsigned long long*)(m_Data.data());
}

void Record::Write(ostream& out) {
	m_Schema->Write(out, GetData());
}

unsigned int Record::GetDataSize()
{
	return m_Data.size();
}

unsigned int Record::GetHeadSize()
{
	return m_Schema->GetVarSize();
}

unsigned int Record::GetFixedSize() const
{
	return m_Schema->GetRecordHeadSize();
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
		int varColumnId = 0;
		int sumCellLength = 0;
		int varHeadOffset = record.GetFixedSize();

		size_t sizeOfVarColumnMap = sizeof(struct Record::VarColumnMap);
		
		string cell;
		while (getline(lineStream, cell, ','))
		{
			auto currentColumn = schema.GetColumn(columnId);
			if (schema.IsVarLengthColumn(currentColumn.Type)) {
				// Create map register
				VarColumnMap varColMap;
				varColMap.m_Start = schema.GetSize() + sumCellLength;
				auto cellSize = cell.size();
				varColMap.m_Length = cellSize;
				sumCellLength += cellSize;

				// Fill into record 
				auto columnDataSpan = span(&record.GetData()->data()[varHeadOffset], sizeOfVarColumnMap);
				memcpy(columnDataSpan.data(), &varColMap, sizeOfVarColumnMap);

				// Register value into varData
				record.m_Data.insert(record.m_Data.end(), cell.begin(), cell.end());
				varHeadOffset += sizeOfVarColumnMap;
			}
			else {
				auto columnDataSpan = schema.GetValue(record.GetData(), columnId);
				Column::Parse(currentColumn, columnDataSpan, cell);
			}
			columnId++;
		}

		result.push_back(record);
	}
	return result;
}
