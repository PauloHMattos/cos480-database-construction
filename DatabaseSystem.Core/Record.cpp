#include "pch.h"
#include "Record.h"


Record::Record(Schema* schema) : m_Schema(schema)
{
	m_Data = vector<unsigned char>(m_Schema->GetSize());
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

vector<Record> Record::LoadFromCsv(Schema& schema, string path, unsigned long long lines) {
	auto result = vector<Record>();

	ifstream infile(path);

	string line;
	
	// Skip first line
	getline(infile, line);
	auto linesRead = 0ull;
	while (getline(infile, line) && (lines < 0 || linesRead < lines))
	{
		linesRead++;
		auto record = Record(&schema);
		auto lineStream = istringstream(line);

		int columnId = 1;
		string cell;
		while (getline(lineStream, cell, ','))
		{
			auto currentColumn = schema.GetColumn(columnId);
			auto columnDataSpan = schema.GetValue(record.GetData(), columnId);
			Column::Parse(currentColumn, columnDataSpan, cell);
			columnId++;
		}
		result.push_back(record);
	}
	return result;
}
