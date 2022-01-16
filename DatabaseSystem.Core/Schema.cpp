#include "pch.h"
#include "Schema.h"
#include "Record.h"
#include <cstdio>
#include <sstream>

Schema::Schema() : m_Size(0), m_Columns(vector<Column>()), m_VarLengthColumnPos(vector<unsigned int>()), m_FixedLengthColumnPos(vector<unsigned int>())
{
	AddColumn("Id", ColumnType::INT64);
}

void Schema::AddColumn(string name, ColumnType type, unsigned int arraySize)
{
	auto newColumn = Column(name, type, arraySize);
	m_Columns.push_back(newColumn);
	m_Size += newColumn.GetLength();

	// Var length column register
	auto pos = m_Columns.size() - 1;
	if (IsVarLengthColumn(type)) {
		m_VarLengthColumnPos.push_back(pos);
		m_VarLengthColumns++;
	}
	else {
		m_FixedLengthColumnPos.push_back(pos);
	}
}

bool Schema::IsVarLengthColumn(ColumnType type)
{
	return (int)type == ColumnType::VARCHAR;
}

unsigned int Schema::GetVarSize() const
{
	return m_VarLengthColumns * sizeof(struct Record::VarColumnMap);
}

unsigned int Schema::GetSize() const
{
	return m_Size + GetVarSize() + GetRecordHeadSize();
}

Column Schema::GetColumn(unsigned int columnId)
{
	return m_Columns[columnId];
}

unsigned int Schema::GetColumnId(string columnName) const
{
	for (size_t i = 0 ; i < m_Columns.size(); i++)
	{
		auto &column = m_Columns[i];
		if (column.Name == columnName) {
			return i;
		}
	}
	throw runtime_error("Invalid column " + columnName);
}

span<unsigned char> Schema::GetValue(vector<unsigned char>* data, unsigned int columnId)
{
	auto& column = m_Columns[columnId];
	if (IsVarLengthColumn(column.Type)) {
		unsigned int pos = 0;
		for (int i = 0; i < m_VarLengthColumnPos.size(); i++) {
			if (m_VarLengthColumnPos[i] == columnId) {
				pos = i;
			}
		}
		
		auto columnMaps = span(&data->data()[24], m_VarLengthColumns * sizeof(struct Record::VarColumnMap));
		auto columnMap = (Record::VarColumnMap*)columnMaps.subspan(pos * sizeof(Record::VarColumnMap), sizeof(Record::VarColumnMap)).data();
		auto start = (size_t)columnMap->m_Start;
		auto length = columnMap->m_Length;
		return span(&data->data()[start], length);
	}

	size_t offset = GetVarSize() + GetRecordHeadSize();
	for (size_t i = 1; i < columnId; i++)
	{
		auto &column = m_Columns[i];
		offset += column.GetLength();
	}
	column = m_Columns[columnId];
	return span(&data->data()[offset], column.GetLength());
}

void Schema::Write(ostream& out, vector<unsigned char>* data)
{
	out << "{" << endl;
	size_t offset = 0;
	
	// Print id
	ProcessOutColumn(out, data, m_FixedLengthColumnPos[0], offset, 1);

	offset = GetVarSize() + GetRecordHeadSize();
	for (size_t i = 1; i < m_FixedLengthColumnPos.size(); i++)
	{
		ProcessOutColumn(out, data, m_FixedLengthColumnPos[i], offset, 0);
	}

	auto columnMaps = span(&data->data()[GetRecordHeadSize()], m_VarLengthColumns * sizeof(struct Record::VarColumnMap));
	for (int i = 0; i < columnMaps.size(); i += sizeof(struct Record::VarColumnMap))
	{
		auto columnMap = (Record::VarColumnMap*)columnMaps.subspan(i, sizeof(Record::VarColumnMap)).data();
		auto start = (size_t)columnMap->m_Start;
		auto length = columnMap->m_Length;

		auto columnIndex = i / sizeof(struct Record::VarColumnMap);
		ProcessOutColumn(out, data, m_VarLengthColumnPos[columnIndex], start, length);
	}
	out << '\b' << "}";
}

void Schema::ProcessOutColumn(ostream& out, vector<unsigned char>* data, size_t i, size_t &offset, unsigned int length)
{
	auto& column = m_Columns[i];
	out << '\t' << column.Name << "[" << column.Type;;
	if (column.ArraySize > 1) {
		out << "(" << column.ArraySize << ")";
	}
	out << "]" << " = ";

	auto columnLength = IsVarLengthColumn(column.Type) ? length : column.GetLength();
	auto columnValue = span(&data->data()[offset], columnLength);
	Column::WriteValue(out, column, columnValue);
	out << "," << endl;

	offset += column.GetLength();
}

void Schema::Serialize(iostream& dst)
{
	auto size = m_Columns.size();
	dst << size << endl;
	for (auto &column : m_Columns)
	{
		dst << column.Name << "," << (int)column.Type << "," << column.ArraySize << endl;
	}
}

void Schema::Deserialize(iostream& src)
{
	size_t size;
	src >> size;
	string line;
	getline(src, line);
	for (size_t i = 0; i < size; i++)
	{
		string line;
		getline(src, line);

		vector<string> row;
		string word;
		stringstream str(line);
		while (getline(str, word, ','))
			row.push_back(word);

		if (row.size() != 3) {
			throw runtime_error("Missing fields in schema");
		}
		AddColumn(row[0], ColumnType::_from_index(stoi(row[1])), stoi(row[2]));
	}
}

unsigned int Schema::GetRecordHeadSize() const
{
	return 24;
}