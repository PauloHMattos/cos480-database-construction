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
	return m_Size + GetVarSize();
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
	size_t offset = 0 + GetVarSize();
	for (size_t i = 0; i < columnId; i++)
	{
		auto &column = m_Columns[i];
		offset += column.GetLength();
	}
	auto &column = m_Columns[columnId];
	return span(&data->data()[offset], column.GetLength());
}

void Schema::Write(ostream& out, vector<unsigned char>* data)
{
	out << "{" << endl;
	size_t offset = 0;
	for (size_t i = 0; i < m_Columns.size(); i++)
	{
		auto& column = m_Columns[i];
		out << '\t' << column.Name << "[" << column.Type;;
		if (column.ArraySize > 1) {
			out << "(" << column.ArraySize << ")";
		}
		out << "]" << " = ";
		auto columnValue = span(&data->data()[offset], column.GetLength());
		Column::WriteValue(out, column, columnValue);
		out << "," << endl;

		offset += column.GetLength();
	}
	out << '\b' << "}";
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
