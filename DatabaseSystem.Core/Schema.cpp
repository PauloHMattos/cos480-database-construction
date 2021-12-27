#include "pch.h"
#include "Schema.h"
#include <cstdio>
#include <sstream>

Schema::Schema() : m_Size(0), m_Columns(vector<Column>())
{
	AddColumn("Id", ColumnType::INT64, 0);
}

void Schema::AddColumn(string name, ColumnType type, unsigned int arraySize)
{
	auto newColumn = Column(name, type, arraySize);
	m_Columns.push_back(newColumn);
	m_Size += newColumn.getLength();
}

unsigned int Schema::GetSize() const
{
	return m_Size;
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
	size_t offset = 0;
	for (size_t i = 0; i < columnId; i++)
	{
		auto &column = m_Columns[i];
		offset += column.getLength();
	}
	auto &column = m_Columns[columnId];
	return span(&data->data()[offset], column.getLength());
}

void Schema::Serialize(iostream& dst)
{
	auto size = m_Columns.size();
	dst.write(reinterpret_cast<const char*>(&size), sizeof(size));
	for (auto &column : m_Columns)
	{
		dst << column.Name << "," << (int)column.Type << "," << column.ArraySize << endl;
	}
}

void Schema::Deserialize(iostream& src)
{
	size_t size = 0;
	src >> size;
	//src.read(reinterpret_cast<char*>(&size), sizeof(size));
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
		AddColumn(row[0], static_cast<ColumnType>(stoi(row[1])), stoi(row[2]));
	}
}
