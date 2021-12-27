#include "pch.h"
#include "Column.h"

Column::Column(string name, ColumnType type, unsigned int arraySize) : Name(name), Type(type), ArraySize(arraySize)
{
}

unsigned int Column::getLength()
{
	switch (Type)
	{
	case ColumnType::INT32:
	case ColumnType::FLOAT:
		return sizeof(float) * (ArraySize + 1);

	case ColumnType::INT64:
	case ColumnType::DOUBLE:
		return sizeof(double) * (ArraySize + 1);

	case ColumnType::CHAR:
		return sizeof(char) * (ArraySize + 1);

	default:
		return 0;
	}
	return 0;
}

int Column::Compare(Column column, span<unsigned char> a, span<unsigned char> b)
{
	if (a.size() != b.size())
	{
		throw runtime_error("a.size() != b.size()");
	}

	switch (column.Type)
	{
	case ColumnType::INT32:
		return reinterpret_cast<int>(&a[0]) < reinterpret_cast<int>(&b[0]);
	case ColumnType::INT64:
		return reinterpret_cast<long long>(&a[0]) < reinterpret_cast<long long>(&b[0]);
	case ColumnType::DOUBLE:
		return (double)reinterpret_cast<long long>(&a[0]) < (double)reinterpret_cast<long long>(&b[0]);
	case ColumnType::FLOAT:
		return (float)reinterpret_cast<int>(&a[0]) < (float)reinterpret_cast<int>(&b[0]);
	default:
		throw runtime_error("Type not implemented");
	}
	return 0;
}

bool Column::Equals(Column column, span<unsigned char> a, span<unsigned char> b)
{
	if (a.size() != b.size())
	{
		throw runtime_error("a.size() != b.size()");
	}

	return memcmp(a.data(), b.data(), a.size()) == 0;
}
